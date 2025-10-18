/*
 * Copyright 2019 Tamas Bolner
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace AsyncFastCGI;

public class Request 
{
    private int _index;
    private Record _inputRecord;
    private Record _outputRecord;
    private int _maxHeaderSize;
    private FifoStream _inputBuffer;
    private FifoStream _outputBuffer;

    private Client.RequestHandlerDelegate _requestHandler;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="index">The index of the request, by which the Client object identifies it.</param>
    /// <param name="requestHandler">The client callback, which handles the incoming HTTP requests.</param>
    /// <param name="maxHeaderSize">The maximum allowed HTTP header size.</param>
    public Request(int index, Client.RequestHandlerDelegate requestHandler, int maxHeaderSize) 
    {
        _index = index;
        _inputRecord = new Record();
        _outputRecord = new Record();
        _requestHandler = requestHandler;
        _maxHeaderSize = maxHeaderSize;
        _inputBuffer = new FifoStream(maxHeaderSize);
        _outputBuffer = new FifoStream(maxHeaderSize);
    }

    /// <summary>
    /// Get the index of the request, by which the Client object identifies it.
    /// </summary>
    /// <returns>The integer index of the request.</returns>
    public int GetIndex() => _index;

    /// <summary>
    /// Handles new incoming connections.
    /// The caller should not wait on it.
    /// </summary>
    /// <param name="request">The socket for the new incoming connection.</param>
    /// <returns>The index of the Request.</returns>
    public async Task<int> NewConnection(Socket request) 
    {
        var stream = new NetworkStream(request);
        Input input;
        Output output;

        do 
        {
            input = new Input(request, stream, _inputRecord, _inputBuffer, _maxHeaderSize);

            try 
            {
                await input.Initialize();
            } 
            catch (ClientException e) 
            {
                await Console.Error.WriteLineAsync(e.Message);
                request.Close();
                return _index;
            }
                
            output = new Output(input, request, stream, input.GetFastCgiRequestID(), _outputRecord, _outputBuffer);

            try 
            {
                await _requestHandler(input, output);

                if (!output.IsEnded()) 
                {
                    await output.EndAsync();
                }
            } 
            catch (ClientException e) 
            {
                await Console.Error.WriteLineAsync(e.Message);
                request.Close();
                return _index;
            }
        } 
        while (input.IsKeepConnection());

        // If keepConnection == false, then the client is responsible for closing the connection.
        // (Lingering is configured already)
        request.Shutdown(SocketShutdown.Both);
        await request.DisconnectAsync(false);

        return _index;
    }
}