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
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace AsyncFastCGI;

public class Output 
{
    private Input _input;
    private Socket _connection;
    private NetworkStream _stream;
    private Record _record;
    private UInt16 _requestID;
    private bool _ended;
    private bool _headerSent;
    private Dictionary<string, string> _header;
    private int _httpStatus = 200;
    private FifoStream _outputBuffer;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="input">input of the client connection</param>
    /// <param name="request">Socket of the client connection.</param>
    /// <param name="stream">Stream of the client connection</param>
    /// <param name="requestId">FastCGI request ID</param>
    /// <param name="record"></param>
    /// <param name="outputBuffer"></param>
    public Output(Input input, Socket request, NetworkStream stream, UInt16 requestId, Record record, FifoStream outputBuffer) 
    {
        _input = input;
        _connection = request;
        _stream = stream;
        _requestID = requestId;
        _header = new Dictionary<string, string>();
        _outputBuffer = outputBuffer;
        _outputBuffer.Reset();
        _record = record;
            
        _ended = false;
        _headerSent = false;

        // Default headers (can be overwritten)
        _header["Content-Type"] = "text/html; charset=utf-8";
        _header["Cache-Control"] = "no-cache";
        _header["Date"] = DateTime.Today.ToUniversalTime().ToString("r");
        _header["Server"] = "AsyncFastCGI.NET";
    }

    /// <summary>
    /// Send a string response back through the connection.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task WriteAsync(string data) 
    {
        if (_ended)
            return;

        if (!_headerSent) 
            /*
                Send HTTP header if it wasn't sent yet.
            */
            WriteHeader();

        _outputBuffer.Write(Encoding.UTF8.GetBytes(data));
        
        await SendBuffer(false);
    }

    /// <summary>
    /// Send a binary response back through the connection.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task WriteBinaryAsync(byte[] data) 
    {
        if (_ended)
            return;

        if (!_headerSent)
            /*
                Send HTTP header if it wasn't sent yet.
            */
            WriteHeader();

        _outputBuffer.Write(data);
        await SendBuffer(false);
    }

    /// <summary>
    /// Flush the remaining output, prevent further writes,
    /// close the FastCGI STDOUT with an empty record,
    /// and send an "end request" record.
    /// </summary>
    public async Task EndAsync() 
    {
        if (_ended)
            return;

        await SendBuffer(true);

        /*
            Send an empty STDOUT closing record.
        */
        _record.STDOUT(_requestID, null);
        await _record.sendAsync(_stream);

        /*
            Send an "end request" record.
        */
        _record.END_REQUEST(_requestID, 0, Record.PROTOCOL_STATUS_REQUEST_COMPLETE);
        await _record.sendAsync(_stream);
            
        _ended = true;
    }

    /// <summary>
    /// Returns true if the output has been closed already, false otherwise.
    /// </summary>
    /// <returns>bool</returns>
    public bool IsEnded() => _ended;

    /// <summary>
    /// Set the HTTP response status.
    /// </summary>
    /// <param name="status">HTTP response status. Example: 200</param>
    public void SetHttpStatus(int status) => _httpStatus = status;

    /// <summary>
    /// Set HTTP header. You have to set all headers before you start to
    /// write the output.
    /// </summary>
    /// <param name="name">Name of the header entry. Example: "Content-Type"</param>
    /// <param name="value">Value of the header entry. Example: "text/html; charset=utf-8"</param>
    public void SetHeader(string name, string value) => _header[name] = value;

    /// <summary>
    /// Writes the HTTP header into the output buffer.
    /// Call it after the first call to a "Write" method.
    /// </summary>
    private void WriteHeader() 
    {
        var codeText = Client.GetHttpStatusText(_httpStatus);
        if (codeText == "") 
            _outputBuffer.Write(Encoding.UTF8.GetBytes($"HTTP/1.1 {_httpStatus}\r\n"));
        else 
            _outputBuffer.Write(Encoding.UTF8.GetBytes($"HTTP/1.1 {_httpStatus} {codeText}\r\n"));
        

        foreach(var entry in _header)
        {
            _outputBuffer.Write(Encoding.UTF8.GetBytes($"{entry.Key}: {entry.Value}\r\n"));
        }

        // Last CR/LF
        _outputBuffer.Write(new byte[] { 0x0D, 0x0A });

        _headerSent = true;
    }

    /// <summary>
    /// Sends the output buffer in 64 KBytes long records, and
    /// empties it out.
    /// </summary>
    /// <param name="sendLeftover">False: Don't send the last segment
    /// if it's not exactly 65535 bytes. True: send all.</param>
    private async Task SendBuffer(bool sendLeftover = false) 
    {
        while(_outputBuffer.GetLength() > 0) 
        {
            if (!sendLeftover && _outputBuffer.GetLength() < Record.MAX_CONTENT_SIZE) 
                return;

            if (!_input.IsInputCompleted())
                await _input.ReadAllAndDiscardAsync();
            

            _record.STDOUT(_requestID, _outputBuffer);
            try 
            {
                await _record.sendAsync(_stream);
            } 
            catch (Exception e) 
            {
                Console.WriteLine(e.ToString());
                _ended = true;
            }
        }
    }
}