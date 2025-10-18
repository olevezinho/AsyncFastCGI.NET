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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace AsyncFastCGI;

public class Client
{
    public delegate Task RequestHandlerDelegate(Input input, Output output);

    /// <summary>
    /// Set here the async callback, that should
    /// be called to handle incoming requests.
    /// </summary>
    public RequestHandlerDelegate RequestHandler;

    private static Dictionary<int, string> httpStatuses;

    private int _port;

    /// <summary>
    /// Set the port on which the library is listening for
    /// connections from the webserver.
    /// </summary>
    /// <param name="port">Listening port. Usually: 1025 - 65535</param>
    public void SetPort(int port)
    {
        _port = port;
    }

    public int GetPort()
    {
        return _port;
    }

    private int _maxConcurrentRequests;

    /// <summary>
    /// Set the maximum number of requests that
    /// can run in parallel.
    /// </summary>
    /// <param name="maxConcurrentRequests">Typical range: 50 - 500</param>
    public void SetMaxConcurrentRequests(int maxConcurrentRequests)
    {
        _maxConcurrentRequests = maxConcurrentRequests;
    }

    public int GetMaxConcurrentRequests()
    {
        return _maxConcurrentRequests;
    }

    private IPAddress _bindAddress;

    /// <summary>
    /// This is a security feature. You can limit where your
    /// FastCGI client is accessible from. By setting it
    /// to "0.0.0.0", its port is accessible from anywhere
    /// on your local network (or internet), but if you
    /// set it to "127.0.0.1", then it's only accessible
    /// from the localhost.
    /// </summary>
    /// <param name="bindAddress"></param>
    public void SetBindAddress(string bindAddress)
    {
        try 
        {
            _bindAddress = IPAddress.Parse(bindAddress);
        }
        catch (Exception e) 
        {
            throw new ClientException($"Invalid bind address '{bindAddress}'.", e);
        }
    }

    /// <summary>
    /// Returns the interface specification, where the listening
    /// socket was bound.
    /// </summary>
    /// <returns>The bind address</returns>
    public string GetBindAddress()
    {
        return _bindAddress.ToString();
    }

    private int connectionTimeout;

    public void SetConnectionTimeout(int ms) {
        connectionTimeout = ms;
    }

    public int GetConnectionTimeout() {
        return connectionTimeout;
    }

    private int maxHeaderSize;

    public int GetMaxHeaderSize() {
        return maxHeaderSize;
    }

    /// <summary>
    /// Set the maximum allowed size for HTTP headers. Don't
    /// forget to also configure this in the webserver,
    /// since that converts them into parameters.
    /// </summary>
    /// <param name="value">The maximum allowed size for HTTP headers.</param>
    public void SetMaxHeaderSize(int value) {
        maxHeaderSize = value;
    }

    /*
        Managing requests
    */
    private Request[] requests;
    private Task<int>[] tasks;

    /// <summary>
    /// Constructor. Setting defaults.
    /// </summary>
    public Client() {
        /*
            Defaults
        */
        _port = 8080;
        _maxConcurrentRequests = 256;
        _bindAddress = IPAddress.Parse("0.0.0.0");  // Listen on all interfaces
        connectionTimeout = 5000;  // 5 sec
        maxHeaderSize = 16384; // 16 KB
    }

    /// <summary>
    /// Main entry point of the client library.
    /// </summary>
    public async Task StartAsync()
    {
        Socket connection;

        int callbackCount = RequestHandler.GetInvocationList().Length;
            
        if (callbackCount < 1) 
            throw new ClientException("Please set a callback for new requests. (Client.OnNewRequest)");

        if (callbackCount > 1)
            throw new ClientException("It isn't allowed to set more than one callback for new requests. (Client.OnNewRequest)");

        if (_port is < 1 or > 65535)
            throw new ClientException($"The specified port is invalid: {_port}");

        InitHttpStatuses();

        /*
            Initialize the arrays of Request objects and tasks.
        */
        requests = new Request[_maxConcurrentRequests];
        tasks = new Task<int>[_maxConcurrentRequests];

        /*
            Listen on socket, wait for the webserver to connect.
        */
        var listeningSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        var bindEndpoint = new IPEndPoint(_bindAddress, _port);
        listeningSocket.Bind(bindEndpoint);
        listeningSocket.ReceiveTimeout = 5000;
        listeningSocket.SendTimeout = 5000;

        listeningSocket.Listen(_maxConcurrentRequests * 5);

        /*
            First fill the arrays with Requests and Tasks as
            the new connections arrive.
        */
        for (var i = 0; i < _maxConcurrentRequests; i++) 
        {
            connection = await AcceptConnection(listeningSocket);

            requests[i] = new Request(i, RequestHandler, GetMaxHeaderSize());
            tasks[i] = requests[i].NewConnection(connection);
        }

        /*
            When they got full, then go into a loop of waiting
            on finished tasks before accepting new connections.
            Re-use the Request objects to minimize memory
            allocation and garbage collection.
        */
        while(true)
        {
            var task = await Task.WhenAny(tasks);
                
            int index = task.Result;
            connection = await AcceptConnection(listeningSocket);
            tasks[index] = requests[index].NewConnection(connection);
        }
    }

    /// <summary>
    /// Waits for an incoming connection from the webserver,
    /// then configures its socket.
    /// </summary>
    /// <param name="listeningSocket"></param>
    /// <returns></returns>
    private async Task<Socket> AcceptConnection(Socket listeningSocket) 
    {
        Socket connection;

        try 
        {
            connection = await listeningSocket.AcceptAsync();
        } 
        catch (Exception e) 
        {
            throw new ClientException("Listening socket lost. (Socket.AcceptAsync)", e);
        }

        // Configure the socket of the connection
        connection.ReceiveTimeout = GetConnectionTimeout();
        connection.SendTimeout = GetConnectionTimeout();

        var lingerOption = new LingerOption (true, 20);
        connection.SetSocketOption (SocketOptionLevel.Socket, SocketOptionName.Linger, lingerOption);

        return connection;
    }

    /// <summary>
    /// Returns the text for an HTTP status code.
    /// Example: returns "Not Found" for code 404.
    /// </summary>
    /// <param name="httpStatusCode"></param>
    /// <returns>Text representation of the code.</returns>
    public static string GetHttpStatusText(int httpStatusCode) 
    {
        if (!httpStatuses.ContainsKey(httpStatusCode)) 
            return "";
            

        return httpStatuses[httpStatusCode];
    }

    /// <summary>
    /// Initialize the dictionary of HTTP status codes/texts.
    /// </summary>
    private static void InitHttpStatuses() {
        httpStatuses = new Dictionary<int, string>();

        httpStatuses[100] = "Continue";
        httpStatuses[101] = "Switching Protocols";
        httpStatuses[102] = "Processing";
        httpStatuses[103] = "Early Hints";
        httpStatuses[200] = "OK";
        httpStatuses[201] = "Created";
        httpStatuses[202] = "Accepted";
        httpStatuses[203] = "Non-Authoritative Information";
        httpStatuses[204] = "No Content";
        httpStatuses[205] = "Reset Content";
        httpStatuses[206] = "Partial Content";
        httpStatuses[207] = "Multi-Status";
        httpStatuses[208] = "Already Reported";
        httpStatuses[226] = "IM Used";
        httpStatuses[300] = "Multiple Choices";
        httpStatuses[301] = "Moved Permanently";
        httpStatuses[302] = "Found";
        httpStatuses[303] = "See Other";
        httpStatuses[304] = "Not Modified";
        httpStatuses[305] = "Use Proxy";
        httpStatuses[307] = "Temporary Redirect";
        httpStatuses[308] = "Permanent Redirect";
        httpStatuses[400] = "Bad Request";
        httpStatuses[401] = "Unauthorized";
        httpStatuses[402] = "Payment Required";
        httpStatuses[403] = "Forbidden";
        httpStatuses[404] = "Not Found";
        httpStatuses[405] = "Method Not Allowed";
        httpStatuses[406] = "Not Acceptable";
        httpStatuses[407] = "Proxy Authentication Required";
        httpStatuses[408] = "Request Timeout";
        httpStatuses[409] = "Conflict";
        httpStatuses[410] = "Gone";
        httpStatuses[411] = "Length Required";
        httpStatuses[412] = "Precondition Failed";
        httpStatuses[413] = "Payload Too Large";
        httpStatuses[414] = "URI Too Long";
        httpStatuses[415] = "Unsupported Media Type";
        httpStatuses[416] = "Range Not Satisfiable";
        httpStatuses[417] = "Expectation Failed";
        httpStatuses[418] = "I'm a teapot";
        httpStatuses[422] = "Unprocessable Entity";
        httpStatuses[423] = "Locked";
        httpStatuses[424] = "Failed Dependency";
        httpStatuses[426] = "Upgrade Required";
        httpStatuses[428] = "Precondition Required";
        httpStatuses[429] = "Too Many Requests";
        httpStatuses[431] = "Request Header Fields Too Large";
        httpStatuses[451] = "Unavailable For Legal Reasons";
        httpStatuses[500] = "Internal Server Error";
        httpStatuses[501] = "Not Implemented";
        httpStatuses[502] = "Bad Gateway";
        httpStatuses[503] = "Service Unavailable";
        httpStatuses[504] = "Gateway Time-out";
        httpStatuses[505] = "HTTP Version Not Supported";
        httpStatuses[506] = "Variant Also Negotiates";
        httpStatuses[507] = "Insufficient Storage";
        httpStatuses[508] = "Loop Detected";
        httpStatuses[510] = "Not Extended";
        httpStatuses[511] = "Network Authentication Required";
    }
}