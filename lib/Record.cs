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
using System.Threading.Tasks;
using System.Net.Sockets;

namespace AsyncFastCGI;

public class Record 
{
    /*
        Size constraints
    */
    public const int HEADER_SIZE = 8;
    public const int MAX_CONTENT_SIZE = 65535;
    public const int MAX_PADDING_SIZE = 255;
    public const int MAX_RECORD_SIZE = HEADER_SIZE + MAX_CONTENT_SIZE + MAX_PADDING_SIZE;

    /*
        Record types
    */
    public const int TYPE_BEGIN_REQUEST = 1;
    public const int TYPE_ABORT_REQUEST = 2;
    public const int TYPE_END_REQUEST = 3;
    public const int TYPE_PARAMS = 4;
    public const int TYPE_STDIN = 5;
    public const int TYPE_STDOUT = 6;
    public const int TYPE_STDERR = 7;
    public const int TYPE_DATA = 8;
    public const int TYPE_GET_VALUES = 9;
    public const int TYPE_GET_VALUES_RESULT = 10;
    public const int TYPE_UNKNOWN_TYPE = 11;

    /*
        Request Roles
    */
    public const int ROLE_RESPONDER = 1;
    public const int ROLE_AUTHORIZER = 2;
    public const int ROLE_FILTER = 3;

    /*
        Protocol status values for an end request response.
    */
    public const int PROTOCOL_STATUS_REQUEST_COMPLETE = 0;
    public const int PROTOCOL_STATUS_CANT_MPX_CONN = 1;
    public const int PROTOCOL_STATUS_OVERLOADED = 2;
    public const int PROTOCOL_STATUS_UNKNOWN_ROLE = 3;

    private byte[] _buffer;
    private int _bufferEnd = 0;

    private bool _headerReconstructed = false;
    private bool _completeRecordReconstructed = false;

    private bool _isLittleEndian;

    private byte _recordVersion = 0;
    private byte _recordType = 0;
    private UInt16 _recordRequestID = 0;
    private UInt16 _recordContentLength = 0;
    private int _recordLength = 0;
    private UInt16 _recordPaddingLength = 0;

    public int GetRecordType() => _recordType;

    public int GetLength() => _recordLength;

    public UInt16 GetRequestID() => _recordRequestID;

    public UInt16 GetContentLength() => _recordContentLength;

    public Record() 
    {
        _buffer = new byte[MAX_RECORD_SIZE];
        _isLittleEndian = BitConverter.IsLittleEndian;
    }

    /// <summary>
    /// Use it in an iteration. Keeps reading from the network
    /// stream until at least one complete record is reconstructed.
    /// </summary>
    /// <returns>True if a complete record has been reconstructed, false otherwise.</returns>
    public async Task<bool> ProcessInputAsync(NetworkStream stream) 
    {
        var skipRead = false;
        if (_completeRecordReconstructed)
            skipRead = StartNextRecord();

        if (!skipRead) 
        {
            var remaining = MAX_RECORD_SIZE - _bufferEnd;
            var bytesRead = await stream.ReadAsync(_buffer, _bufferEnd, remaining);
            if (bytesRead == 0) 
                throw new ClientException("Socket disconnected while trying to read.");
                
            _bufferEnd += bytesRead;
        }
        
        /*
            Reconstruct the header
        */
        if (!_headerReconstructed) 
        {
            if (_bufferEnd + 1 < HEADER_SIZE) 
                return false;

            _recordVersion = _buffer[0];
            _recordType = _buffer[1];
            if (_isLittleEndian) 
            {
                _recordRequestID = (UInt16)((_buffer[2] << 8) | _buffer[3]);
                _recordContentLength = (UInt16)((_buffer[4] << 8) | _buffer[5]);
            } 
            else 
            {
                _recordRequestID = (UInt16)((_buffer[3] << 8) | _buffer[2]);
                _recordContentLength = (UInt16)((_buffer[5] << 8) | _buffer[4]);
            }
            _recordPaddingLength = _buffer[6];

            _recordLength = HEADER_SIZE + _recordContentLength + _recordPaddingLength;
            _headerReconstructed = true;
        }

        if (_bufferEnd >= _recordLength) 
        {
            _completeRecordReconstructed = true;
            return true;
        }

        return false;
    }

    private bool StartNextRecord() 
    {
        var leftover = _bufferEnd - _recordLength;
        if (leftover > 0) 
            Array.Copy(_buffer, _recordLength, _buffer, 0, leftover);

        _bufferEnd = leftover;
        _completeRecordReconstructed = false;
        _headerReconstructed = false;

        if (leftover > 0)
            return true;

        return false;
    }

    public void Reset()
    {
        _bufferEnd = 0;
        _completeRecordReconstructed = false;
        _headerReconstructed = false;
    }

    /// <summary>
    /// Returns the role, which can be: responder, authorizer, filter.
    /// </summary>
    /// <returns>Role identifier value</returns>
    public UInt16 GetRole() 
    {
        if (_isLittleEndian)
            return (UInt16)((_buffer[HEADER_SIZE + 0] << 8) | _buffer[HEADER_SIZE + 1]);

        return (UInt16)((_buffer[HEADER_SIZE + 1] << 8) | _buffer[HEADER_SIZE + 0]);
    }

    /// <summary>
    /// For BEGIN_REQUEST records. If zero, the application closes the connection
    /// after responding to this request. If not zero, the application does not
    /// close the connection after responding to this request; the Web server
    /// retains responsibility for the connection.
    /// </summary>
    /// <returns>0 for closing, 1 for keeping the connection open after this request.</returns>
    public bool IsKeepConnection() => _buffer[HEADER_SIZE + 2] > 0;

    /// <summary>
    /// Makes a copy of the content data, and pushes it into
    /// the passed FIFO stream.
    /// </summary>
    /// <param name="stream">The stream which receives the data</param>
    public void CopyContentTo(FifoStream stream) 
    {
        var data = new byte[_recordContentLength];
        Array.Copy(_buffer, HEADER_SIZE, data, 0, _recordContentLength);
        stream.Write(data);
    }

    /// <summary>
    /// Converts the record buffer into an STDOUT record, and fills it with data.
    /// </summary>
    /// <param name="requestID">FastCGI request ID</param>
    /// <param name="fifo">Data source. Pass null to create an empty closing record.</param>
    /// <returns>Number of bytes transferred from the FIFO stream.</returns>
    public int STDOUT(UInt16 requestID, FifoStream fifo) 
    {
        /*
            Set content
        */
        UInt16 length;

        if (fifo == null) 
            length = 0;
        else
            length = (UInt16)fifo.Read(MAX_CONTENT_SIZE, _buffer, 8);
            
        /*
            Set header
        */
        _buffer[0] = (byte)1;               // Version
        _buffer[1] = (byte)TYPE_STDOUT;     // Type

        if (_isLittleEndian)
        {
            _buffer[2] = (byte)(requestID >> 8);      // Request ID 1
            _buffer[3] = (byte)(requestID & 0x00FF);  // Request ID 0

            _buffer[4] = (byte)(length >> 8);         // Content Length 1
            _buffer[5] = (byte)(length & 0x00FF);     // Content Length 0
        } 
        else 
        {
            _buffer[2] = (byte)(requestID << 8);      // Request ID 1
            _buffer[3] = (byte)(requestID & 0xFF00);  // Request ID 0

            _buffer[4] = (byte)(length << 8);         // Content Length 1
            _buffer[5] = (byte)(length & 0xFF00);     // Content Length 0
        }

        _buffer[6] = 0;     // Padding
        _buffer[7] = 0;     // Reserved

        _bufferEnd = 8 + length;

        return length;
    }

    /// <summary>
    /// Converts this record into type "END_REQUEST".
    /// </summary>
    /// <param name="requestID">FastCGI request ID</param>
    /// <param name="appStatus">Return 0 for success, or an error code otherwise.</param>
    /// <param name="protocolStatus">See the FastCGI specification for possible values.</param>
    public void END_REQUEST(UInt16 requestID, int appStatus, byte protocolStatus) 
    {
        /*
            Set header
        */
        _buffer[0] = (byte)1;                   // Version
        _buffer[1] = (byte)TYPE_END_REQUEST;    // Type

        if (_isLittleEndian) 
        {
            _buffer[2] = (byte)(requestID >> 8);      // Request ID 1
            _buffer[3] = (byte)(requestID & 0x00FF);  // Request ID 0

            _buffer[4] = (byte)0;                     // Content Length 1
            _buffer[5] = (byte)6;                     // Content Length 0
        } 
        else 
        {
            _buffer[2] = (byte)(requestID << 8);      // Request ID 1
            _buffer[3] = (byte)(requestID & 0xFF00);  // Request ID 0

            _buffer[4] = (byte)0;                     // Content Length 1
            _buffer[5] = (byte)6;                     // Content Length 0
        }

        _buffer[6] = 0;     // Padding
        _buffer[7] = 0;     // Reserved

        /*
            Set content
        */
        if (_isLittleEndian) 
        {
            _buffer[8] = (byte)(appStatus >> 24);
            _buffer[9] = (byte)(appStatus >> 16);
            _buffer[10] = (byte)(appStatus >> 8);
            _buffer[11] = (byte)appStatus;
        } 
        else 
        {
            _buffer[8] = (byte)appStatus;
            _buffer[9] = (byte)(appStatus << 8);
            _buffer[10] = (byte)(appStatus << 16);
            _buffer[11] = (byte)(appStatus << 24);
        }

        _buffer[12] = protocolStatus;
        _buffer[13] = 0;

        _bufferEnd = 8 + 6;
    }

    /// <summary>
    /// Send the record through the connection.
    /// </summary>
    /// <param name="stream">Stream of the connection socket.</param>
    public async Task sendAsync(NetworkStream stream) 
    {
        try 
        {
            await stream.WriteAsync(_buffer, 0, _bufferEnd);
            await stream.FlushAsync();
        } 
        catch (Exception e) 
        {
            throw new ClientException("Socket disconnected while trying to write to stream.", e);
        }
    }
}