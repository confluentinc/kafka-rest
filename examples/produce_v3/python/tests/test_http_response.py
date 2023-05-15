import socket
import pytest
import io

from http_parser_chunked.http_response import HttpResponse
from test_http_server_chunked import TestHTTPServerChunked

import urllib3
from contextlib import closing

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

# Test HttpResponse class can read all chunks OK against a static
# input stream.
def test_reading_chunks_of_http_response_body_ok():
    stream = io.BytesIO(b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nVary: Accept-Encoding, User-Agent\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n8\r\n, World!\r\n0\r\n\r\n')
    http_stream = HttpResponse(stream)
    chunk1 = next(http_stream)
    assert chunk1 == b'Hello'
    print("Got 1st chunk:", chunk1)
    chunk2 = next(http_stream)
    assert chunk2 == b', World!'
    print("Got 2nd chunk:", chunk2)

    assert http_stream.status_code() == 200
    assert http_stream.headers().get("Content-Type") == "text/plain"
    assert http_stream.headers().get("Transfer-Encoding") == "chunked"
    with pytest.raises(StopIteration):
        next(http_stream)

# Test HttpResponse class can read all chunks OK returned from 
# a "real" HTTP server.
def test_reading_chunks_of_http_response_body_from_real_server_ok():
    test_port = find_free_port()
    test_server = TestHTTPServerChunked(port=test_port,message="test", response_chunks=10)
    test_server.start()

    # Make a request to the test server
    connection = urllib3.connection.HTTPConnection(
        host='localhost',
        port=test_port,
    )
    connection.request(method='GET', url='/chunked')

    print("Request sent.")

    http_stream = HttpResponse(socket.SocketIO(connection.sock, "rb"))
    assert http_stream.status_code() == 200
    chunk_received_counter = 0
    for chunk in http_stream:
        chunk_received_counter += 1
        print("Got chunk:", chunk)
        assert chunk == b'test'
    assert chunk_received_counter == 10
    test_server.stop()

# Test that http-response-body can be read as a string in one go, Vs 1 chunk at a time.
def test_reading_http_response_body_from_real_server_ok():
    test_port = find_free_port()
    test_server = TestHTTPServerChunked(port=test_port,message="test", response_chunks=10)
    test_server.start()

    # Make a request to the test server
    connection = urllib3.connection.HTTPConnection(
        host='localhost',
        port=test_port,
    )
    connection.request(method='GET', url='/chunked')

    print("Request sent.")

    http_stream = HttpResponse(socket.SocketIO(connection.sock, "rb"))
    status_code = http_stream.status_code()
    print("Got status code:", status_code)
    body = http_stream.body_string()
    print("Got response-body:", body)
    assert status_code == 200
    assert body == b'test' * 10
    test_server.stop()