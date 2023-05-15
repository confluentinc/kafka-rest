import http.server
import socketserver
import threading


# Class exposing a simple HTTP server, that has ony 1 endpoint "/chunked".
# This endpoint will return a chunked response, with "caller provided" number of chunks. This is used for testing.
class TestHTTPServerChunked:
    def __init__(self, port=8000, message="Hello", response_chunks=3):
        self.port = port
        self.handler = self.get_request_handler()
        self.server = socketserver.TCPServer(("", self.port), self.handler)
        self.response_chunks = response_chunks
        self.chunk_message = message
        self.thread = threading.Thread(target=self.server.serve_forever)
        
    def start(self):
        self.thread.start()
        print(f"Serving at port {self.port}")
        
    def stop(self):
        self.server.shutdown()
        self.thread.join()
        print("Server stopped.")
        
    def get_request_handler(self):
        class CustomRequestHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                self.chunk_message = kwargs.pop("chunk_message", "Hello")
                self.response_chunks = kwargs.pop("response_chunks", 3)
                super().__init__(*args, **kwargs)

            def do_GET(self):
                if self.path == "/chunked":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    
                    # Send the response in chunks
                    for i in range(0, self.response_chunks):
                        self.wfile.write(f"{len(self.chunk_message):x}\r\n{self.chunk_message}\r\n".encode())
                        self.wfile.flush()
                    self.wfile.write(b"0\r\n\r\n")
                else:
                    # Default behavior for all other requests
                    super().do_GET()
        
        return lambda *args, **kwargs: CustomRequestHandler(*args, chunk_message=self.chunk_message, response_chunks=self.response_chunks, **kwargs)
    

if __name__ == "__main__":
    server = TestHTTPServerChunked()
    server.start()
        
    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop()
