from http_parser_chunked.http_parser.pyparser import HttpParser

"""This is a subclass of HttpParser. It gives an extra method
recv_body_chunk_fifo to retrieve message-body chunk in fifo order of its arrival."""
class HttpParserChunked(HttpParser):
    def __init__(self, kind=0, decompress=False):
        super().__init__(kind=kind, decompress=decompress)

    def recv_body_chunk_fifo(self):
        if len(self._body) == 0:
            return None
        chunk = self._body.pop(0)
        if len(self._body) == 0:
            self._body = []
            self._partial_body = False
        return chunk