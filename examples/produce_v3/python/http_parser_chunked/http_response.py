from io import DEFAULT_BUFFER_SIZE, BufferedReader, TextIOWrapper

from http_parser_chunked.parser import HttpParserChunked
from http_parser_chunked.http_parser.reader import HttpBodyReader
from http_parser_chunked.http_parser.util import status_reasons, bytes_to_str, b

HTTP_BOTH = 2
HTTP_RESPONSE = 1
HTTP_REQUEST = 0


class NoMoreData(Exception):
    """ exception raised when trying to parse headers but
    we didn't get all data needed.
    """

class ParserError(Exception):
    """ error while parsing http request """

class BadStatusLine(Exception):
    """ error when status line is invalid """
            
class HttpResponse(object):
    """ An HTTP-response object. Its implementation is inspired by class HttpStream defined here https://github.com/benoitc/http-parser/blob/d6ce4b5c58e68d5cf3be0676d9b97c3bd9ca88df/http_parser/http.py#L31. Main difference between this class and HttpStream is
    1. HttpStream.__next__ got renamed to _read_from_stream_and_parse.
    2. This class provides a simple interface to when response-body is "chunked". So it can be iterated over to get the chunks of the response-body.
    """

    def __init__(self, stream, kind=HTTP_RESPONSE, decompress=False,
            parser_class=HttpParserChunked):
        """ constructor.

        :attr stream: an io.RawIOBase object
        :attr kind: Int,  could be 0 to parseonly requests,
        1 to parse only responses or 2 if we want to let
        the parser detect the type.
        """
        self.parser = parser_class(kind=kind, decompress=decompress)
        self.stream = stream

    def _check_headers_complete(self):
        if self.parser.is_headers_complete():
            return

        while True:
            try:
                self._read_from_stream_and_parse()
            except StopIteration:
                if self.parser.is_headers_complete():
                    return
                raise NoMoreData("Can't parse headers")

            if self.parser.is_headers_complete():
                return


    def _wait_status_line(self, cond):
        if self.parser.is_headers_complete():
            return True

        data = []
        if not cond():
            while True:
                try:
                    d = self._read_from_stream_and_parse()
                    data.append(d)
                except StopIteration:
                    if self.parser.is_headers_complete():
                        return True
                    raise BadStatusLine(b"".join(data))
                if cond():
                    return True
        return True

    def _wait_on_url(self):
        return self._wait_status_line(self.parser.get_url)

    def _wait_on_status(self):
        return self._wait_status_line(self.parser.get_status_code)

    def _wait_on_method(self):
        return self._wait_status_line(self.parser.get_method)

    def url(self):
        """ get full url of the request """
        self._wait_on_url()
        return self.parser.get_url()

    def path(self):
        """ get path of the request (url without query string and
        fragment """
        self._wait_on_url()
        return self.parser.get_path()

    def query_string(self):
        """ get query string of the url """
        self._wait_on_url()
        return self.parser.get_query_string()

    def fragment(self):
        """ get fragment of the url """
        self._wait_on_url()
        return self.parser.get_fragment()

    def version(self):
        self._wait_on_status()
        return self.parser.get_version()

    def status_code(self):
        """ get status code of a response as integer """
        self._wait_on_status()
        return self.parser.get_status_code()

    def status(self):
        """ return complete status with reason """
        status_code = self.status_code()
        reason = status_reasons.get(int(status_code), 'unknown')
        return "%s %s" % (status_code, reason)


    def method(self):
        """ get HTTP method as string"""
        self._wait_on_method()
        return self.parser.get_method()

    def headers(self):
        """ get request/response headers, headers are returned in a
        OrderedDict that allows you to get value using insensitive
        keys."""
        self._check_headers_complete()
        headers = self.parser.get_headers()
        return headers.copy()

    def should_keep_alive(self):
        """ return True if the connection should be kept alive
        """
        self._check_headers_complete()
        return self.parser.should_keep_alive()

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        self._check_headers_complete()
        return self.parser.is_chunked()

    def wsgi_environ(self, initial=None):
        """ get WSGI environ based on the current request.

        :attr initial: dict, initial values to fill in environ.
        """
        self._check_headers_complete()
        return self.parser.get_wsgi_environ()

    # This will be broken with execute() being changed.
    def body_file(self, buffering=None, binary=True, encoding=None,
            errors=None, newline=None):
        """ return the body as a buffered stream object. If binary is
        true an io.BufferedReader will be returned, else an
        io.TextIOWrapper.
        """
        self._check_headers_complete()

        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = DEFAULT_BUFFER_SIZE

        raw = HttpBodyReader(self)
        buf = BufferedReader(raw, buffering)
        if binary:
            return buf
        text = TextIOWrapper(buf, encoding, errors, newline)
        return text

    def body_string(self, binary=True, encoding=None, errors=None,
            newline=None):
        """ return body as string """
        return self.body_file(binary=binary, encoding=encoding,
                newline=newline).read()

    def __iter__(self):
        return self

    # In HttpStream class, this method was called __next__, which
    # has been renamed.
    def _read_from_stream_and_parse(self):
        if self.parser.is_message_complete():
            raise StopIteration

        # fetch data
        b = bytearray(DEFAULT_BUFFER_SIZE)

        # if a nonblocking socket is used
        # then pep 3116 demands read/readinto to return 0
        recved = self.stream.readinto(b)
        if recved is None:
            raise IOError('nonblocking socket used in blocking code')

        del b[recved:]
        to_parse = bytes(b)
        # parse data
        nparsed = self.parser.execute(to_parse, recved)
        if nparsed != recved and not self.parser.is_message_complete():
            raise ParserError("nparsed != recved (%s != %s) [%s]" % (nparsed,
                recved, bytes_to_str(to_parse)))

        if recved == 0:
            raise StopIteration

        return to_parse
    
    def __next__(self):
        # This will make sure all headers are parsed, and 
        # "transfer-encoding: chunked" header is present.
        if not self.is_chunked():
            raise ParserError("Http response isn't chunked, as expected")
        
        while True:
            # fetch data
            b = bytearray(DEFAULT_BUFFER_SIZE)

            # if a nonblocking socket is used
            # then pep 3116 demands read/readinto to return 0
            recved = self.stream.readinto(b)
            if recved is None:
                raise IOError('nonblocking socket used in blocking code')

            del b[recved:]
            to_parse = bytes(b)
            # parse data
            nparsed = self.parser.execute(to_parse, recved)
            if nparsed != recved and not self.parser.is_message_complete():
                raise ParserError("nparsed != recved (%s != %s) [%s]" % (nparsed,
                    recved, bytes_to_str(to_parse)))

            # partial message-body is parsed, i.e. chunks are present
            if self.parser.is_partial_body():
                chunk = self.parser.recv_body_chunk_fifo()
                assert chunk is not None
                return chunk
            
            # message-body is completely parsed, and no more chunks to return
            if self.parser.is_message_complete() and not self.parser.is_partial_body():
                raise StopIteration