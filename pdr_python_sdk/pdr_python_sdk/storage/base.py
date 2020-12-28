
from __future__ import absolute_import

import logging
import socket
import ssl

import urllib
import http.client
from ..storage.record import record


__all__ = [
    "connect",
    "Base",
    "handler",
    "HTTPError"
]

# If you change these, update the docstring
# on _uri as well.
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "9200"
DEFAULT_SCHEME = "http"


class UrlEncoded(str):
    """
    URL-encoded strings.

    ``UrlEncoded`` objects are identical to ``str`` objects (including being
    equal if their contents are equal) except when passed to ``UrlEncoded``
    again.

    ``UrlEncoded`` removes the ``str`` type support for interpolating values
    with ``%`` (doing that raises a ``TypeError``). There is no reliable way to
    encode values this way, so instead, interpolate into a string, quoting by
    hand, and call ``UrlEncode`` with ``skip_encode=True``.
    """
    def __new__(self, val='', skip_encode=False, encode_slash=False):
        if isinstance(val, UrlEncoded):
            return val
        elif skip_encode:
            return str.__new__(self, val)
        elif encode_slash:
            return str.__new__(self, urllib.parse.quote_plus(val))
        else:
            return str.__new__(self, urllib.parse.quote(val))

    def __add__(self, other):
        """
        self + other

        If *other* is not a ``UrlEncoded``, URL encode it before
        adding it.
        """
        if isinstance(other, UrlEncoded):
            return UrlEncoded(str.__add__(self, other), skip_encode=True)
        else:
            return UrlEncoded(str.__add__(self, urllib.parse.quote(other)), skip_encode=True)

    def __radd__(self, other):
        """
        other + self

        If *other* is not a ``UrlEncoded``, URL _encode it before
        adding it.
        """
        if isinstance(other, UrlEncoded):
            return UrlEncoded(str.__radd__(self, other), skip_encode=True)
        else:
            return UrlEncoded(str.__add__(urllib.parse.quote(other), self), skip_encode=True)

    def __mod__(self, fields):
        """
        Interpolation into ``UrlEncoded``s is disabled.

        If you try to write ``UrlEncoded("%s") % "abc", will get a
        ``TypeError``.
        """
        raise TypeError("Cannot interpolate into a UrlEncoded object.")
    def __repr__(self):
        return "UrlEncoded(%s)" % repr(urllib.parse.unquote(str(self)))

def _uri(scheme=DEFAULT_SCHEME, host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Construct a URI from the given *scheme*, *host*, and *port*.

    :param scheme: URL scheme (the default is "https")
    :type scheme: "http" or "https"
    :param host: The host name (the default is "localhost")
    :type host: string
    :param port: The port number (the default is 8089)
    :type port: integer
    :return: The URI.
    :rtype: UrlEncoded (subclass of ``str``)
    """
    if ':' in host:
        # IPv6 addresses must be enclosed in [ ] in order to be well
        # formed.
        host = '[' + host + ']'
    return UrlEncoded("%s://%s:%s" % (scheme, host, port), skip_encode=True)

class Base(object):
    """
    The ``Base`` class encapsulates the details of HTTP requests,
    authentication, and URL prefixes to simplify access to
    the REST API.

    :param host: The host name (the default is "localhost").
    :type host: ``string``
    :param port: The port number (the default is 8089).
    :type port: ``integer``
    :param scheme: The scheme for accessing the service (the default is "https").
    :type scheme: "https" or "http"
    :param headers: List of extra HTTP headers to send (optional).
    :type headers: ``list`` of 2-tuples.
    :param handler: The HTTP request handler (optional).
    :returns: A ``Base`` instance.
    """
    def __init__(self, handler=None, **kwargs):
        self.http = HttpBase(handler, kwargs.get("verify", False), key_file=kwargs.get("key_file"),
                            cert_file=kwargs.get("cert_file"))  # Default to False for backward compat
        self.scheme = kwargs.get("scheme", DEFAULT_SCHEME)
        self.host = kwargs.get("host", DEFAULT_HOST)
        self.port = int(kwargs.get("port", DEFAULT_PORT))
        self.uri = _uri(self.scheme, self.host, self.port)
        self.additional_headers = kwargs.get("headers", [])
        self.additional_headers = self.assign_default_header(self.additional_headers, 'Content-Type', 'application/json')

    def assign_default_header(self, headers, header, value):
        exists = False
        for h in headers:
            if h[0] == header:
                exists = True
                break

        if not exists:
            return headers + [(header, value)]


    def connect(self):
        """
        Return an open connection (socket).

        This method is used for writing bulk events to an index or similar tasks
        where the overhead of opening a connection multiple times would be
        prohibitive.

        :returns: A socket.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.scheme == "https":
            sock = ssl.wrap_socket(sock)
        sock.connect((socket.gethostbyname(self.host), self.port))
        return sock

    def delete(self, path_segment, **kwargs):
        """
        Perform a DELETE operation at the REST path segment.

        This method is named to match the HTTP method.

        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment. type ``string``
        :param kwargs: All other arguments. type ``dict``
        :return: The response from the server.
        """
        path = self.uri + self._abspath(path_segment)
        logging.debug("DELETE request to %s (body: %s)", path, repr(kwargs))
        response = self.http.delete(path, self.additional_headers, **kwargs)
        return response

    def get(self, path_segment, headers=None, **kwargs):
        """
        Perform a GET operation from the REST path segment.

        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment. type ``string``
        :param headers: List of extra HTTP headers to send (optional). type ``list`` of 2-tuples.
        :param kwargs: All other arguments. type ``dict``
        :return: The response from the server.
        """
        if headers is None:
            headers = []

        path = self.uri + self._abspath(path_segment)
        logging.debug("GET request to %s (body: %s)", path, repr(kwargs))
        all_headers = headers + self.additional_headers
        response = self.http.get(path, all_headers, **kwargs)
        return response

    def post(self, path_segment, headers=None, **kwargs):
        """
        Perform a POST operation from the REST path segment.

        :raises HTTPError: Raised when an error occurred in a GET operation from *path_segment*.
        :param path_segment: A REST path segment. type ``string``
        :param headers: List of extra HTTP headers (optional). type ``list`` of 2-tuples.
        :param kwargs: All other arguments. type ``dict``
        :return: The response from the server.
        """
        if headers is None:
            headers = []

        path = self.uri + self._abspath(path_segment)
        logging.debug("POST request to %s (body: %s)", path, repr(kwargs))
        all_headers = headers + self.additional_headers
        response = self.http.post(path, all_headers, **kwargs)
        return response
    
    def put(self, path_segment, headers=None, **kwargs):
        """
        Perform a POST operation from the REST path segment.

        :raises HTTPError: Raised when an error occurred in a GET operation from *path_segment*.
        :param path_segment: A REST path segment. type ``string``
        :param headers: List of extra HTTP headers (optional). type ``list`` of 2-tuples.
        :param kwargs: All other arguments. type ``dict``
        :return: The response from the server.
        """
        if headers is None:
            headers = []

        path = self.uri + self._abspath(path_segment)
        logging.debug("PUT request to %s (body: %s)", path, repr(kwargs))
        all_headers = headers + self.additional_headers
        response = self.http.put(path, all_headers, **kwargs)
        return response    

    def request(self, path_segment, method="GET", headers=None, body=""):
        """
        Issue an arbitrary HTTP request to the REST path segment.

        This method is named to match ``httplib.request``. This function
        makes a single round trip to the server.

        :raises HTTPError: Raised when an error occurred in a GET operation from
             *path_segment*.
        :param path_segment: A REST path segment.
        :type path_segment: ``string``
        :param method: The HTTP method to use (optional).
        :type method: ``string``
        :param headers: List of extra HTTP headers to send (optional).
        :type headers: ``list`` of 2-tuples.
        :param body: Content of the HTTP request (optional).
        :type body: ``string``
        :return: The response from the server.
        """
        if headers is None:
            headers = []

        path = self.uri + self._abspath(path_segment)
        all_headers = headers + self.additional_headers
        logging.debug("%s request to %s (headers: %s, body: %s)",
                      method, path, str(all_headers), repr(body))
        response = self.http.request(path,
                                     {'method': method,
                                     'headers': all_headers,
                                     'body': body})
        return response
    
    def _abspath(self, path_segment):
        skip_encode = isinstance(path_segment, UrlEncoded)
        return UrlEncoded(path_segment, skip_encode=skip_encode)

class HTTPError(Exception):
    """This exception is raised for HTTP responses that return an error."""
    def __init__(self, response, _message=None):
        status = response.status
        reason = response.reason
        body = response.body
        self.status = status
        self.reason = reason
        self.headers = response.headers
        self.body = body
        self._response = response

def _encode(**kwargs):
    items = []
    for key, value in iter(kwargs.items()):
        if isinstance(value, list):
            items.extend([(key, item) for item in value])
        else:
            items.append((key, value))
    return urllib.parse.urlencode(items)

# Split the url into (scheme, host, port, path)
def _spliturl(url):
    parsed_url = urllib.parse.urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port
    path = '?'.join((parsed_url.path, parsed_url.query)) if parsed_url.query else parsed_url.path
    # Strip brackets if its an IPv6 address
    if host.startswith('[') and host.endswith(']'): host = host[1:-1]
    if port is None: port = DEFAULT_PORT
    return parsed_url.scheme, host, port, path

# Given an HTTP request handler, this wrapper objects provides a related
# family of convenience methods built using that handler.
class HttpBase(object):
    """
    ``HttpBase`` provides `request` `delete` `post` `get` methods

    The handling function: ``handler(`url`, `request_dict`) -> response_dict``

    `url` is a dictionary with the following keys:

        - method: The method for the request, typically ``GET`` ``POST`` ``DELETE``.
        - headers: A list of pairs specifying the HTTP headers (for example: ``[('key': value), ...]``).
        - body: A string containing the body to send (default to '').

    ``response_dict`` is a dictionary with the following keys:

        - status: An integer containing the HTTP status code (such as 200 or 404).
        - reason: The reason phrase, if any, returned by the server.
        - headers: A list of pairs containing the response headers (for example, ``[('key': value), ...]``).
        - body: Response body.
    """
    def __init__(self, custom_handler=None, verify=False, key_file=None, cert_file=None):
        if custom_handler is None:
            self.handler = handler(verify=verify, key_file=key_file, cert_file=cert_file)
        else:
            self.handler = custom_handler

    def delete(self, url, headers=None, **kwargs):
        """
        Send a DELETE request to a URL.

        :param url: The URL. type ``string``
        :param headers: A list of pairs specifying the headers (for example, ``[('Content-Type', 'application/json')]``). type ``list``
        :param kwargs: Additional keyword arguments (optional). type ``dict``
        :returns: A dict (see :class:`HttpBase` for its structure).
        """
        if headers is None: headers = []
        if kwargs:
            url = url + UrlEncoded('?' + _encode(**kwargs), skip_encode=True)
        message = {
            'method': "DELETE",
            'headers': headers,
        }
        return self.request(url, message)

    def get(self, url, headers=None, **kwargs):
        """
        Send a GET request to a URL.

        :param url: The URL. type ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type', 'application/json')]``). type ``list``
        :param kwargs: Additional arguments (optional). type ``dict``
        :returns: A dict (see :class:`HttpBase` for its structure).
        """
        if headers is None: headers = []
        if kwargs:
            url = url + UrlEncoded('?' + _encode(**kwargs), skip_encode=True)
        return self.request(url, { 'method': "GET", 'headers': headers })

    def post(self, url, headers=None, **kwargs):
        """
        Send a POST request to a URL.

        :param url: The URL. type ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type', 'application/json')]``). type ``list``
        :param kwargs: Additional arguments (optional). type ``dict``
        :returns: A dict (see :class:`HttpBase` for its structure).
        """
        if headers is None: headers = []

        if 'body' in kwargs:
            if len([x for x in headers if x[0].lower() == "content-type"]) == 0:
                headers.append(("Content-Type", "application/x-www-form-urlencoded"))

            body = kwargs.pop('body')
            if len(kwargs) > 0:
                url = url + UrlEncoded('?' + _encode(**kwargs), skip_encode=True)
        else:
            body = _encode(**kwargs).encode('utf-8')
        message = {
            'method': "POST",
            'headers': headers,
            'body': body
        }
        return self.request(url, message)
    
    def put(self, url, headers=None, **kwargs):
        """
        Send a PUT request to a URL.

        :param url: The URL. type ``string``
        :param headers: A list of pairs specifying the headers for the HTTP
            response (for example, ``[('Content-Type', 'application/json')]``). type ``list``
        :param kwargs: Additional arguments (optional). type ``dict``
        :returns: A dict (see :class:`HtHttpBasetpLib` for its structure).
        """
        if headers is None: headers = []

        if 'body' in kwargs:
            if len([x for x in headers if x[0].lower() == "content-type"]) == 0:
                headers.append(("Content-Type", "application/x-www-form-urlencoded"))

            body = kwargs.pop('body')
            if len(kwargs) > 0:
                url = url + UrlEncoded('?' + _encode(**kwargs), skip_encode=True)
        else:
            body = _encode(**kwargs).encode('utf-8')
        message = {
            'method': "PUT",
            'headers': headers,
            'body': body
        }
        return self.request(url, message)

    def request(self, url, message, **kwargs):
        """
        An HTTP request to a URL.

        :param url: The URL. type ``string``
        :param message: A dict described in :class:`HttpBase`. type ``dict``
        :param kwargs: Additional arguments (optional). type ``dict``
        :returns: A dict (see :class:`HttpBase` for its structure).
        """
        response = self.handler(url, message, **kwargs)
        response = record(response)
        if 400 <= response.status:
            raise HTTPError(response)

        return response

def handler(key_file=None, cert_file=None, timeout=None, verify=False):
    """
    Return an instance of the default HTTP request handler.

    :param `key_file`: Private key path (optional). type ``string``
    :param `cert_file`: Certificate chain file (optional). type ``string``
    :param `timeout`: Request time-out period, in seconds (optional). type ``integer`` or "None"
    :param `verify`: Set to False to disable SSL verification on https connections. type ``Boolean``
    """

    def connect(scheme, host, port):
        kwargs = {}
        if timeout is not None: kwargs['timeout'] = timeout
        if scheme == "http":
            return http.client.HTTPConnection(host, port, **kwargs)
        if scheme == "https":
            if key_file is not None: kwargs['key_file'] = key_file
            if cert_file is not None: kwargs['cert_file'] = cert_file

            if not verify:
                kwargs['context'] = ssl._create_unverified_context()
            return http.client.HTTPSConnection(host, port, **kwargs)
        raise ValueError("unsupported scheme: %s" % scheme)

    def request(url, message, **kwargs):
        scheme, host, port, path = _spliturl(url)
        body = message.get("body", "")
        head = {
            "Content-Length": str(len(body)),
            "Host": host,
            "Accept": "*/*",
            "Connection": "Close",
        } # defaults
        for key, value in message["headers"]:
            head[key] = value
        method = message.get("method", "GET")

        connection = connect(scheme, host, port)
        is_keepalive = False
        try:
            connection.request(method, path, body, head)
            if timeout is not None:
                connection.sock.settimeout(timeout)
            response = connection.getresponse()    
            is_keepalive = "keep-alive" in response.getheader("connection", default="close").lower()        
    
            return {
                "status": response.status,
                "reason": response.reason,
                "headers": response.getheaders(),
                "body": response.read().decode()
            }            
        finally:
            if not is_keepalive:
                connection.close()

    return request
