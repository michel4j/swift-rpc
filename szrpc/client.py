from __future__ import annotations

import functools
import socket
import time
import uuid
import importlib
from queue import Queue
from threading import Thread, Lock

import zmq

from . import log
from .result import Result
from .server import Request, Response

logger = log.get_module_logger('szrpc')


class ResultManager:
    def __init__(self):
        self.data = {}
        self.lock = Lock()

    def add(self, result: Result):
        with self.lock:
            self.data[result.result_id] = result

    def get(self, result_id: bytes) -> Result | None:
        with self.lock:
            return self.data.get(result_id)

    def remove(self, result_id: bytes):
        with self.lock:
            del self.data[result_id]

    def process(self, response):
        result = self.get(response.request_id)
        if result is not None:
            result.process(response)

            if result.is_ready():
                self.remove(response.request_id)


def load_class(dotted_path: str) -> type:
    """
    Dynamically loads a class from a dotted path string.
    e.g., "my_package.my_module.MyClass"
    
    """
    parts = dotted_path.rsplit('.', 1)
    try:
        module_name, class_name = parts
        module = importlib.import_module(module_name)
        class_object = getattr(module, class_name)
        return class_object
    except IndexError as e:
        raise IndexError(f"Invalid dotted path: {dotted_path}: {e}")
    except ImportError as e:
        raise ImportError(f"Could not import module {parts[0]}: {e}")
    except AttributeError as e:
        raise AttributeError(f"Could not find class {parts[1]} in module {parts[0]}: {e}")


class Client(object):
    """
    Base class for all clients.
    """

    RESULT_CLASS = Result

    def __init__(self, address, methods=(), heartbeat: int = 0, client_id: str | None = None, linger: bool = True):
        """
        :param address: Server address for the client, For example: tcp://localhost:9990
        :param methods: sequence of method names to allow for this client
        :param heartbeat: heartbeat interval in seconds, if 0, no heartbeat is used (default). Allows the client to
        detect server disconnections.
        :param client_id: client identifier slug. If not provided a new one will be generated. Must be unique between
        simultaneously connected clients. use the Client.create_id() class method to generate compatible unique ids.
        :param linger: if True, keep unsent messages in the queue on exit
        """
        self.client_id = client_id.encode('utf-8') if client_id else self.create_id()
        self.context = zmq.Context()
        self.url = address
        self.heartbeat = heartbeat
        self.requests = Queue()
        self.responses = Queue()
        self.remote_methods = set(methods)
        self.results = ResultManager()
        self.ready = False
        self.linger = linger
        self.starting = True
        self.last_available = time.time()
        self.last_ping = time.time()
        self.start(introspect=(not methods))

    @classmethod
    def create_id(cls) -> bytes:
        """
        Generate a unique client ID
        """
        host = socket.getfqdn().split('.')[0].lower()
        client = str(uuid.uuid1())[:8]
        return f'{host}-{client}'.encode('ascii')

    @classmethod
    def use(cls, result_class: type | str):
        """
        Swap out the Result Class. Used for integration with different main-loops like
        Gtk, Qt or other bespoke main loops.

        :param result_class: Class object or dotted path string
        """
        if isinstance(result_class, str):
            result_class = load_class(result_class)
        cls.RESULT_CLASS = result_class

    def get_id(self) -> bytes:
        """
        Get the client ID
        """
        return self.client_id

    def create_request_id(self):
        """
        Generate a unique request ID
        """
        call_id = str(uuid.uuid1())[:8]
        return f'{self.client_id.decode("ascii")}-{call_id}'.encode('ascii')

    def start(self, introspect=True):
        """
        Start the client threads
        :param introspect: whether to introspect the server for available methods

        """
        Thread(target=self.send_requests, daemon=True).start()
        Thread(target=self.emit_results, daemon=True).start()
        if introspect:
            res = self.call_remote('client_config')
            res.connect('done', self.setup)
            res.wait()
        else:
            self.ready = True
            logger.debug(f'~> {self.url}... Ready!')

    def setup(self, result, methods):
        """
        Configure the client with the remote methods
        :param result: result object
        :param methods: sequence of method names returned from the server
        """
        self.remote_methods = methods
        self.ready = True
        logger.debug(f'~> {self.url}... Ready!')

    def is_ready(self) -> bool:
        """
        Check if the server is ready to receive commands
        """
        return self.ready and self.remote_methods

    def call_remote(self, method: str, **kwargs) -> Result:
        """
        Call the remote method on the server
        :param method: method name
        :param kwargs: parameters to pass to server
        :return: Returns a result object for deferred execution.
        """
        request_id = self.create_request_id()
        kwargs = {} if kwargs is None else kwargs
        request = Request(self.client_id, request_id, method, kwargs)
        logger.debug(f'~> {request}')
        self.requests.put(request)
        result = self.RESULT_CLASS(request_id)
        self.results.add(result)
        return result

    def send_requests(self):
        """
        Monitors the request queue and sends pending requests to the server. Also
        receives responses from the server and adds them to the response queue

        """
        sock = self.context.socket(zmq.DEALER)
        sock.identity = self.client_id
        sock.connect(self.url)

        self.last_available = time.time()
        self.last_ping = time.time()

        if not self.linger:
            sock.setsockopt(zmq.LINGER, 0)
        try:
            while True:
                ping_pending = 0 < self.heartbeat < time.time() - self.last_ping

                if sock.poll(10, zmq.POLLIN):
                    # receive replies
                    reply_data = sock.recv_multipart()
                    self.last_available = time.time()
                    self.last_ping = time.time()
                    self.responses.put(reply_data)
                elif self.is_ready() and ping_pending:
                    # send ping if no activity within heartbeat interval
                    try:
                        self.ping()
                    except AttributeError:
                        self.client_config()    # ping is not available, use client_config
                    self.last_ping = time.time()

                if (self.is_ready() or self.starting) and not self.requests.empty():
                    request = self.requests.get()
                    sock.send_multipart(request.parts())
                    self.starting = False
        finally:
            sock.close()
            self.context.term()

    def emit_results(self):
        """
        Triggers pending result signals and cleans up the results dictionary. Also monitors for connection issues
        """
        while True:
            if not self.responses.empty():
                response_data = self.responses.get()
                try:
                    response = Response.create(self.client_id, *response_data)
                except Exception as e:
                    logger.error('Invalid response!')
                    logger.exception(e)
                else:
                    logger.debug(f'<~ {response}')
                    self.results.process(response)

            # check connection
            if self.heartbeat > 0:
                has_heartbeat = self.last_available + 2 * self.heartbeat > time.time()
                if self.ready and not has_heartbeat:
                    self.ready = False
                    logger.error('Server connection lost!')
                elif not self.ready and has_heartbeat and self.remote_methods:
                    self.ready = True
                    logger.info('Server connection restored!')
            time.sleep(0.01)

    def __getattr__(self, name):
        if name in ['client_config', 'ping'] or name in self.remote_methods:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')


def use(result_class: type | str):
    """
    Swap out the Result Class

    :param result_class: Class object or dotted string
    """

    Client.use(result_class)
