from __future__ import annotations

import functools
import socket
import time
import uuid
import importlib
from queue import Empty, Queue
from threading import Event, Lock, Thread, current_thread

from . import log
from .protocol import Request
from .result import Result
from .transport import ClientTransport, ZmqClientTransport

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

    def __init__(
        self,
        address: str = '',
        methods=(),
        heartbeat: int = 0,
        client_id: str | None = None,
        linger: bool = True,
        transport: ClientTransport | None = None,
    ):
        """
        :param address: Server address for the client, For example: tcp://localhost:9990
        :param methods: sequence of method names to allow for this client
        :param heartbeat: heartbeat interval in seconds, if 0, no heartbeat is used (default). Allows the client to
        detect server disconnections.
        :param client_id: client identifier slug. If not provided a new one will be generated. Must be unique between
        simultaneously connected clients. use the Client.create_id() class method to generate compatible unique ids.
        :param linger: if True, keep unsent messages in the queue on exit
        :param transport: Optional ClientTransport adapter; defaults to ZmqClientTransport
        """
        if transport is not None:
            self.client_id = transport.client_id
            self.transport = transport
        else:
            self.client_id = client_id.encode('utf-8') if client_id else self.create_id()
            self.transport = ZmqClientTransport(address, self.client_id, linger=linger)

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
        self._stop_event = Event()
        self._threads: list[Thread] = []
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
        self._threads = [
            Thread(target=self.send_requests, daemon=True),
            Thread(target=self.emit_results, daemon=True),
        ]
        for t in self._threads:
            t.start()
        if introspect:
            res = self.call_remote('client_config')
            res.connect('done', self.setup)
            res.wait()
        else:
            self.ready = True
            logger.debug(f'~> {self.url}... Ready!')

    def stop(self) -> None:
        """Signal client background loops to stop and wait for them to exit."""
        self._stop_event.set()
        caller = current_thread()
        for t in self._threads:
            if t is not caller and t.is_alive():
                t.join(timeout=1.0)

    def close(self) -> None:
        """Alias for stop()."""
        self.stop()

    def __enter__(self) -> 'Client':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

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
        self.last_available = time.time()
        self.last_ping = time.time()

        try:
            while not self._stop_event.is_set():
                ping_pending = 0 < self.heartbeat < time.time() - self.last_ping

                response = self.transport.recv(10)
                if response is not None:
                    self.last_available = time.time()
                    self.last_ping = time.time()
                    self.responses.put(response)
                elif self.is_ready() and ping_pending:
                    try:
                        self.ping()
                    except AttributeError:
                        self.client_config()    # ping is not available, use client_config
                    self.last_ping = time.time()

                if (self.is_ready() or self.starting) and not self.requests.empty():
                    request = self.requests.get()
                    self.transport.send(request)
                    self.starting = False
        finally:
            self.transport.close()

    def emit_results(self):
        """
        Triggers pending result signals and cleans up the results dictionary. Also monitors for connection issues
        """
        while not self._stop_event.is_set():
            try:
                response = self.responses.get(timeout=0.01)
            except Empty:
                response = None

            if response is not None:
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
