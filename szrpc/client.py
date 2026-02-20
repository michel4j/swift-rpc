from __future__ import annotations

import functools
import time
import uuid
import importlib
from queue import Queue
from threading import Thread

import zmq

from . import log
from .result import Result
from .server import ResponseType, Request, Response, get_client_id

logger = log.get_module_logger('szrpc')


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

    def __init__(self, address, methods=(), heartbeat: int = 0):
        """
        :param address: Server address for the client, eg. tcp://localhost:9990
        :param methods: sequence of method names to allow for this client
        :param heartbeat: heartbeat interval in seconds, if 0, no heartbeat is used (default). Allows the client to
        detect server disconnections.
        """
        self.client_id = get_client_id()
        self.context = zmq.Context()
        self.url = address
        self.heartbeat = heartbeat
        self.requests = Queue()
        self.remote_methods = set(methods)
        self.results = {}
        self.ready = False
        self.starting = True
        self.last_available = time.time()
        self.last_ping = time.time()
        self.start(introspect=(not methods))

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
            print(res)
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
        self.requests.put(request)
        self.results[request_id] = self.RESULT_CLASS(request_id)
        logger.debug(f'-> {request}')
        return self.results[request_id]

    def send_requests(self):
        """
        Monitors the request queue and sends pending requests to the server

        """
        socket = self.context.socket(zmq.DEALER)
        socket.identity = self.client_id
        socket.connect(self.url)

        self.last_available = time.time()
        self.last_ping = time.time()

        while True:
            ping_pending = self.heartbeat > 0 and (time.time() - self.heartbeat > self.last_ping)
            if socket.poll(10, zmq.POLLIN):
                reply_data = socket.recv_multipart()
                self.last_available = time.time()
                self.last_ping = time.time()
                try:
                    response = Response.create(self.client_id, *reply_data)
                except Exception as e:
                    logger.error('Invalid response!')
                    logger.exception(e)
                else:
                    logger.debug(f'<- {response}')
                    res = self.results.get(response.request_id, None)
                    if res is not None:
                        if response.type == ResponseType.UPDATE:
                            res.update(response.content)
                        elif response.type == ResponseType.DONE:
                            res.done(response.content)
                        elif response.type == ResponseType.ERROR:
                            res.failure(response.content)
            elif self.is_ready() and ping_pending:
                # send ping if no activity within heartbeat interval
                try:
                    self.ping()
                except AttributeError:
                    self.client_config()    # ping is not available, use client_config
                self.last_ping = time.time()

            if (self.is_ready() or self.starting) and not self.requests.empty():
                request = self.requests.get()
                socket.send_multipart(request.parts())
                self.starting = False

    def emit_results(self):
        """
        Triggers pending result signals and cleans-up the results dictionary. Also monitors for connection issues
        """
        while True:
            expired = set()
            # process result signals
            for req_id in list(self.results.keys()):
                res = self.results[req_id]
                res.process()
                if res.is_ready():
                    expired.add(req_id)
                time.sleep(0.01)

            # remove expired items
            for req_id in expired:
                del self.results[req_id]
                time.sleep(0.01)

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
