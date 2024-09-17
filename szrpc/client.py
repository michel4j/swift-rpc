import functools
import time
from queue import Queue
from threading import Thread

import zmq

from . import log
from .result import Result
from .server import ResponseType, Request, Response, short_uuid

logger = log.get_module_logger('szrpc')

RESULT_CLASS = Result


def use(result_class):
    """
    Swap out the Result Class

    :param result_class: Class object
    """

    global RESULT_CLASS
    RESULT_CLASS = result_class


class Client(object):
    """
    Base class for all clients.
    """

    def __init__(self, address, methods=(), heartbeat: int = 0):
        """
        :param address: Server address for the client, eg. tcp://localhost:9990
        :param methods: sequence of method names to allow for this client
        :param heartbeat: heartbeat interval in seconds, if 0, no heartbeat is used (default)
        """
        self.client_id = short_uuid()
        self.context = zmq.Context()
        self.url = address
        self.heartbeat = heartbeat
        self.requests = Queue()
        self.remote_methods = set(methods)
        self.results = {}
        self.ready = False
        self.last_available = time.time()
        self.last_ping = time.time()
        self.start(introspect=(not methods))

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
        else:
            self.ready = True
            logger.debug(f'~> {self.url}... Ready!')

    def setup(self, result, methods):
        """
        Configure the client with the remote methods
        :param result: result object
        :param methods: sequence of method names returned from the server
        """
        self.ready = True
        self.remote_methods = methods
        logger.debug(f'~> {self.url}... Ready!')

    def is_ready(self) -> bool:
        """
        Check if the server is ready to receive commands
        """
        return self.ready

    def call_remote(self, method: str, **kwargs) -> Result:
        """
        Call the remote method on the server
        :param method: method name
        :param kwargs: parameters to pass to server
        :return: Returns a result object for deferred execution.
        """
        request_id = short_uuid()
        kwargs = {} if kwargs is None else kwargs
        request = Request(self.client_id, request_id, method, kwargs)
        self.requests.put(request)
        self.results[request_id] = RESULT_CLASS(request_id)
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

            if socket.poll(10, zmq.POLLIN):
                reply_data = socket.recv_multipart()
                self.last_available = time.time()
                self.last_ping = time.time()
                try:
                    response = Response.create(self.client_id, *reply_data)
                except TypeError:
                    logger.error('Invalid response!')
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
            elif self.heartbeat and self.last_ping + self.heartbeat < time.time():
                if self.ready and time.time() > self.last_available + self.heartbeat:
                    try:
                        self.ping()
                    except AttributeError:
                        self.client_config()    # ping is not available, use client_config
                    self.last_ping = time.time()

            if socket .poll(10, zmq.POLLOUT):
                if not self.requests.empty():
                    request = self.requests.get()
                    socket.send_multipart(request.parts())

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
            has_heartbeat = time.time() - self.last_available < 2 * self.heartbeat
            if self.is_ready() and not has_heartbeat:
                self.ready = False
                logger.error('Server connection lost!')
            elif not self.is_ready() and has_heartbeat:
                self.ready = True
                logger.info('Server connection restored!')
            time.sleep(0.01)

    def __getattr__(self, name):
        if name in ['client_config', 'ping'] or name in self.remote_methods:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')
