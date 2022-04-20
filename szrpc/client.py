import functools
import re
import time
import uuid
from queue import Queue
from threading import Thread

import zmq

from . import log
from .result import Result
from .server import ResponseType, Request, Response, SERVER_TIMEOUT

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

    def __init__(self, address):
        self.client_id = str(uuid.uuid4())
        self.context = zmq.Context()
        self.url = address
        self.requests = Queue()
        self.remote_methods = []
        self.results = {}
        self.ready = False
        self.start()

    def start(self):
        Thread(target=self.send_requests, daemon=True).start()
        Thread(target=self.emit_results, daemon=True).start()
        res = self.call_remote('client_config')
        res.connect('done', self.setup)

    def setup(self, result, data):
        self.ready = True
        self.remote_methods = data
        logger.debug(f'~> {self.url}... Ready!')

    def is_ready(self):
        return self.ready

    def call_remote(self, method: str, **kwargs):
        """
        Call the remote method on the server
        :param method: method name
        :param kwargs: parameters to pass to server
        :return: Returns a result object for deferred execution.
        """
        request_id = str(uuid.uuid4())
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
        socket.identity = self.client_id.encode('utf-8')
        socket.connect(self.url)

        poll = zmq.Poller()
        poll.register(socket, zmq.POLLIN)

        while True:
            sockets = dict(poll.poll(10))
            if socket in sockets:
                reply_data = socket.recv_multipart()
                try:
                    response = Response.create(self.client_id.encode('utf-8'), *reply_data)
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

            time.sleep(0.01)

    def __getattr__(self, name):
        if name == 'client_config' or name in self.remote_methods:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')
