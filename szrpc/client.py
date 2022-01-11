import functools
import re
import time
import uuid
from queue import Queue
from threading import Thread

import zmq

from . import log
from .result import Result
from .server import ResponseType, Request, Response

logger = log.get_module_logger(__name__)

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
        info = re.match(r'^(?P<url>...://[^:]+):(?P<port>\d+)$', address).groupdict()
        self.url = info['url']
        self.port = int(info['port'])
        self.req_url = f'{self.url}:{self.port}'
        self.rep_url = f'{self.url}:{self.port + 1}'
        self.requests = Queue(maxsize=1000)
        self.allowed = ['get_api']
        self.results = {}
        self.start()

    def start(self):
        sender = Thread(target=self.send_requests, daemon=True)
        sender.start()
        receiver = Thread(target=self.monitor_responses, daemon=True)
        receiver.start()
        emitter = Thread(target=self.emit_results, daemon=True)
        emitter.start()

        res = self.get_api()
        res.connect('done', self.__on_api)

    def __on_api(self, res, apis):
        """
        Update API
        :param res: result object
        """
        self.allowed = apis

    def call_remote(self, method: str, **kwargs):
        request_id = str(uuid.uuid4())
        kwargs = {} if kwargs is None else kwargs
        self.requests.put(
            Request(self.client_id, request_id, method, kwargs)
        )
        self.results[request_id] = RESULT_CLASS(request_id)
        return self.results[request_id]

    def send_requests(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(self.req_url)
        logger.debug(f'Sending requests to {self.req_url}...')

        while True:
            request = self.requests.get()
            if request.method in self.allowed:
                socket.send_multipart(
                    request.parts()
                )
            time.sleep(0.01)

    def emit_results(self):
        while True:
            expired = set()
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

    def monitor_responses(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, self.client_id)
        socket.setsockopt_string(zmq.SUBSCRIBE, 'heartbeat')
        socket.connect(self.rep_url)
        logger.debug(f'Receiving replies from {self.rep_url}...')

        while True:
            reply_data = socket.recv_multipart()
            response = Response.create(*reply_data)
            res = self.results.get(response.request_id, None)
            if res is not None:
                if response.type == ResponseType.UPDATE:
                    res.update(response.content)
                elif response.type == ResponseType.DONE:
                    res.done(response.content)
                elif response.type == ResponseType.ERROR:
                    res.failure(response.content)
            elif response.type == ResponseType.HEARTBEAT:
               self.last_update = time.time()
            time.sleep(0.01)

    def __getattr__(self, name):
        if name in self.allowed:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')
