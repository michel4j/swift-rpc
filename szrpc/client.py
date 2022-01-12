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
        self.remote_methods = ['client_config']  # allow config to go through before config is complete
        self.last_update = time.time()
        self.results = {}
        self.ready = False
        self.start()

    def start(self):
        sender = Thread(target=self.send_requests, daemon=True)
        sender.start()
        receiver = Thread(target=self.monitor_responses, daemon=True)
        receiver.start()
        emitter = Thread(target=self.emit_results, daemon=True)
        emitter.start()
        self.setup(wait=True)

    def setup(self, wait=False):
        res = self.client_config()
        if res.wait(timeout=5):
            self.ready = True
            self.remote_methods = res.results
        else:
            self.setup()

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
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(self.req_url)
        logger.debug(f'~> {self.req_url}...')

        time.sleep(2)

        while True:
            request = self.requests.get()
            socket.send_multipart(
                request.parts()
            )
            time.sleep(0.01)

    def monitor_responses(self):
        """
        Fetch responses from the server and updates the result objects
        """
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, self.client_id)
        socket.setsockopt_string(zmq.SUBSCRIBE, 'heartbeat')
        socket.connect(self.rep_url)
        logger.debug(f'<~ {self.rep_url}...')

        while True:
            reply_data = socket.recv_multipart()
            try:
                response = Response.create(*reply_data)
            except TypeError:
                logger.error('Invalid response!')
            else:
                if response.type != ResponseType.HEARTBEAT:
                    logger.debug(f'<- {response}')
                res = self.results.get(response.request_id, None)
                if res is not None:
                    if response.type == ResponseType.UPDATE:
                        res.update(response.content)
                    elif response.type == ResponseType.DONE:
                        res.done(response.content)
                    elif response.type == ResponseType.ERROR:
                        res.failure(response.content)
                if response.type == ResponseType.HEARTBEAT and not self.ready:
                    logger.info('Connected to Server!')


                self.last_update = time.time()
            time.sleep(0.01)

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
            if self.ready and time.time() - self.last_update > 2*SERVER_TIMEOUT:
                self.ready = False
                logger.error('Server connection lost!')

    def __getattr__(self, name):
        if name == 'client_config' or name in self.remote_methods:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')
