import re
import time
from datetime import datetime

from threading import Thread
from multiprocessing import Process, Queue

import msgpack
import zmq
import hashlib
from . import log

logger = log.get_module_logger(__name__)

SERVER_TIMEOUT = 4


class ResponseType:
    DONE = 1
    UPDATE = 2
    ERROR = 3
    HEARTBEAT = 4


ResponseMessage = {
    1: 'DONE',
    2: 'UPDATE',
    3: 'ERROR',
    4: 'HEARTBEAT'
}


class Request(object):
    __slots__ = ('client_id', 'request_id', 'method', 'kwargs', 'reply_to')

    def __init__(self, client_id: str, request_id: str, method: str, kwargs: dict, reply_to: Queue = None):
        """
        Request object

        :param client_id: client identification
        :param request_id: request identification
        :param method: remote method to call
        :param kwargs: kwargs
        :param reply_to:  A queue for responses, defaults to None
        """
        self.client_id = client_id
        self.request_id = request_id
        self.method = method
        self.kwargs = kwargs
        self.reply_to = reply_to

    def parts(self):
        """
        Return the request parts suitable to transmission over network

        :return: a list consisting of [client_id, request_id, method_name, args_data
        """
        return [
            self.client_id.encode('utf-8'), self.request_id.encode('utf-8'),
            self.method.encode('utf-8'), msgpack.dumps(self.kwargs)
        ]

    @staticmethod
    def create(client_id: bytes, request_id: bytes, method: bytes, arg_data: bytes, reply_to: Queue = None):
        """
        Generate a request object from the raw information received through the network

        :param client_id:  client identifier
        :param request_id: request identifier
        :param method: method name
        :param arg_data: raw data for the arguments, msgpack encoded bytes
        :reply_to:  reply queue for responses to be sent to
        :return: new Request object
        """
        args = msgpack.loads(arg_data)
        return Request(
            client_id.decode('utf-8'),
            request_id.decode('utf-8'),
            method.decode('utf-8'),
            args if isinstance(args, dict) else {},
            reply_to=reply_to
        )

    def reply(self, content, response_type: int = ResponseType.DONE):
        """
        Generate a response object from the current request and send it
        to the reply queue.

        :param content: content of the reply
        :param response_type: Response type
        :return: Response object
        """
        response = Response(
            self.client_id, self.request_id, response_type, content
        )
        if self.reply_to is not None:
            self.reply_to.put(response)
        return response

    def __str__(self):
        h = hashlib.blake2b(digest_size=10)
        h.update(f'{self.client_id}|{self.request_id}'.encode('utf-8'))
        return f'REQ[{h.hexdigest()}] - {self.method}()'


class Response(object):
    __slots__ = ('client_id', 'request_id', 'type', 'content')

    def __init__(self, client_id, request_id, response_type, content):
        self.client_id = client_id
        self.request_id = request_id
        self.type = response_type
        self.content = content

    def parts(self):
        """
        Return the response parts suitable to transmission over network

        :return: a list consisting of [client_id, request_id, response_type, response_data
        """
        return [
            self.client_id.encode('utf-8'), self.request_id.encode('utf-8'),
            msgpack.dumps(self.type), msgpack.dumps(self.content)
        ]

    @staticmethod
    def create(client_id: bytes, request_id: bytes, response_type: bytes, content: bytes):
        """
        Generate a response object from the raw information received through the network

        :param client_id:
        :param request_id:
        :param response_type:
        :param response_type:
        :return: new Response object
        """
        return Response(
            client_id.decode('utf-8'),
            request_id.decode('utf-8'),
            msgpack.loads(response_type),
            msgpack.loads(content)
        )

    @staticmethod
    def heart_beat():
        """
        Generate a heartbeat response network

        :param response_type:
        :param response_type:
        :return: new Response object
        """
        return Response(
            'heartbeat',
            '',
            ResponseType.HEARTBEAT,
            {'time': datetime.now().isoformat()}
        ).parts()

    def __str__(self):
        h = hashlib.blake2b(digest_size=10)
        h.update(f'{self.client_id}|{self.request_id}'.encode('utf-8'))
        return f'REP[{h.hexdigest()}] - {ResponseMessage[self.type]}'


class Service(object):
    """
    A base class for all service objects. Service objects carry out the business logic of the server.
    They can maintain internal state across requests.

    Remote methods have the following requirements:
    - Must start with "remote__" prefix.
    - Must accept the request object as the first argument
    - The rest of the arguments must be keyworded arguments

    A service object can return either a single response or multiple responses per request. This can be implemented by
    overriding the call_remote method.
    """

    def __init__(self):
        self.allowed_methods = tuple(
            re.sub('^remote__', '', attr)
            for attr in dir(self) if attr.startswith('remote__')
        )

    def call_remote(self, request: Request):
        """
        Call the remote method in the request and place the response object in the reply queue when ready.
        This is the main method which is invoked by the server once a request is received. This method will be called
        in a separate thread for each request.

        :param request: Request object
        """

        try:
            method = self.__getattribute__(f'remote__{request.method}')
        except AttributeError:
            logger.error(f'Service does not support remote method "{request.method}"')
            request.reply(
                content=f'Service does not support remote method "{request.method}"',
                response_type=ResponseType.ERROR,
            )
        else:
            try:
                reply = method(request, **request.kwargs)
                response_type = ResponseType.DONE
            except Exception as e:
                reply = f'Error: {e}'
                response_type = ResponseType.ERROR
            request.reply(content=reply, response_type=response_type)

    def remote__client_config(self, request):
        """
        Called by clients on connect. Return a list of allowed methods to call
        """
        return self.allowed_methods


class Worker(object):
    """
    A worker which manages an instance of the Service. Each work is able to perform the same tasks
    """

    def __init__(self, service: Service, backend: str):
        """
        :param service:  A Service class which provides the API for the server
        :param backend: Backend address to connect to
        """
        self.service = service
        self.context = zmq.Context()
        self.backend = backend
        self.replies = Queue()

    def run(self):
        socket = self.context.socket(zmq.DEALER)
        socket.connect(self.backend)

        while True:
            req_data = socket.recv_multipart()
            try:
                request = Request.create(*req_data, reply_to=self.replies)
                logger.info(f'<- {request}')
            except Exception:
                logger.error('Invalid request!')
                print(req_data)
            else:
                thread = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                thread.start()

                # block until task completes and reply queue is empty
                while thread.is_alive() or not self.replies.empty():
                    if not self.replies.empty():
                        response = self.replies.get()
                        socket.send_multipart(response.parts())
                        logger.debug(f'-> {response}')
                        last_time = time.time()
                    time.sleep(0.001)
        socket.close()


def start_worker(service_class, backend, *args, **kwargs):
    service = service_class(*args, **kwargs)
    worker = Worker(service, backend)
    return worker.run()


class Server(object):
    def __init__(self, service: Service, port: int = 9990, workers: int = 1):
        """
        :param service: A Service class which provides the API for the server
        :param port: Connection request port, the reply port is always the next port, must be available
        :param workers: Number of workers to start on server. Additional workers can be started on other hosts
        """
        self.service = service
        self.frontend_addr = f'tcp://*:{port}'
        self.backend_addr = f'tcp://*:{port+1}'
        self.num_workers = workers


    def run(self):
        """
        Listen for requests on the frontend and proxy them to the backend process them. Each request is handled in a separate thread.
        """
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(self.frontend_addr)

        backend = context.socket(zmq.DEALER)
        backend.bind(self.backend_addr)

        workers = []
        for i in range(self.num_workers):
            p = Process(target=start_worker, args=(self.service.__class__, self.backend_addr.replace('*', 'localhost')))
            p.start()
            workers.append(p)

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()
        context.term()

