import re
import time
from datetime import datetime
from queue import Queue
from threading import Thread

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


class Server(object):
    def __init__(self, service: Service, port: int = 9990):
        """
        :param service: A Service class which provides the API for the server
        :param port: Connection request port, the reply port is always the next port, must be available
        """
        self.service = service
        self.req_port = port
        self.rep_port = port + 1
        self.replies = Queue(maxsize=2000)

        reply_thread = Thread(target=self.send_replies, daemon=True)
        reply_thread.start()

    def run(self):
        """
        Listen for requests and process them. Each request is handled in a separate thread.
        """
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
        socket.bind(f'tcp://0.0.0.0:{self.req_port}')
        logger.info(f'<~ "tcp://0.0.0.0:{self.req_port}"...')

        while True:
            req_data = socket.recv_multipart()
            try:
                request = Request.create(*req_data, reply_to=self.replies)
                logger.info(f'<- {request}')
            except Exception:
                logger.error('Invalid request!')
            else:
                thread = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                thread.start()
            time.sleep(0.001)

    def send_replies(self):
        """
        Monitor the response queue and publish the replies over the network
        """
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind(f'tcp://0.0.0.0:{self.rep_port}')
        logger.info(f'~> "tcp://0.0.0.0:{self.rep_port}"...')
        last_time = 0
        while True:
            if not self.replies.empty():
                response = self.replies.get()
                socket.send_multipart(
                    response.parts()
                )
                logger.debug(f'-> {response}')
                last_time = time.time()
            elif time.time() - last_time > SERVER_TIMEOUT:
                socket.send_multipart(
                    Response.heart_beat()
                )
                last_time = time.time()
            time.sleep(0.01)
