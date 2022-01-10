import time
from queue import Queue
from threading import Thread
from datetime import datetime
import msgpack
import zmq

import log

logger = log.get_module_logger(__name__)

HEARTBEAT_INTERVAL = 5


class ResponseType:
    REPLY = 1
    PROGRESS = 2
    ERROR = 3
    HEARTBEAT = 4


class Request(object):
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

    def reply(self, content, response_type:int = ResponseType.REPLY):
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


class Response(object):
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
        pass

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
                logger.debug(f'{request.client_id}: {request.method}(**{request.kwargs})')
                reply = method(request, **request.kwargs)
                response_type = ResponseType.REPLY
            except Exception as e:
                reply = f'Error: {e}'
                response_type = ResponseType.ERROR
            request.reply(content=reply, response_type=response_type)

    def remote__get_api(self, request):
        """
        Return the list of allowed remote methods.
        """
        allowed = []
        for attr in dir(self):
            if attr.startswith('remote__'):
                allowed.append(attr.removeprefix('remote__'))
        return allowed


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
        socket.bind(f'tcp://*:{self.req_port}')
        logger.debug(f'Waiting for requests from "tcp://*:{self.req_port}"...')
        while True:
            req_data = socket.recv_multipart()
            request = Request.create(*req_data, reply_to=self.replies)
            logger.debug(f'Request received: {request.client_id}|{request.request_id}')
            thread = Thread(target=self.service.call_remote, args=(request,), daemon=True)
            thread.start()
            time.sleep(0.001)

    def send_replies(self):
        """
        Monitor the response queue and publish the replies over the network
        """
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind(f'tcp://*:{self.rep_port}')
        logger.debug(f'Sending replies to "tcp://*:{self.rep_port}"...')
        last_time = 0
        while True:
            if not self.replies.empty():
                response = self.replies.get()
                socket.send_multipart(
                   response.parts()
                )
                logger.debug(f'Response sent: {response.client_id}|{response.request_id}')
                last_time = time.time()
            elif time.time() - last_time > HEARTBEAT_INTERVAL:
                socket.send_multipart(
                    Response.heart_beat()
                )
                last_time = time.time()
            time.sleep(0.005)


if __name__ == '__main__':
    service = Service()
    log.log_to_console()
    server = Server(service=service, port=9990)
    server.run()