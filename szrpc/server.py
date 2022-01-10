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
    def __init__(self, client_id: str, request_id: str, method: str, kwargs: dict):
        """
        Request object

        :param client_id: client identification
        :param request_id: request identification
        :param method: remote method to call
        :param kwargs: kwargs
        """
        self.client_id = client_id
        self.request_id = request_id
        self.method = method
        self.kwargs = kwargs

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
    def create(client_id: bytes, request_id: bytes, method: bytes, arg_data: bytes):
        """
        Generate a request object from the raw information received through the network

        :param client_id:
        :param request_id:
        :param method:
        :param arg_data:
        :return: new Request object
        """
        return Request(
            client_id.decode('utf-8'),
            request_id.decode('utf-8'),
            method.decode('utf-8'),
            msgpack.loads(arg_data)
        )

    def reply(self, content, response_type:int = ResponseType.REPLY):
        """
        Generate a response object from the current request

        :param content: content of the reply
        :param response_type: Response type
        :return: Response object
        """
        return Response(
            self.client_id, self.request_id, response_type, content
        )


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
    def __init__(self):
        pass

    def call_remote(self, request: Request, reply_to: Queue):
        """
        Call the remote method in the request and place the response object in the reply queue when ready.

        :param request: Request object
        :param reply_to: Queue into which results should be placed if any
        """

        try:
            method = self.__getattribute__(f'remote__{request.method}')
        except AttributeError:
            logger.error(f'Service does not support remote method "{request.method}"')
            response = request.reply(
                content=f'Service does not support remote method "{request.method}"',
                response_type=ResponseType.ERROR,
            )
            reply_to.put(response)
        else:
            if request.kwargs:
                logger.debug(f'{request.client_id}: {request.method}(**{request.kwargs})')
                reply = method(request.kwargs)
            else:
                logger.debug(f'{request.client_id}: {request.method}()')
                reply = method()
            response = request.reply(content=reply, response_type=ResponseType.REPLY)
            reply_to.put(response)

    def remote__get_api(self):
        """
        Return information about available remote methods as a dictionary with name: documentation pairs
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
            request = Request.create(*req_data)
            logger.debug(f'Request received: {request.client_id}|{request.request_id}')
            thread = Thread(target=self.service.call_remote, args=(request,), kwargs={'reply_to': self.replies}, daemon=True)
            thread.start()
            time.sleep(0.001)

    def send_replies(self):
        """
        Monitor the response queue and publish the replies over the network to

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