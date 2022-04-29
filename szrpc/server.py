import re
import time
import uuid
import base64
from datetime import datetime

from threading import Thread
from multiprocessing import Process, Queue

import msgpack
import zmq
from . import log

logger = log.get_module_logger(__name__)

SERVER_TIMEOUT = 4
MIN_HEARTBEAT_INTERVAL = 1
MAX_HEARTBEAT_INTERVAL = 2

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


def short_uuid():
    """
    Generate a 22 character UUID4 representation
    """
    return base64.b64encode(uuid.uuid4().bytes).strip(b'=')


class Request(object):
    __slots__ = ('client_id', 'request_id', 'method', 'kwargs', 'reply_to', 'identity')

    def __init__(self, client_id: bytes, request_id: bytes, method: str, kwargs: dict, reply_to: Queue = None):
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
        self.identity = request_id.decode('utf-8')
        self.method = method
        self.kwargs = kwargs
        self.reply_to = reply_to

    def parts(self):
        """
        Return the request parts suitable to transmission over network

        :return: a list consisting of [request_id, method_name, args_data
        """
        return [
            self.request_id,
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
            client_id,
            request_id,
            method.decode('utf-8'),
            args if isinstance(args, dict) else {},
            reply_to=reply_to
        )

    def reply(self, content, response_type: int = ResponseType.UPDATE):
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
        return "req[{}..:{}..] - {}()".format(
            self.client_id[:5].decode("utf-8"), self.request_id[:5].decode("utf-8"), self.method
        )


class Response(object):
    __slots__ = ('client_id', 'request_id', 'type', 'content', 'identity')

    def __init__(self, client_id, request_id, response_type, content):
        self.client_id = client_id
        self.request_id = request_id
        self.type = response_type
        self.content = content
        self.identity = request_id.decode('utf-8')

    def parts(self):
        """
        Return the response parts suitable to transmission over network

        :return: a list consisting of [client_id, request_id, response_type, response_data
        """
        return [
            self.client_id, self.request_id,
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
            client_id,
            request_id,
            msgpack.loads(response_type),
            msgpack.loads(content)
        )

    @staticmethod
    def heartbeat():
        """
        Generate a heartbeat response packet
        :return: new Response object
        """
        return Response(
            b'',
            b'heartbeat',
            ResponseType.HEARTBEAT,
            b''
        ).parts()

    def __str__(self):
        return "rep[{}..:{}..] - {}".format(
            self.client_id[:5].decode("utf-8"), self.request_id[:5].decode("utf-8"), ResponseMessage[self.type]
        )


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

        socket.send_multipart(Response.heartbeat())
        last_message = time.time()

        task = None
        while True:
            if not self.replies.empty():
                response = self.replies.get()
                logger.debug(f'-> {response}')
                socket.send_multipart(response.parts())
                last_message = time.time()

            if task is None:
                if socket.poll(10, zmq.POLLIN):
                    req_data = socket.recv_multipart()
                    try:
                        request = Request.create(*req_data, reply_to=self.replies)
                        logger.info(f'<- {request}')
                    except Exception:
                        logger.error('Invalid request!')
                    else:
                        task = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                        task.start()
            elif not task.is_alive():
                task = None

            # Send a heartbeat every so often
            if time.time() - last_message > MIN_HEARTBEAT_INTERVAL:
                socket.send_multipart(Response.heartbeat())
                last_message = time.time()

            time.sleep(0.01)


def start_worker(service, backend):
    worker = Worker(service, backend)
    return worker.run()


class Server(object):
    def __init__(self, service: Service, ports: tuple = (9990, 9991), instances: int = 1):
        """
        :param service: A Service class which provides the API for the server
        :param ports: pair of ports for frontend and backend
        :param instances: Number of workers to start on server. Additional workers can be started on other hosts
        """
        self.service = service
        self.frontend_addr = f'tcp://*:{ports[0]}'
        self.backend_addr = f'tcp://*:{ports[1]}'
        self.instances = instances
        self.context = zmq.Context()
        self.processes = []

    def start_workers(self):
        worker_addr = self.backend_addr.replace('*', 'localhost')
        logger.info(f'Connecting {self.instances} worker(s) to {worker_addr}')
        for i in range(self.instances):
            p = Process(target=start_worker, args=(self.service, worker_addr))
            p.start()
            self.processes.append(p)

    def run(self, load_balancing=False):
        """
        Listen for requests on the frontend and proxy them to the backend process them. Each request is handled in a separate thread.
        """
        if load_balancing:
            self.load_balancing_proxy()
        else:
            self.simple_proxy()

        for process in self.processes:
            process.join()

    def simple_proxy(self):
        frontend = self.context.socket(zmq.ROUTER)
        backend = self.context.socket(zmq.DEALER)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)

        self.start_workers()

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()

    def load_balancing_proxy(self):
        frontend = self.context.socket(zmq.ROUTER)
        backend = self.context.socket(zmq.ROUTER)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)

        self.start_workers()

        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        workers = []
        living = {}

        backend_ready = False

        while True:
            sockets = dict(poller.poll())

            if backend in sockets:
                # Handle worker activity on the backend
                reply = backend.recv_multipart()
                worker = reply[0]

                response = Response.create(*reply[1:])
                living[worker] = time.time()

                if response.type in [ResponseType.DONE, ResponseType.ERROR, ResponseType.HEARTBEAT]:
                    workers.append(worker)

                if workers and not backend_ready:
                    # Poll for clients now that a worker is available and backend was not ready
                    poller.register(frontend, zmq.POLLIN)
                    backend_ready = True
                if response.type != ResponseType.HEARTBEAT:
                    frontend.send_multipart(response.parts())

            # check and expire workers
            if workers:
                expired = time.time() - MAX_HEARTBEAT_INTERVAL
                living = {w: t for w, t in living.items() if t > expired}
                workers = [w for w in workers if w in living]

            if frontend in sockets:
                # Get next client request, route to last-used worker
                request = frontend.recv_multipart()
                worker = workers.pop(0)
                backend.send_multipart([worker] + request)
                if not workers:
                    # Don't poll clients if no workers are available and set backend_ready flag to false
                    poller.unregister(frontend)
                    backend_ready = False

        frontend.close()
        backend.close()


class WorkerManager(object):
    def __init__(self, service: Service, backend: str, instances: int = 1):
        """
        :param service:  A Service class which provides the API for the server
        :param backend: Backend address to connect to
        :param instances: Number of worker instances to manage
        """
        self.service = service
        self.backend_addr = backend.replace('*', 'localhost')
        self.instances = instances

    def run(self):
        logger.info(f'Connecting {self.instances} worker(s) to {self.backend_addr}')
        workers = []
        for i in range(self.instances):
            p = Process(target=start_worker, args=(self.service, self.backend_addr))
            p.start()
            workers.append(p)

        for p in workers:
            p.join()
