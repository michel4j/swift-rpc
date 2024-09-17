import re
import os
import time
import uuid
import base64

from typing import Type
from threading import Thread
from multiprocessing import Process, Queue
from enum import Enum

import msgpack
import zmq
from . import log

logger = log.get_module_logger(__name__)

SERVER_TIMEOUT = 4
MIN_HEARTBEAT_INTERVAL = 1
MAX_HEARTBEAT_INTERVAL = 2


class ResponseType(Enum):
    DONE = 1
    UPDATE = 2
    ERROR = 3
    HEARTBEAT = 4
    READY = 5


def repr_worker_id(b):
    """
    Represent bytes in base64
    :param b: bytes
    """
    return base64.b64encode(b).decode('ascii')


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

    def reply(self, content, response_type: ResponseType = ResponseType.UPDATE):
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
            msgpack.dumps(self.type.value), msgpack.dumps(self.content)
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
            ResponseType(msgpack.loads(response_type)),
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
            self.client_id[:5].decode("utf-8"), self.request_id[:5].decode("utf-8"), self.type.name
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

    PING_PACKET = b''

    def __init__(self, *args, **kwargs):
        self.allowed_methods = tuple(
            re.sub('^remote__', '', attr)
            for attr in dir(self) if attr.startswith('remote__')
        )

    def call_remote(self, request: Request):
        """
        Call the remote method in the request and place the response object in the reply queue when ready.
        This is the main method which is invoked by the server once a request is received.

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
                logger.exception(e)
                response_type = ResponseType.ERROR
            request.reply(content=reply, response_type=response_type)

    def remote__client_config(self, request: Request):
        """
        Called by clients on connect. Return a list of allowed methods to call
        """
        return self.allowed_methods

    def remote__ping(self, request: Request):
        """
        Respond to a ping request to indicate the server is alive
        """
        return self.PING_PACKET


class ServiceFactory(object):
    """
    A Factory which takes a service type class and arguments for instantiating it, and then creates
    new instances as needed.
    """

    def __init__(self, service_type: Type[Service], *args, **kwargs):
        """
        :param service_type: Service class
        :param args: positional arguments for Service
        :param kwargs: Keyword arguments for Service
        """
        self.service_type = service_type
        self.args = args
        self.kwargs = kwargs

    def new(self):
        """
        Create a new Service instance
        :return: Service object
        """
        return self.service_type(*self.args, **self.kwargs)


class Worker(object):
    """
    A worker which manages an instance of the Service. Each worker is able to perform the same tasks
    """

    def __init__(self, backend: str, service: Service):
        """
        :param backend: Backend address to connect to
        :param service: A Service class which provides the API for the server
        """

        self.service = service
        self.context = zmq.Context()
        self.backend = backend
        self.replies = Queue()

    def run(self):
        """
        Main loop of the worker
        """
        socket = self.context.socket(zmq.DEALER)
        socket.connect(self.backend)

        socket.send_multipart(Response.heartbeat())
        last_message = time.time()

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        while True:
            if not self.replies.empty():
                response = self.replies.get()
                logger.debug(f'-> {response}')
                socket.send_multipart(response.parts())
                last_message = time.time()

            socks = dict(poller.poll(10))
            if socket in socks and socks[socket] == zmq.POLLIN:
                req_data = socket.recv_multipart()
                try:
                    request = Request.create(*req_data, reply_to=self.replies)
                    logger.info(f'<- {request}')
                except Exception:
                    logger.error('Invalid request!')
                else:
                    task = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                    task.start()

            # Send a heartbeat every so often when idle
            if time.time() - last_message > MIN_HEARTBEAT_INTERVAL:
                socket.send_multipart(Response.heartbeat())
                last_message = time.time()

            time.sleep(0.01)


def start_worker(address: str, factory: ServiceFactory):
    """
    Start a single worker in a subprocess
    :param address: backend address
    :param factory: Service Factory

    """
    service = factory.new()
    worker = Worker(address, service)
    logger.debug(f'Starting new worker process: {os.getpid()}')
    return worker.run()


class Server(object):
    def __init__(self, service_factory: ServiceFactory, ports: tuple = (9990, 9991), instances: int = 1):
        """
        :param service_factory: A Service factory which creates service instances
        :param kwargs: Keyword arguments for the Service instance
        :param ports: a pair of ports for frontend and backend
        :param instances: Number of workers to start on server. Additional workers can be started on other hosts

        """
        self.service_factory = service_factory
        self.frontend_addr = f'tcp://*:{ports[0]}'
        self.backend_addr = f'tcp://*:{ports[1]}'
        self.context = zmq.Context()
        self.manager = WorkerManager(self.service_factory, self.backend_addr, instances=instances)

    def run(self, balancing=False):
        """
        Listen for requests on the frontend and proxy them to the backend process them.
        Each request is handled in a separate thread.
        """
        if balancing:
            self.load_balancing_proxy()
        else:
            self.simple_proxy()

        self.manager.wait_for_workers()

    def simple_proxy(self):
        frontend = self.context.socket(zmq.ROUTER)
        backend = self.context.socket(zmq.DEALER)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)

        self.manager.start_workers()

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()

    def load_balancing_proxy(self):
        frontend = self.context.socket(zmq.ROUTER)
        backend = self.context.socket(zmq.ROUTER)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)

        self.manager.start_workers()

        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        community = set()
        workers = {}

        backend_ready = False

        try:
            while True:
                sockets = dict(poller.poll(10))

                if backend in sockets:
                    # Handle worker activity on the backend
                    reply = backend.recv_multipart()
                    worker = reply[0]

                    response = Response.create(*reply[1:])

                    # Update heartbeat time every time we receive something from a worker that's on the list
                    # or if it is a new member of a community
                    if worker in workers or worker not in community:
                        workers[worker] = time.time()

                    # Add worker to community if needed
                    if worker not in community:
                        community.add(worker)
                        logger.debug(f'Workers [{len(workers):4d}], + : {repr_worker_id(worker)}')

                    # Add worker to list if a previous task completes or fails
                    if response.type in [ResponseType.DONE, ResponseType.ERROR] and worker not in workers:
                        workers[worker] = time.time()

                    if workers and not backend_ready:
                        # Poll for clients now that a worker is available and backend was not ready
                        poller.register(frontend, zmq.POLLIN)
                        backend_ready = True

                    if response.type != ResponseType.HEARTBEAT:
                        frontend.send_multipart(response.parts())

                # check and expire workers who haven't chatted in while
                if workers:
                    expired = time.time() - MAX_HEARTBEAT_INTERVAL
                    removed = [w for w, t in workers.items() if t <= expired]
                    workers = {w: t for w, t in workers.items() if t > expired}
                    if removed:
                        removed_workers = ', '.join(map(repr_worker_id, removed))
                        logger.debug(f'Workers [{len(workers):4d}], - : {removed_workers}')
                        community.difference_update(removed)

                if frontend in sockets:
                    # Get next client request, route to last-used worker, the oldest item in workers dictionary
                    request = frontend.recv_multipart()

                    worker = next(iter(workers))
                    workers.pop(worker)     # remove worker from list as it is now busy
                    backend.send_multipart([worker] + request)

                    # Don't poll clients if no workers are available and set backend_ready flag to false
                    if not workers:
                        poller.unregister(frontend)
                        backend_ready = False
        finally:
            frontend.close()
            backend.close()


class WorkerManager(object):
    def __init__(self, factory: ServiceFactory, address: str, instances: int = 1):
        """
        :param factory:  A Service class which provides the API for the server
        :param address: Backend address to connect to
        :param instances: Number of worker instances to manage
        """
        self.factory = factory
        self.backend_addr = address.replace('*', 'localhost')
        self.instances = instances
        self.processes = []

    def start_workers(self):
        """
        Start subprocesses for each worker
        :return:
        """
        logger.info(f'Connecting {self.instances} worker(s) to {self.backend_addr}')
        self.processes = []
        for i in range(self.instances):
            p = Process(target=start_worker, args=(self.backend_addr, self.factory))
            p.start()
            self.processes.append(p)

    def wait_for_workers(self):
        """
        Wait for all worker processes to terminate
        """
        for proc in self.processes:
            proc.join()

    def run(self):
        self.start_workers()
        self.wait_for_workers()
