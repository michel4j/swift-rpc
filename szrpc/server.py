import re
import os
import sys
import time
import uuid
import queue
import socket

from typing import Type, Literal
from threading import Thread
from multiprocessing import Process
from enum import Enum

import msgpack
import zmq
from . import log, namer
from . import monitor as mon

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


def human_bytes(size: int) -> str:
    """
    Format byte size in human-readable form
    :param size: integer number of bytes
    :return: string representation of size
    """
    units = ('KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    if size < 1000:
        return f"{size} B"
    for i, unit in enumerate(units):
        size /= 1024.0
        if size < 1000:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} {units[-1]}"


def get_client_id():
    """
    Generate a unique client ID
    """
    host = socket.getfqdn().split('.')[0].lower()
    client = str(uuid.uuid1())[:8]
    return f'{host}/{client}'.encode('ascii')


class Request(object):
    __slots__ = ('client_id', 'request_id', 'method', 'kwargs', 'reply_to', 'identity')

    def __init__(self, client_id: bytes, request_id: bytes, method: str, kwargs: dict, reply_to: queue.Queue = None):
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
    def create(client_id: bytes, request_id: bytes, method: bytes, arg_data: bytes, reply_to: queue.Queue = None):
        """
        Generate a request object from the raw information received through the network

        :param client_id:  client identifier
        :param request_id: request identifier
        :param method: method name
        :param arg_data: raw data for the arguments, msgpack encoded bytes
        :param reply_to:  reply queue for responses to be sent to
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
        req_id = '/'.join(self.request_id.decode('ascii').split('/')[-2:])
        call_signature = log.log_call(self.method, (), self.kwargs)
        return f"req[{req_id}] - {call_signature}"


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
        :param content:
        :return: new Response object
        """
        return Response(
            client_id,
            request_id,
            ResponseType(msgpack.loads(response_type)),
            msgpack.loads(content)
        )

    @staticmethod
    def heartbeat(system_id: bytes = b''):
        """
        Generate a heartbeat response packet
        :return: new Response object
        """
        return Response(
            system_id,
            b'heartbeat',
            ResponseType.HEARTBEAT,
            b''
        ).parts()

    def __str__(self):
        req_id = '/'.join(self.request_id.decode('ascii').split('/')[-2:])
        size = sys.getsizeof(self.content)
        return f"req[{req_id}] - {self.type.name} {human_bytes(size)}"


class Service(object):
    """
    A base class for all service objects. Service objects carry out the business logic of the server.
    They can maintain internal state across requests.

    Remote methods have the following requirements:
    - Must start with "remote__" prefix.
    - Must accept the request object as the first argument
    - The rest of the arguments must be key-worded arguments

    A service object can return either a single response or multiple responses per request. This can be implemented by
    overriding the call_remote method.
    """

    PING_PACKET = b''

    def __init__(self, *args, **kwargs):
        self.allowed_methods = tuple(
            re.sub('^remote__', '', attr)
            for attr in dir(self) if attr.startswith('remote__')
        )
        self.name_generator = namer.RandomGenerator()

    def create_worker_id(self) -> bytes:
        """
        Generate a unique worker id for the current host
        :return: worker id bytes
        """
        host = socket.getfqdn().split('.')[0].upper()
        unique = self.name_generator.generate_name()
        return f'{unique}@{host}'.encode('utf-8')

    def call_remote(self, request: Request):
        """
        Call the remote method in the request and place the response object in the reply queue when ready.
        This is the main method which is invoked by the server once a request is received.

        :param request: Request object
        """

        try:
            method = getattr(self, f'remote__{request.method}')
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
        :param service: A Service instance which provides the API for the server
        """

        self.service = service
        self.context = zmq.Context()
        self.backend = backend
        self.replies = queue.Queue()

    def run(self):
        """
        Main loop of the worker
        """
        sock = self.context.socket(zmq.DEALER)
        sock.identity = self.service.create_worker_id()
        sock.connect(self.backend)
        sock.send_multipart(Response.heartbeat())
        last_message = time.time()

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        while True:
            while not self.replies.empty():
                response = self.replies.get()
                logger.debug(f'-> {response}')
                sock.send_multipart(response.parts())
                last_message = time.time()

            socks = dict(poller.poll(10))
            if sock in socks and socks[sock] == zmq.POLLIN:
                req_data = sock.recv_multipart()
                try:
                    request = Request.create(*req_data, reply_to=self.replies)
                    logger.info(f'<- {request}')
                except Exception as e:
                    logger.error(f'Invalid request: {e}')
                    logger.exception(e)
                else:
                    task = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                    task.start()

            # Send a heartbeat every so often when idle
            if time.time() - last_message > MIN_HEARTBEAT_INTERVAL:
                sock.send_multipart(Response.heartbeat())
                last_message = time.time()


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
    def __init__(self, service_factory: ServiceFactory, ports: tuple = (9990, 9991), instances: int = 1, monitor_port: int = None):
        """
        :param service_factory: A Service factory which creates service instances
        :param ports: a pair of ports for frontend and backend
        :param instances: Number of workers to start on server. Additional workers can be started on other hosts
        :param monitor_port: Port for the introspection web server. If None, introspection is disabled.

        """
        self.service_factory = service_factory
        self.frontend_addr = f'tcp://*:{ports[0]}'
        self.backend_addr = f'tcp://*:{ports[1]}'
        self.context = zmq.Context()
        self.manager = WorkerManager(self.service_factory, self.backend_addr, instances=instances)
        self.monitor = None
        if monitor_port:
            self.monitor = mon.Monitor()
            mon.start_monitor_thread(self.monitor, port=monitor_port)

    def run(self, balancing=False, proxy: Literal['simple', 'balancing', 'round-robin'] = 'round-robin'):
        """
        Listen for requests on the frontend and proxy them to the backend process them.
        Each request is handled in a separate thread.
        :param balancing: if True, force the use the load balancing proxy
        :param proxy: use this proxy type. One of 'simple', 'balancing', 'round-robin', default 'round-robin'.

        """
        proxy = 'balancing' if balancing else proxy

        if proxy == 'balancing':
            self.load_balancing_proxy()
        elif proxy == 'round-robin':
            self.round_robin_proxy()
        elif proxy == 'simple':
            self.simple_proxy()
        else:
            self.round_robin_proxy()    # default

        self.manager.wait_for_workers()

    def _setup_sockets(self, front=zmq.ROUTER, back=zmq.ROUTER) -> tuple:
        """
        Setup zmq frontend and backend sockets and start worker processes
        :param front:
        :param back:
        :return:
        """
        frontend = self.context.socket(front)
        backend = self.context.socket(back)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)
        self.manager.start_workers()
        return frontend, backend

    def _process_response(self, worker, reply, frontend):
        try:
            response = Response.create(*reply[1:])
        except Exception as e:
            logger.error(f"Invalid response from worker {worker}: {e}")
            return None

        if response.type != ResponseType.HEARTBEAT:
            frontend.send_multipart(response.parts())
            if self.monitor:
                self.monitor.record_response(
                    worker.decode('utf-8'),
                    response.identity,
                    response.type.name,
                    response.content
                )
        elif self.monitor:
            self.monitor.update_worker(worker.decode('utf-8'))

        return response

    def _monitor_request(self, worker, request):
        if self.monitor:
            try:
                req_obj = Request.create(*request)
                self.monitor.record_request(
                    worker.decode('utf-8'),
                    req_obj.client_id.decode('utf-8'),
                    req_obj.identity,
                    req_obj.method,
                    req_obj.kwargs
                )
            except Exception as e:
                logger.error(f"Failed to record request in monitor: {e}")

    def simple_proxy(self):
        """
        A simple proxy which forwards all messages from the front-end to the back-end in round-robin fashion.
        """
        frontend, backend = self._setup_sockets(zmq.ROUTER, zmq.DEALER)
        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()

    def round_robin_proxy(self):
        """
        A proxy which forwards all messages from the front-end to the back-end in round-robin fashion.
        """
        frontend, backend = self._setup_sockets()
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        backend_ready = False
        workers = {}
        worker_queue = []

        try:
            while True:
                sockets = dict(poller.poll(10))

                if backend in sockets:
                    reply = backend.recv_multipart()
                    worker = reply[0]
                    if worker not in workers:
                        workers[worker] = time.time()
                        worker_queue.append(worker)
                        logger.debug(f'Workers [{len(workers):4d}], + : {worker.decode("utf-8")}')
                    else:
                        workers[worker] = time.time()

                    self._process_response(worker, reply, frontend)

                # check and expire workers who haven't chatted in while
                expired = time.time() - MAX_HEARTBEAT_INTERVAL
                removed = [w for w, t in workers.items() if t <= expired]
                workers = {w: t for w, t in workers.items() if t > expired}
                worker_queue = list(workers.keys())
                if removed:
                    removed_workers = ', '.join([w.decode('utf-8') for w in removed])
                    logger.debug(f'Workers [{len(workers):4d}], - : {removed_workers}')

                if workers and not backend_ready:
                    # Poll for clients now that a worker is available and backend was not ready
                    poller.register(frontend, zmq.POLLIN)
                    backend_ready = True
                elif backend_ready and not workers:
                    poller.unregister(frontend)
                    backend_ready = False

                if frontend in sockets:
                    request = frontend.recv_multipart()

                    if worker_queue:
                        worker = worker_queue.pop(0)
                        worker_queue.append(worker)
                        backend.send_multipart([worker] + request)
                        self._monitor_request(worker, request)
        finally:
            frontend.close()
            backend.close()

    def load_balancing_proxy(self):
        """
        A proxy which performs basic load balancing. That is a busy worker is removed from the worker pool until
        it's current task is completed. At that point, it's added back to the pool for the next task
        :return:
        """
        frontend, backend = self._setup_sockets()
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

                    # Update heartbeat time every time we receive something from a worker that's on the list
                    # or if it is a new member of a community
                    if worker in workers or worker not in community:
                        workers[worker] = time.time()

                    # Add worker to community if needed
                    if worker not in community:
                        community.add(worker)
                        logger.debug(f'Workers [{len(workers):4d}], + : {worker.decode("utf-8")}')

                    response = self._process_response(worker, reply, frontend)
                    if not response:
                        continue

                    # Add worker to list if a previous task completes or fails
                    if response.type in [ResponseType.DONE, ResponseType.ERROR] and worker not in workers:
                        workers[worker] = time.time()

                    if workers and not backend_ready:
                        # Poll for clients now that a worker is available and backend was not ready
                        poller.register(frontend, zmq.POLLIN)
                        backend_ready = True

                # check and expire workers who haven't chatted in while
                if workers:
                    expired = time.time() - MAX_HEARTBEAT_INTERVAL
                    removed = [w for w, t in workers.items() if t <= expired]
                    workers = {w: t for w, t in workers.items() if t > expired}
                    if removed:
                        removed_workers = ', '.join([w.decode('utf-8') for w in removed])
                        logger.debug(f'Workers [{len(workers):4d}], - : {removed_workers}')
                        community.difference_update(removed)

                if frontend in sockets:
                    # Get next client request, route to last-used worker, the oldest item in workers dictionary
                    request = frontend.recv_multipart()

                    worker = next(iter(workers))
                    workers.pop(worker)     # remove worker from list as it is now busy
                    backend.send_multipart([worker] + request)
                    self._monitor_request(worker, request)

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
        :param factory:  A ServiceFactory instance
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
