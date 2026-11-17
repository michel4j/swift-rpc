from __future__ import annotations

import re
import os
import sys
import time
import queue
import socket

from typing import Type, Literal, Sequence
from threading import Event, Thread
from multiprocessing import Process

import zmq
from . import log, namer, __version__
from .protocol import Request, Response, ResponseType
from .transport import (
    BrokerTransport,
    WorkerTransport,
    ZmqBrokerTransport,
    ZmqWorkerTransport,
)

try:
    from . import monitor as mon
    DASH_ENABLED = True
except ImportError:
    DASH_ENABLED = False


logger = log.get_module_logger(__name__)

SERVER_TIMEOUT = 4
MIN_HEARTBEAT_INTERVAL = 1
MAX_HEARTBEAT_INTERVAL = 2


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

    @classmethod
    def __str__(cls):
        if not cls.__doc__:
            return cls.__name__

        description = ' '.join(cls.__doc__.split())
        return f'{cls.__name__} - {description}'

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

    def get_description(self) -> str:
        """
        Get a description of the service type from the class name and the docstring
        :return: description string
        """
        return self.service_type.__str__()

    def get_methods(self) -> tuple:
        """
        Get a list of allowed remote methods for the service type
        :return: list of method names
        """
        return tuple({
            re.sub('^remote__', '', attr)
            for attr in dir(self.service_type) if attr.startswith('remote__')
        } | {'client_config'})

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

    def __init__(
        self,
        backend: str,
        service: Service,
        methods: Sequence[str] = (),
        *args,
        transport: WorkerTransport | None = None,
        **kwargs,
    ):
        """
        :param backend: Backend address to connect to
        :param service: A Service instance which provides the API for the server
        :param methods: List of remote methods supported by this worker instance
        :param transport: Optional WorkerTransport adapter; defaults to ZmqWorkerTransport
        """

        self.service = service
        self.backend = backend
        self.replies = queue.Queue()
        self.methods = methods
        self.transport = transport
        self._stop_event = Event()

    def is_supported(self, method: str) -> bool:
        """
        Check if a method is supported by this worker
        :param method: method name
        :return: bool
        """
        if not self.methods:
            return True
        elif method in {'ping', 'client_config'}:
            return True
        else:
            return method in self.methods

    def stop(self) -> None:
        """Signal the worker loop to stop."""
        self._stop_event.set()

    def close(self) -> None:
        """Alias for stop()."""
        self.stop()

    def __enter__(self) -> 'Worker':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def run(self):
        """
        Main loop of the worker
        """
        transport = self.transport or ZmqWorkerTransport(self.backend, self.service.create_worker_id())

        try:
            # initial heartbeat message
            transport.send(Response.heartbeat_message(methods=self.methods))
            last_message = time.time()

            while not self._stop_event.is_set():
                while not self.replies.empty():
                    response = self.replies.get()
                    logger.debug(f'-> {response}')
                    transport.send(response)
                    last_message = time.time()

                request = transport.recv(10)
                if request is not None:
                    request.reply_to = self.replies
                    logger.info(f'<- {request}')
                    if not self.is_supported(request.method):
                        logger.warning(f'Processing unsupported method `{request.method}`!')
                    task = Thread(target=self.service.call_remote, args=(request,), daemon=True)
                    task.start()

                # Send a heartbeat every so often when idle
                if time.time() - last_message > MIN_HEARTBEAT_INTERVAL:
                    transport.send(Response.heartbeat_message(methods=self.methods))
                    last_message = time.time()
        finally:
            transport.close()


def start_worker(address: str, factory: ServiceFactory, methods: Sequence[str] = (), *args, **kwargs):
    """
    Start a single worker in a subprocess
    :param address: backend address
    :param factory: Service Factory
    :param methods: List of remote methods supported by this worker instance
    """
    service = factory.new()
    worker = Worker(address, service, methods=methods, *args, **kwargs)
    logger.debug(f'Starting new worker process: {os.getpid()}')
    return worker.run()


class Server(object):
    def __init__(
        self,
        service_factory: ServiceFactory,
        ports: tuple = (9990, 9991),
        instances: int = 1,
        monitor_port: int = None,
        transport: BrokerTransport | None = None,
    ):
        """
        :param service_factory: A Service factory which creates service instances
        :param ports: a tuple of ports for frontend, backend
        :param instances: Number of workers to start on server. Additional workers can be started on other hosts
        :param monitor_port: alternate way to specify a monitor port
        :param transport: Optional BrokerTransport adapter; defaults to ZmqBrokerTransport
        """
        self.service_factory = service_factory
        self.frontend_addr = f'tcp://*:{ports[0]}'
        self.backend_addr = f'tcp://*:{ports[1]}'
        self.transport = transport
        self._stop_event = Event()
        self.manager = WorkerManager(self.service_factory, self.backend_addr, instances=instances)
        self.monitor = None

        if DASH_ENABLED and monitor_port:
            info = {
                'host': socket.getfqdn().split('.')[0].upper(),
                'version': f"v{__version__}",
                'frontend': self.frontend_addr,
                'backend': self.backend_addr,
                'description': self.service_factory.get_description(),
                'methods': self.service_factory.get_methods()
            }
            self.monitor = mon.Monitor(service_info=info)
            mon.start_monitor_thread(self.monitor, port=monitor_port)
        elif monitor_port:
            logger.warning('Dashboard not enabled. Please run "pip install szrpc[dash] to enable it"')

    def stop(self) -> None:
        """Signal the broker loop to stop and terminate managed worker processes."""
        self._stop_event.set()
        self.manager.stop_workers()

    def close(self) -> None:
        """Alias for stop()."""
        self.stop()

    def __enter__(self) -> 'Server':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

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
        Setup raw zmq frontend and backend sockets and start worker processes (used by simple_proxy).
        """
        ctx = zmq.Context()
        frontend = ctx.socket(front)
        backend = ctx.socket(back)
        frontend.bind(self.frontend_addr)
        backend.bind(self.backend_addr)
        logger.info(f'Listening for tasks from: {self.frontend_addr}')
        logger.info(f'Listening for workers at: {self.backend_addr}')

        self.manager.start_workers()
        return ctx, frontend, backend

    def _process_response(self, worker: bytes, response: Response, transport: BrokerTransport):
        if response.type != ResponseType.HEARTBEAT:
            transport.send_to_client(response)
            if self.monitor:
                self.monitor.record_response(
                    worker.decode('utf-8'),
                    response.identity,
                    response.type.name,
                    response.content
                )
        elif self.monitor:
            self.monitor.update_worker(worker.decode('utf-8'))

    def _monitor_request(self, worker: bytes, request: Request):
        if self.monitor:
            try:
                self.monitor.record_request(
                    worker.decode('utf-8'),
                    request.client_id.decode('utf-8'),
                    request.identity,
                    request.method,
                    request.kwargs
                )
            except Exception as e:
                logger.error(f"Failed to record request in monitor: {e}")

    def _monitor_cleanup(self, workers):
        """
        Remove active jobs if their workers disappear. This is called when a worker is removed from the pool due to
        inactivity or failure.
        :param workers: sequence of workers to remove from the monitor
        """
        if self.monitor:
            self.monitor.clear_workers(workers)

    def simple_proxy(self):
        """
        A simple proxy which forwards all messages from the front-end to the back-end in round-robin fashion.
        """
        ctx, frontend, backend = self._setup_sockets(zmq.ROUTER, zmq.DEALER)
        try:
            zmq.proxy(frontend, backend)
        finally:
            frontend.close(linger=0)
            backend.close(linger=0)
            ctx.term()

    def _select_worker(
        self,
        method: str,
        worker_queue: list[bytes],
        worker_methods: dict[bytes, set[str]],
    ) -> bytes | None:
        """Pick the next available worker supporting `method`, or fall back to the head of `worker_queue`."""
        if not worker_queue:
            return None
        for i, worker in enumerate(worker_queue):
            w_methods = worker_methods.get(worker, set())
            if not w_methods or method in w_methods:
                return worker_queue.pop(i)
        logger.warning(f'No available workers support method `{method}`!')
        return worker_queue.pop(0)

    def _run_broker(self, strategy: Literal['round-robin', 'balancing']) -> None:
        """Unified broker loop supporting 'round-robin' and 'balancing' routing strategies."""
        transport = self.transport or ZmqBrokerTransport(self.frontend_addr, self.backend_addr)
        if self.transport is None:
            logger.info(f'Listening for tasks from: {self.frontend_addr}')
            logger.info(f'Listening for workers at: {self.backend_addr}')
        self.manager.start_workers()

        worker_times: dict[bytes, float] = {}
        worker_methods: dict[bytes, set[str]] = {}
        worker_queue: list[bytes] = []

        try:
            while not self._stop_event.is_set():
                backend_replies, frontend_requests = transport.poll(
                    10, accept_clients=bool(worker_queue)
                )

                for worker, response in backend_replies:
                    worker_is_new = worker not in worker_times
                    worker_times[worker] = time.time()
                    self._process_response(worker, response, transport)

                    if response.type == ResponseType.HEARTBEAT:
                        try:
                            w_methods = set(response.content)
                        except (ValueError, TypeError):
                            w_methods = set()
                        worker_methods[worker] = w_methods
                    elif (
                        strategy == 'balancing'
                        and response.type in (ResponseType.DONE, ResponseType.ERROR)
                        and worker not in worker_queue
                    ):
                        worker_queue.append(worker)

                    if worker_is_new:
                        worker_queue.append(worker)
                        suffix = ','.join(worker_methods.get(worker, set())) or 'All'
                        logger.info(f'Workers [{len(worker_times):4d}], + : {worker.decode("utf-8")} | {suffix}')

                expired = time.time() - MAX_HEARTBEAT_INTERVAL
                removed = [w for w, t in worker_times.items() if t <= expired]
                if removed:
                    removed_set = set(removed)
                    worker_times = {w: t for w, t in worker_times.items() if w not in removed_set}
                    worker_queue = [w for w in worker_queue if w not in removed_set]
                    worker_methods = {w: m for w, m in worker_methods.items() if w not in removed_set}
                    removed_workers = ', '.join(w.decode('utf-8') for w in removed)
                    logger.warning(f'Workers [{len(worker_times):4d}], - : {removed_workers}')
                    self._monitor_cleanup(removed)

                for request in frontend_requests:
                    if request.method in ('client_config', 'ping'):
                        content = '' if request.method == 'ping' else self.service_factory.get_methods()
                        transport.send_to_client(request.reply(content, ResponseType.DONE))
                        continue

                    worker = self._select_worker(request.method, worker_queue, worker_methods)
                    if worker is None:
                        transport.send_to_client(
                            request.reply('No workers available', ResponseType.ERROR)
                        )
                        continue

                    if strategy == 'round-robin':
                        worker_queue.append(worker)

                    transport.send_to_worker(worker, request)
                    self._monitor_request(worker, request)
        finally:
            transport.close()

    def round_robin_proxy(self):
        """
        A proxy which forwards all messages from the front-end to the back-end in round-robin fashion.
        This proxy supports heterogeneous workers.
        """
        self._run_broker('round-robin')

    def load_balancing_proxy(self):
        """
        A proxy which performs basic load balancing. That is a busy worker is removed from the worker pool until
        it's current task is completed. At that point, it's added back to the pool for the next task.
        """
        self._run_broker('balancing')


class WorkerManager(object):
    def __init__(self, factory: ServiceFactory, address: str, instances: int = 1, methods: Sequence[str] = ()):
        """
        :param factory:  A ServiceFactory instance
        :param address: Backend address to connect to
        :param instances: Number of worker instances to manage
        :param methods: List of allowed methods to run
        """
        self.factory = factory
        self.backend_addr = address.replace('*', 'localhost')
        self.instances = instances
        self.methods = methods
        self.processes = []

    def start_workers(self):
        """
        Start subprocesses for each worker
        :return:
        """

        self.processes = []
        for i in range(self.instances):
            p = Process(target=start_worker, args=(self.backend_addr, self.factory, self.methods))
            p.start()
            self.processes.append(p)
        if self.instances:
            logger.info(f'Connected {self.instances} worker(s) to {self.backend_addr}')
            _method_txt = ','.join(self.methods) if self.methods else 'All'
            logger.info(f'Supported methods: {_method_txt}')

    def stop_workers(self):
        """Terminate any running worker subprocesses."""
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=1.0)
        self.processes = []

    def wait_for_workers(self):
        """
        Wait for all worker processes to terminate
        """
        for proc in self.processes:
            proc.join()

    def run(self):
        self.start_workers()
        self.wait_for_workers()
