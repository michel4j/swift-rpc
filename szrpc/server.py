from __future__ import annotations

import functools
import re
import os
import sys
import time
import queue
import socket
from collections import defaultdict, deque
from pathlib import Path

from typing import Any, Callable, Protocol, Type, Literal, Sequence
from threading import Event, Lock, Thread, current_thread
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

try:
    import psutil
except ImportError:
    psutil = None


logger = log.get_module_logger(__name__)

SERVER_TIMEOUT = 4
MIN_HEARTBEAT_INTERVAL = 1
MAX_HEARTBEAT_INTERVAL = 2


def _parse_heartbeat_content(content: Any) -> tuple[set[str], dict[str, Any] | None]:
    """Extract advertised methods and optional telemetry metrics from HEARTBEAT content."""
    if isinstance(content, dict):
        methods = content.get('methods', ())
        metrics = content.get('metrics')
        return set(methods), metrics if isinstance(metrics, dict) else None
    try:
        return set(content), None
    except (ValueError, TypeError):
        return set(), None


class _WorkerMetricsSampler:
    """Collects per-worker CPU, memory, and network I/O rates via psutil when available."""

    def __init__(self):
        self._proc = psutil.Process() if psutil is not None else None
        self._last_ts = time.time()
        self._last_net = psutil.net_io_counters() if psutil is not None else None
        if self._proc is not None:
            try:
                self._proc.cpu_percent(interval=None)
            except Exception:
                self._proc = None

    def sample(self, active_tasks: int) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            'cpu_pct': 0.0,
            'mem_rss': 0,
            'net_rx_bps': 0.0,
            'net_tx_bps': 0.0,
            'active_tasks': active_tasks,
        }
        if self._proc is None:
            return metrics
        now = time.time()
        dt = max(now - self._last_ts, 1e-3)
        try:
            metrics['cpu_pct'] = round(self._proc.cpu_percent(interval=None), 2)
            metrics['mem_rss'] = int(self._proc.memory_info().rss)
            net_io = psutil.net_io_counters()
            if net_io is not None and self._last_net is not None:
                metrics['net_rx_bps'] = round(max(0, net_io.bytes_recv - self._last_net.bytes_recv) / dt, 1)
                metrics['net_tx_bps'] = round(max(0, net_io.bytes_sent - self._last_net.bytes_sent) / dt, 1)
            self._last_net = net_io
            self._last_ts = now
        except Exception:
            pass
        return metrics


def control(fn: Callable) -> Callable:
    """Mark a Service remote__* method as a control RPC that bypasses load-balancing busy gates."""
    fn._szrpc_control = True
    return fn


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
        self.allowed_methods = self.get_methods()
        self.name_generator = namer.RandomGenerator()
        self._publisher: Callable[[str, Any], None] | None = None

    def publish(self, topic: str, payload: Any = None) -> None:
        """Broadcast an unsolicited event to subscribed clients."""
        if self._publisher is None:
            raise RuntimeError('Service is not bound to a PUB-capable Worker transport')
        self._publisher(topic, payload)

    @classmethod
    def get_description(cls) -> str:
        """Get a description of the service type from the class name and docstring."""
        return cls.__str__()

    @classmethod
    def get_methods(cls) -> tuple[str, ...]:
        """Single source of truth for allowed remote__* method names on this Service."""
        return tuple(
            attr[len('remote__'):]
            for attr in dir(cls) if attr.startswith('remote__')
        )

    @classmethod
    def get_control_methods(cls) -> set[str]:
        """Return remote method names decorated with @control."""
        return {
            attr[len('remote__'):]
            for attr in dir(cls)
            if attr.startswith('remote__') and getattr(getattr(cls, attr, None), '_szrpc_control', False)
        }

    @classmethod
    def factory(cls, *args, **kwargs) -> functools.partial:
        """Bind constructor arguments for deferred worker instantiation."""
        return functools.partial(cls, *args, **kwargs)

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
            msg = f'Service does not support remote method "{request.method}"'
            logger.error(msg)
            request.reply(
                content={'type': 'AttributeError', 'message': msg, 'code': 'METHOD_NOT_FOUND'},
                response_type=ResponseType.ERROR,
            )
        else:
            try:
                reply = method(request, **request.kwargs)
                response_type = ResponseType.DONE
            except Exception as e:
                reply = {
                    'type': type(e).__name__,
                    'message': str(e),
                    'code': getattr(e, 'code', None),
                }
                logger.exception(e)
                response_type = ResponseType.ERROR
            request.reply(content=reply, response_type=response_type)

    def on_start(self) -> None:
        """Lifecycle hook called when the Worker starts."""
        pass

    def on_shutdown(self) -> None:
        """Lifecycle hook called when the Worker exits."""
        pass

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
    """Deprecated compatibility wrapper; prefer passing a Service class or Service.factory(...) directly."""

    def __init__(self, service_type: Type[Service], *args, **kwargs):
        self.service_type = service_type
        self.args = args
        self.kwargs = kwargs

    def get_description(self) -> str:
        return self.service_type.get_description()

    def get_methods(self) -> tuple:
        return self.service_type.get_methods()

    def new(self) -> Service:
        return self.service_type(*self.args, **self.kwargs)


def _resolve_service(target) -> tuple[Type[Service], Callable[[], Service]]:
    """Normalize a Service class, Service.factory() partial, or legacy ServiceFactory."""
    if isinstance(target, functools.partial):
        return target.func, target
    if isinstance(target, type) and issubclass(target, Service):
        return target, target
    return target.service_type, target.new


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
        pub_addr: str | None = None,
        **kwargs,
    ):
        """
        :param backend: Backend address to connect to
        :param service: A Service instance which provides the API for the server
        :param methods: List of remote methods supported by this worker instance
        :param transport: Optional WorkerTransport adapter; defaults to ZmqWorkerTransport
        :param pub_addr: Optional ZMQ PUB address (broker XSUB) for broadcast events
        """

        self.service = service
        self.backend = backend
        self.pub_addr = pub_addr
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
        transport = self.transport or ZmqWorkerTransport(
            self.backend,
            self.service.create_worker_id(),
            pub_addr=self.pub_addr,
        )
        if hasattr(transport, 'publish'):
            self.service._publisher = transport.publish
        active_requests: dict[str, Request] = {}
        active_lock = Lock()

        def _dispatch(req: Request) -> None:
            try:
                self.service.call_remote(req)
            finally:
                with active_lock:
                    active_requests.pop(req.identity, None)

        try:
            self.service.on_start()
            sampler = _WorkerMetricsSampler()
            # initial heartbeat message
            transport.send(Response.heartbeat_message(methods=self.methods, metrics=sampler.sample(0)))
            last_heartbeat = time.time()

            while not self._stop_event.is_set():
                while not self.replies.empty():
                    response = self.replies.get()
                    logger.debug(f'-> {response}')
                    transport.send(response)

                request = transport.recv(10)
                if request is not None:
                    if request.method == '__cancel__':
                        target_id = request.kwargs.get('request_id', '')
                        with active_lock:
                            target = active_requests.get(target_id)
                        if target is not None:
                            target.cancel()
                        continue

                    request.reply_to = self.replies
                    logger.info(f'<- {request}')
                    if not self.is_supported(request.method):
                        logger.warning(f'Processing unsupported method `{request.method}`!')
                    with active_lock:
                        active_requests[request.identity] = request
                    task = Thread(target=_dispatch, args=(request,), daemon=True)
                    task.start()

                # Send periodic heartbeat + telemetry even when busy
                if time.time() - last_heartbeat >= MIN_HEARTBEAT_INTERVAL:
                    with active_lock:
                        n_active = len(active_requests)
                    transport.send(
                        Response.heartbeat_message(methods=self.methods, metrics=sampler.sample(n_active))
                    )
                    last_heartbeat = time.time()
        finally:
            try:
                self.service.on_shutdown()
            except Exception as exc:
                logger.exception(exc)
            transport.close()


def start_worker(address: str, factory, methods: Sequence[str] = (), pub_addr: str | None = None, *args, **kwargs):
    """
    Start a single worker in a subprocess
    :param address: backend address
    :param factory: Service class, Service.factory(), or ServiceFactory
    :param methods: List of remote methods supported by this worker instance
    :param pub_addr: Optional ZMQ PUB address (broker XSUB)
    """
    _, make_service = _resolve_service(factory)
    service = make_service()
    worker = Worker(address, service, methods=methods, pub_addr=pub_addr, *args, **kwargs)
    logger.debug(f'Starting new worker process: {os.getpid()}')
    return worker.run()


class MonitorPort(Protocol):
    """Port for recording broker RPC lifecycle events."""

    def record_request(self, worker_id: str, client_id: str, request_id: str, method: str, kwargs: dict[str, Any]) -> None: ...
    def record_response(self, worker_id: str, request_id: str, status: str, result: Any) -> None: ...
    def update_worker(self, worker_id: str, metrics: dict[str, Any] | None = None) -> None: ...
    def clear_workers(self, workers: Sequence[str]) -> None: ...


class NullMonitor:
    """No-op MonitorPort adapter used when monitoring is disabled."""

    def record_request(self, worker_id: str, client_id: str, request_id: str, method: str, kwargs: dict[str, Any]) -> None:
        pass

    def record_response(self, worker_id: str, request_id: str, status: str, result: Any) -> None:
        pass

    def update_worker(self, worker_id: str, metrics: dict[str, Any] | None = None) -> None:
        pass

    def clear_workers(self, workers: Sequence[str]) -> None:
        pass


class Server(object):
    def __init__(
        self,
        service_factory,
        ports: tuple = (9990, 9991),
        instances: int = 1,
        monitor_port: int = None,
        monitor_db: str | Path | None = None,
        transport: BrokerTransport | None = None,
        monitor: MonitorPort | None = None,
        threaded: bool = False,
        worker_transport_factory: Callable[[], WorkerTransport] | None = None,
        client_timeout: float | None = None,
        pub_port: int | None = None,
    ):
        """
        :param service_factory: A Service class, Service.factory(), or ServiceFactory
        :param ports: a tuple of ports for frontend, backend
        :param instances: Number of workers to start on server. Additional workers can be started on other hosts
        :param monitor_port: alternate way to specify a monitor port
        :param transport: Optional BrokerTransport adapter; defaults to ZmqBrokerTransport
        :param monitor: Optional MonitorPort adapter; defaults to NullMonitor unless monitor_port is set
        :param threaded: If True, run workers in-process in daemon threads instead of multiprocessing.Process
        :param worker_transport_factory: Optional factory for WorkerTransport when threaded=True
        :param client_timeout: Optional seconds of inactivity before cancelling a client's active requests
        :param pub_port: Optional port for XPUB broadcast events (binds XSUB on pub_port + 1)
        """
        self.service_type, _ = _resolve_service(service_factory)
        self.service_factory = service_factory
        self.frontend_addr = f'tcp://*:{ports[0]}'
        self.backend_addr = f'tcp://*:{ports[1]}'
        self.pub_addr = f'tcp://*:{pub_port}' if pub_port is not None else None
        self.xsub_addr = f'tcp://*:{pub_port + 1}' if pub_port is not None else None
        self.transport = transport
        self._stop_event = Event()
        self._broker_thread: Thread | None = None
        self.client_timeout = client_timeout
        self.manager = WorkerManager(
            self.service_factory,
            self.backend_addr,
            instances=instances,
            threaded=threaded,
            worker_transport_factory=worker_transport_factory,
            pub_addr=self.xsub_addr,
        )
        self.monitor: MonitorPort = NullMonitor()

        if monitor is not None:
            self.monitor = monitor
        elif DASH_ENABLED and monitor_port:
            info = {
                'host': socket.getfqdn().split('.')[0].upper(),
                'version': f"v{__version__}",
                'frontend': self.frontend_addr,
                'backend': self.backend_addr,
                'description': self.service_type.get_description(),
                'methods': self.service_type.get_methods()
            }
            self.monitor = mon.Monitor(service_info=info, db_path=monitor_db or mon.default_db_path())
            mon.start_monitor_thread(self.monitor, port=monitor_port)
        elif monitor_port:
            logger.warning('Dashboard not enabled. Please run "pip install szrpc[dash] to enable it"')

    def start(
        self,
        balancing: bool = False,
        proxy: Literal['simple', 'balancing', 'round-robin'] = 'round-robin',
    ) -> Thread:
        """Start the broker loop in a background daemon thread and return it."""
        self._broker_thread = Thread(
            target=self.run,
            kwargs={'balancing': balancing, 'proxy': proxy},
            daemon=True,
        )
        self._broker_thread.start()
        return self._broker_thread

    def stop(self) -> None:
        """Signal the broker loop to stop and terminate managed workers."""
        self._stop_event.set()
        self.manager.stop_workers()
        if (
            self._broker_thread is not None
            and self._broker_thread is not current_thread()
            and self._broker_thread.is_alive()
        ):
            self._broker_thread.join(timeout=1.0)
        if hasattr(self.monitor, 'close'):
            self.monitor.close()

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
        if response.type not in (ResponseType.HEARTBEAT, ResponseType.READY):
            transport.send_to_client(response)
            self.monitor.record_response(
                worker.decode('utf-8'),
                response.identity,
                response.type.name,
                response.content,
            )
        else:
            metrics = None
            if response.type == ResponseType.HEARTBEAT:
                _, metrics = _parse_heartbeat_content(response.content)
            self.monitor.update_worker(worker.decode('utf-8'), metrics=metrics)

    def _monitor_request(self, worker: bytes, request: Request):
        try:
            self.monitor.record_request(
                worker.decode('utf-8'),
                request.client_id.decode('utf-8'),
                request.identity,
                request.method,
                request.kwargs,
            )
        except Exception as e:
            logger.error(f"Failed to record request in monitor: {e}")

    def _monitor_cleanup(self, workers):
        """
        Remove active jobs if their workers disappear. This is called when a worker is removed from the pool due to
        inactivity or failure.
        :param workers: sequence of worker ID bytes to remove from the monitor
        """
        self.monitor.clear_workers([w.decode('utf-8') for w in workers])

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
        transport = self.transport or ZmqBrokerTransport(
            self.frontend_addr,
            self.backend_addr,
            pub_addr=self.pub_addr,
            xsub_addr=self.xsub_addr,
        )
        if self.transport is None:
            logger.info(f'Listening for tasks from: {self.frontend_addr}')
            logger.info(f'Listening for workers at: {self.backend_addr}')
        self.manager.start_workers()

        worker_times: dict[bytes, float] = {}
        worker_methods: dict[bytes, set[str]] = {}
        worker_queue: list[bytes] = []
        control_methods = self.service_type.get_control_methods()
        pending_requests: deque[Request] = deque()
        active_request_worker: dict[str, bytes] = {}
        active_client_requests: dict[bytes, set[str]] = defaultdict(set)
        client_last_seen: dict[bytes, float] = {}

        try:
            while not self._stop_event.is_set():
                accept_clients = bool(worker_queue) or (bool(control_methods) and bool(worker_times))
                backend_replies, frontend_requests = transport.poll(
                    10, accept_clients=accept_clients
                )

                for worker, response in backend_replies:
                    worker_is_new = worker not in worker_times
                    worker_times[worker] = time.time()
                    self._process_response(worker, response, transport)

                    if response.type in (ResponseType.DONE, ResponseType.ERROR):
                        active_request_worker.pop(response.identity, None)
                        active_client_requests.get(response.client_id, set()).discard(response.identity)

                    if response.type == ResponseType.HEARTBEAT:
                        w_methods, _ = _parse_heartbeat_content(response.content)
                        worker_methods[worker] = w_methods
                    elif (
                        strategy == 'balancing'
                        and response.type in (ResponseType.DONE, ResponseType.ERROR, ResponseType.READY)
                        and worker not in worker_queue
                    ):
                        worker_queue.append(worker)

                    if worker_is_new:
                        worker_queue.append(worker)
                        suffix = ','.join(worker_methods.get(worker, set())) or 'All'
                        logger.info(f'Workers [{len(worker_times):4d}], + : {worker.decode("utf-8")} | {suffix}')

                now = time.time()
                expired = now - MAX_HEARTBEAT_INTERVAL
                removed = [w for w, t in worker_times.items() if t <= expired]
                if removed:
                    removed_set = set(removed)
                    worker_times = {w: t for w, t in worker_times.items() if w not in removed_set}
                    worker_queue = [w for w in worker_queue if w not in removed_set]
                    worker_methods = {w: m for w, m in worker_methods.items() if w not in removed_set}
                    removed_workers = ', '.join(w.decode('utf-8') for w in removed)
                    logger.warning(f'Workers [{len(worker_times):4d}], - : {removed_workers}')
                    self._monitor_cleanup(removed)

                if self.client_timeout is not None:
                    stale_clients = [c for c, t in client_last_seen.items() if now - t > self.client_timeout]
                    for cid in stale_clients:
                        client_last_seen.pop(cid, None)
                        for req_id in active_client_requests.pop(cid, set()):
                            target_worker = active_request_worker.pop(req_id, None)
                            if target_worker in worker_times:
                                cancel_req = Request(cid, b'disconnect-cancel', '__cancel__', {'request_id': req_id})
                                transport.send_to_worker(target_worker, cancel_req)

                for req in frontend_requests:
                    client_last_seen[req.client_id] = now

                pending_requests.extend(frontend_requests)
                deferred: deque[Request] = deque()

                while pending_requests:
                    request = pending_requests.popleft()
                    if request.method == '__cancel__':
                        target_id = request.kwargs.get('request_id', '')
                        target_worker = active_request_worker.get(target_id)
                        if target_worker in worker_times:
                            transport.send_to_worker(target_worker, request)
                        continue

                    if request.method in ('client_config', 'ping'):
                        content = '' if request.method == 'ping' else self.service_type.get_methods()
                        transport.send_to_client(request.reply(content, ResponseType.DONE))
                        continue

                    if not worker_times:
                        transport.send_to_client(
                            request.reply(
                                {'type': 'RuntimeError', 'message': 'No workers available', 'code': 'NO_WORKERS'},
                                ResponseType.ERROR,
                            )
                        )
                        continue

                    if request.method in control_methods and not worker_queue:
                        live_pool = list(worker_times.keys())
                        worker = self._select_worker(request.method, live_pool, worker_methods)
                    else:
                        worker = self._select_worker(request.method, worker_queue, worker_methods)
                        if worker is None:
                            deferred.append(request)
                            continue
                        if strategy == 'round-robin':
                            worker_queue.append(worker)

                    active_request_worker[request.identity] = worker
                    active_client_requests[request.client_id].add(request.identity)
                    transport.send_to_worker(worker, request)
                    self._monitor_request(worker, request)

                pending_requests = deferred
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
    def __init__(
        self,
        factory,
        address: str,
        instances: int = 1,
        methods: Sequence[str] = (),
        threaded: bool = False,
        worker_transport_factory: Callable[[], WorkerTransport] | None = None,
        pub_addr: str | None = None,
    ):
        """
        :param factory: Service class, Service.factory(), or ServiceFactory
        :param address: Backend address to connect to
        :param instances: Number of worker instances to manage
        :param methods: List of allowed methods to run
        :param threaded: Run workers in-process in threads instead of multiprocessing.Process
        :param worker_transport_factory: Optional factory for WorkerTransport when threaded=True
        :param pub_addr: Optional ZMQ XSUB address for worker PUB sockets
        """
        self.factory = factory
        self.backend_addr = address.replace('*', 'localhost')
        self.pub_addr = pub_addr.replace('*', 'localhost') if pub_addr else None
        self.instances = instances
        self.methods = methods
        self.threaded = threaded
        self.worker_transport_factory = worker_transport_factory
        self.processes: list[Process] = []
        self._workers: list[Worker] = []
        self._threads: list[Thread] = []

    def start_workers(self):
        """Start subprocesses or in-process threads for each worker."""
        self.processes = []
        self._workers = []
        self._threads = []
        _, make_service = _resolve_service(self.factory)

        for _ in range(self.instances):
            if self.threaded:
                transport = self.worker_transport_factory() if self.worker_transport_factory else None
                worker = Worker(
                    self.backend_addr,
                    make_service(),
                    methods=self.methods,
                    transport=transport,
                    pub_addr=self.pub_addr,
                )
                t = Thread(target=worker.run, daemon=True)
                t.start()
                self._workers.append(worker)
                self._threads.append(t)
            else:
                p = Process(target=start_worker, args=(self.backend_addr, self.factory, self.methods, self.pub_addr))
                p.start()
                self.processes.append(p)
        if self.instances:
            mode = 'threaded' if self.threaded else 'process'
            logger.info(f'Connected {self.instances} {mode} worker(s) to {self.backend_addr}')
            _method_txt = ','.join(self.methods) if self.methods else 'All'
            logger.info(f'Supported methods: {_method_txt}')

    def stop_workers(self):
        """Stop any running worker threads and terminate worker subprocesses."""
        for worker in self._workers:
            worker.stop()
        caller = current_thread()
        for t in self._threads:
            if t is not caller and t.is_alive():
                t.join(timeout=1.0)
        self._workers = []
        self._threads = []

        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=1.0)
        self.processes = []

    def wait_for_workers(self):
        """Wait for all worker processes and threads to terminate."""
        for proc in self.processes:
            proc.join()
        for t in self._threads:
            t.join()

    def run(self):
        self.start_workers()
        self.wait_for_workers()
