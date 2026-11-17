from __future__ import annotations

import queue
import threading
import uuid
from typing import Protocol

import zmq

from . import log
from .protocol import Request, Response

logger = log.get_module_logger(__name__)


class ClientTransport(Protocol):
    """Port for client-side RPC message transport."""

    client_id: bytes

    def send(self, request: Request) -> None: ...
    def recv(self, timeout_ms: int = 10) -> Response | None: ...
    def close(self) -> None: ...


class WorkerTransport(Protocol):
    """Port for worker-side RPC message transport."""

    worker_id: bytes

    def send(self, response: Response) -> None: ...
    def recv(self, timeout_ms: int = 10) -> Request | None: ...
    def close(self) -> None: ...


class BrokerTransport(Protocol):
    """Port for broker-side routing between frontend clients and backend workers."""

    def poll(
        self, timeout_ms: int = 10, *, accept_clients: bool = True
    ) -> tuple[list[tuple[bytes, Response]], list[Request]]: ...
    def send_to_client(self, response: Response) -> None: ...
    def send_to_worker(self, worker_id: bytes, request: Request) -> None: ...
    def close(self) -> None: ...


class ZmqClientTransport:
    """ZeroMQ DEALER transport for Client."""

    def __init__(self, address: str, client_id: bytes, linger: bool = True):
        self.address = address
        self.client_id = client_id
        self.linger = linger
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = client_id
        if not linger:
            self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(address)

    def send(self, request: Request) -> None:
        self.socket.send_multipart(request.encode_payload())

    def recv(self, timeout_ms: int = 10) -> Response | None:
        if not self.socket.poll(timeout_ms, zmq.POLLIN):
            return None
        frames = self.socket.recv_multipart()
        try:
            return Response.decode(self.client_id, frames)
        except Exception as exc:
            logger.error('Invalid response!')
            logger.exception(exc)
            return None

    def close(self) -> None:
        linger = None if self.linger else 0
        self.socket.close(linger=linger)
        self.context.term()


class ZmqWorkerTransport:
    """ZeroMQ DEALER transport for Worker."""

    def __init__(self, backend: str, worker_id: bytes):
        self.backend = backend
        self.worker_id = worker_id
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = worker_id
        self.socket.connect(backend)

    def send(self, response: Response) -> None:
        self.socket.send_multipart(response.parts())

    def recv(self, timeout_ms: int = 10) -> Request | None:
        if not self.socket.poll(timeout_ms, zmq.POLLIN):
            return None
        frames = self.socket.recv_multipart()
        try:
            return Request.create(*frames)
        except Exception as exc:
            logger.error(f'Invalid request: {exc}')
            logger.exception(exc)
            return None

    def close(self) -> None:
        self.socket.close(linger=0)
        self.context.term()


class ZmqBrokerTransport:
    """ZeroMQ ROUTER/ROUTER transport for Server broker."""

    def __init__(self, frontend_addr: str, backend_addr: str):
        self.frontend_addr = frontend_addr
        self.backend_addr = backend_addr
        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.frontend.bind(frontend_addr)
        self.backend.bind(backend_addr)
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self._accepting_clients = False

    def _set_accept_clients(self, accept: bool) -> None:
        if accept and not self._accepting_clients:
            self.poller.register(self.frontend, zmq.POLLIN)
            self._accepting_clients = True
        elif not accept and self._accepting_clients:
            self.poller.unregister(self.frontend)
            self._accepting_clients = False

    def poll(
        self, timeout_ms: int = 10, *, accept_clients: bool = True
    ) -> tuple[list[tuple[bytes, Response]], list[Request]]:
        self._set_accept_clients(accept_clients)
        sockets = dict(self.poller.poll(timeout_ms))
        backend_replies: list[tuple[bytes, Response]] = []
        frontend_requests: list[Request] = []

        if self.backend in sockets:
            frames = self.backend.recv_multipart()
            worker_id = frames[0]
            try:
                response = Response.create(*frames[1:])
            except Exception as exc:
                logger.error(f'Invalid response from worker {worker_id}: {exc}')
            else:
                backend_replies.append((worker_id, response))

        if self.frontend in sockets:
            frames = self.frontend.recv_multipart()
            try:
                request = Request.create(*frames)
            except Exception as exc:
                logger.error(f'Invalid request from client: {exc}')
            else:
                frontend_requests.append(request)

        return backend_replies, frontend_requests

    def send_to_client(self, response: Response) -> None:
        self.frontend.send_multipart(response.parts())

    def send_to_worker(self, worker_id: bytes, request: Request) -> None:
        self.backend.send_multipart([worker_id, request.client_id, *request.encode_payload()])

    def close(self) -> None:
        self.frontend.close(linger=0)
        self.backend.close(linger=0)
        self.context.term()


class InMemoryNetwork:
    """In-process message hub creating paired Broker, Client, and Worker transports."""

    def __init__(self):
        self._cond = threading.Condition()
        self._to_broker_backend: list[tuple[bytes, Response]] = []
        self._to_broker_frontend: list[Request] = []
        self._client_queues: dict[bytes, queue.Queue[Response]] = {}
        self._worker_queues: dict[bytes, queue.Queue[Request]] = {}

    def broker(self) -> 'InMemoryBrokerTransport':
        return InMemoryBrokerTransport(self)

    def client(self, client_id: bytes | None = None) -> 'InMemoryClientTransport':
        cid = client_id or f'mem-client-{uuid.uuid4().hex[:8]}'.encode('ascii')
        self._client_queues[cid] = queue.Queue()
        return InMemoryClientTransport(self, cid)

    def worker(self, worker_id: bytes | None = None) -> 'InMemoryWorkerTransport':
        wid = worker_id or f'mem-worker-{uuid.uuid4().hex[:8]}'.encode('ascii')
        self._worker_queues[wid] = queue.Queue()
        return InMemoryWorkerTransport(self, wid)


class InMemoryClientTransport:
    """In-process ClientTransport adapter."""

    def __init__(self, network: InMemoryNetwork, client_id: bytes):
        self._network = network
        self.client_id = client_id
        self._inbox = network._client_queues[client_id]

    def send(self, request: Request) -> None:
        with self._network._cond:
            self._network._to_broker_frontend.append(request)
            self._network._cond.notify_all()

    def recv(self, timeout_ms: int = 10) -> Response | None:
        try:
            return self._inbox.get(timeout=timeout_ms / 1000.0)
        except queue.Empty:
            return None

    def close(self) -> None:
        pass


class InMemoryWorkerTransport:
    """In-process WorkerTransport adapter."""

    def __init__(self, network: InMemoryNetwork, worker_id: bytes):
        self._network = network
        self.worker_id = worker_id
        self._inbox = network._worker_queues[worker_id]

    def send(self, response: Response) -> None:
        with self._network._cond:
            self._network._to_broker_backend.append((self.worker_id, response))
            self._network._cond.notify_all()

    def recv(self, timeout_ms: int = 10) -> Request | None:
        try:
            return self._inbox.get(timeout=timeout_ms / 1000.0)
        except queue.Empty:
            return None

    def close(self) -> None:
        pass


class InMemoryBrokerTransport:
    """In-process BrokerTransport adapter."""

    def __init__(self, network: InMemoryNetwork):
        self._network = network

    def poll(
        self, timeout_ms: int = 10, *, accept_clients: bool = True
    ) -> tuple[list[tuple[bytes, Response]], list[Request]]:
        timeout_s = timeout_ms / 1000.0
        with self._network._cond:
            def has_work() -> bool:
                return bool(self._network._to_broker_backend) or (
                    accept_clients and bool(self._network._to_broker_frontend)
                )

            if not has_work():
                self._network._cond.wait(timeout=timeout_s)

            backend_replies = list(self._network._to_broker_backend)
            self._network._to_broker_backend.clear()

            frontend_requests: list[Request] = []
            if accept_clients:
                frontend_requests = list(self._network._to_broker_frontend)
                self._network._to_broker_frontend.clear()

            return backend_replies, frontend_requests

    def send_to_client(self, response: Response) -> None:
        q = self._network._client_queues.get(response.client_id)
        if q is not None:
            q.put(response)

    def send_to_worker(self, worker_id: bytes, request: Request) -> None:
        q = self._network._worker_queues.get(worker_id)
        if q is not None:
            q.put(request)

    def close(self) -> None:
        pass
