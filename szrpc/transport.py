from __future__ import annotations

import queue
import threading
import uuid
from typing import Any, Protocol

import msgpack
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
    """ZeroMQ DEALER transport for Client (with optional SUB socket for broadcast events)."""

    def __init__(
        self,
        address: str,
        client_id: bytes,
        linger: bool = True,
        sub_addr: str | None = None,
    ):
        self.address = address
        self.client_id = client_id
        self.linger = linger
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = client_id
        if not linger:
            self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(address)

        self.sub_socket = None
        if sub_addr:
            self.sub_socket = self.context.socket(zmq.SUB)
            self.sub_socket.setsockopt(zmq.LINGER, 0)
            self.sub_socket.connect(sub_addr)

    def subscribe_topic(self, topic: str) -> None:
        if self.sub_socket is not None:
            self.sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))

    def unsubscribe_topic(self, topic: str) -> None:
        if self.sub_socket is not None:
            self.sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode('utf-8'))

    def recv_event(self, timeout_ms: int = 0) -> tuple[str, Any] | None:
        if self.sub_socket is None:
            return None
        if not self.sub_socket.poll(timeout_ms, zmq.POLLIN):
            return None
        topic_bytes, payload_bytes = self.sub_socket.recv_multipart()
        return topic_bytes.decode('utf-8'), msgpack.loads(payload_bytes)

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
        if self.sub_socket is not None:
            self.sub_socket.close(linger=0)
        self.context.term()


class ZmqWorkerTransport:
    """ZeroMQ DEALER transport for Worker (with optional PUB socket for broadcast events)."""

    def __init__(self, backend: str, worker_id: bytes, pub_addr: str | None = None):
        self.backend = backend
        self.worker_id = worker_id
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = worker_id
        self.socket.connect(backend)

        self.pub_socket = None
        if pub_addr:
            self.pub_socket = self.context.socket(zmq.PUB)
            self.pub_socket.setsockopt(zmq.LINGER, 0)
            self.pub_socket.connect(pub_addr)

    def publish(self, topic: str, payload: Any) -> None:
        if self.pub_socket is None:
            raise RuntimeError('PUB transport not configured on this worker')
        self.pub_socket.send_multipart([topic.encode('utf-8'), msgpack.dumps(payload)])

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
        if self.pub_socket is not None:
            self.pub_socket.close(linger=0)
        self.context.term()


class ZmqBrokerTransport:
    """ZeroMQ ROUTER/ROUTER transport for Server broker (with optional XPUB/XSUB forwarder)."""

    def __init__(
        self,
        frontend_addr: str,
        backend_addr: str,
        pub_addr: str | None = None,
        xsub_addr: str | None = None,
    ):
        self.frontend_addr = frontend_addr
        self.backend_addr = backend_addr
        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self.frontend.bind(frontend_addr)
        self.backend.bind(backend_addr)
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self._accepting_clients = False

        self._pub_stop = threading.Event()
        self._pub_thread: threading.Thread | None = None
        self.xpub = None
        self.xsub = None
        if pub_addr and xsub_addr:
            self.xpub = self.context.socket(zmq.XPUB)
            self.xsub = self.context.socket(zmq.XSUB)
            self.xpub.setsockopt(zmq.LINGER, 0)
            self.xsub.setsockopt(zmq.LINGER, 0)
            self.xpub.bind(pub_addr)
            self.xsub.bind(xsub_addr)
            self._pub_thread = threading.Thread(target=self._forward_pubsub, daemon=True)
            self._pub_thread.start()

    def _forward_pubsub(self) -> None:
        poller = zmq.Poller()
        poller.register(self.xsub, zmq.POLLIN)
        poller.register(self.xpub, zmq.POLLIN)
        while not self._pub_stop.is_set():
            socks = dict(poller.poll(10))
            if self.xsub in socks:
                self.xpub.send_multipart(self.xsub.recv_multipart())
            if self.xpub in socks:
                self.xsub.send_multipart(self.xpub.recv_multipart())

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
        self._pub_stop.set()
        if self._pub_thread is not None and self._pub_thread.is_alive():
            self._pub_thread.join(timeout=1.0)
        self.frontend.close(linger=0)
        self.backend.close(linger=0)
        if self.xpub is not None:
            self.xpub.close(linger=0)
        if self.xsub is not None:
            self.xsub.close(linger=0)
        self.context.term()


class InMemoryNetwork:
    """In-process message hub creating paired Broker, Client, and Worker transports."""

    def __init__(self):
        self._cond = threading.Condition()
        self._to_broker_backend: list[tuple[bytes, Response]] = []
        self._to_broker_frontend: list[Request] = []
        self._client_queues: dict[bytes, queue.Queue[Response]] = {}
        self._client_event_queues: dict[bytes, tuple[set[str], queue.Queue[tuple[str, Any]]]] = {}
        self._worker_queues: dict[bytes, queue.Queue[Request]] = {}

    def broker(self) -> 'InMemoryBrokerTransport':
        return InMemoryBrokerTransport(self)

    def client(self, client_id: bytes | None = None) -> 'InMemoryClientTransport':
        cid = client_id or f'mem-client-{uuid.uuid4().hex[:8]}'.encode('ascii')
        self._client_queues[cid] = queue.Queue()
        self._client_event_queues[cid] = (set(), queue.Queue())
        return InMemoryClientTransport(self, cid)

    def worker(self, worker_id: bytes | None = None) -> 'InMemoryWorkerTransport':
        wid = worker_id or f'mem-worker-{uuid.uuid4().hex[:8]}'.encode('ascii')
        self._worker_queues[wid] = queue.Queue()
        return InMemoryWorkerTransport(self, wid)

    def publish(self, topic: str, payload: Any) -> None:
        for prefixes, eq in list(self._client_event_queues.values()):
            if any(topic.startswith(p) for p in prefixes):
                eq.put((topic, payload))


class InMemoryClientTransport:
    """In-process ClientTransport adapter."""

    def __init__(self, network: InMemoryNetwork, client_id: bytes):
        self._network = network
        self.client_id = client_id
        self._inbox = network._client_queues[client_id]
        self._prefixes, self._events = network._client_event_queues[client_id]

    def subscribe_topic(self, topic: str) -> None:
        self._prefixes.add(topic)

    def unsubscribe_topic(self, topic: str) -> None:
        self._prefixes.discard(topic)

    def recv_event(self, timeout_ms: int = 0) -> tuple[str, Any] | None:
        try:
            return self._events.get(timeout=timeout_ms / 1000.0 if timeout_ms else 0)
        except queue.Empty:
            return None

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

    def publish(self, topic: str, payload: Any) -> None:
        self._network.publish(topic, payload)

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
