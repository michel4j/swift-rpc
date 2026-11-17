from __future__ import annotations

import queue
import sys
from enum import Enum
from threading import Event, Lock
from typing import Callable, Sequence, Any

import msgpack

from . import log


class ResponseType(Enum):
    DONE = 1
    UPDATE = 2
    ERROR = 3
    HEARTBEAT = 4
    READY = 5


class Request:
    """RPC request message with wire encoding and reply helper."""

    __slots__ = (
        'client_id',
        'request_id',
        'method',
        'kwargs',
        'reply_to',
        'identity',
        'cancelled',
        '_cancel_callbacks',
        '_cancel_lock',
    )

    def __init__(
        self,
        client_id: bytes,
        request_id: bytes,
        method: str,
        kwargs: dict,
        reply_to: queue.Queue | None = None,
    ):
        self.client_id = client_id
        self.request_id = request_id
        self.identity = request_id.decode('utf-8')
        self.method = method
        self.kwargs = kwargs
        self.reply_to = reply_to
        self.cancelled = Event()
        self._cancel_callbacks: list[Callable[[], None]] = []
        self._cancel_lock = Lock()

    def is_cancelled(self) -> bool:
        """Return True if this request has been cancelled."""
        return self.cancelled.is_set()

    def on_cancel(self, callback: Callable[[], None]) -> None:
        """Register a callback invoked when the request is cancelled (or immediately if already cancelled)."""
        run_now = False
        with self._cancel_lock:
            if self.cancelled.is_set():
                run_now = True
            else:
                self._cancel_callbacks.append(callback)
        if run_now:
            callback()

    def cancel(self) -> None:
        """Mark the request as cancelled and invoke any registered on_cancel callbacks."""
        with self._cancel_lock:
            if self.cancelled.is_set():
                return
            self.cancelled.set()
            callbacks = list(self._cancel_callbacks)
            self._cancel_callbacks.clear()
        for cb in callbacks:
            cb()

    def encode_payload(self) -> list[bytes]:
        """Return [request_id, method_bytes, msgpack_kwargs] without routing client_id."""
        return [
            self.request_id,
            self.method.encode('utf-8'),
            msgpack.dumps(self.kwargs),
        ]

    def parts(self) -> list[bytes]:
        """Backwards-compatible alias for encode_payload()."""
        return self.encode_payload()

    @staticmethod
    def create(
        client_id: bytes,
        request_id: bytes,
        method: bytes,
        arg_data: bytes,
        reply_to: queue.Queue | None = None,
    ) -> 'Request':
        """Decode a Request from wire frames."""
        args = msgpack.loads(arg_data)
        return Request(
            client_id,
            request_id,
            method.decode('utf-8'),
            args if isinstance(args, dict) else {},
            reply_to=reply_to,
        )

    def reply(self, content, response_type: ResponseType = ResponseType.UPDATE) -> 'Response':
        """Build a Response for this request and enqueue it on reply_to if set."""
        response = Response(self.client_id, self.request_id, response_type, content)
        if self.reply_to is not None:
            self.reply_to.put(response)
        return response

    def ready(self) -> 'Response':
        """Signal to the broker that the worker is ready for additional requests while this one streams."""
        return self.reply(None, ResponseType.READY)

    def __str__(self) -> str:
        req_id = '/'.join(self.request_id.decode('ascii').split('/')[-2:])
        call_signature = log.log_call(self.method, (), self.kwargs, simple=True)
        return f'req[{req_id}] - {call_signature}'


class Response:
    """RPC response message with wire encoding and heartbeat factory."""

    __slots__ = ('client_id', 'request_id', 'type', 'content', 'identity')

    def __init__(
        self,
        client_id: bytes,
        request_id: bytes,
        response_type: ResponseType,
        content,
    ):
        self.client_id = client_id
        self.request_id = request_id
        self.type = response_type
        self.content = content
        self.identity = request_id.decode('utf-8')

    def encode_payload(self) -> list[bytes]:
        """Return [request_id, msgpack_type, msgpack_content] without routing client_id."""
        return [
            self.request_id,
            msgpack.dumps(self.type.value),
            msgpack.dumps(self.content),
        ]

    def parts(self) -> list[bytes]:
        """Return [client_id, request_id, msgpack_type, msgpack_content]."""
        return [self.client_id, *self.encode_payload()]

    @staticmethod
    def decode(client_id: bytes, payload: Sequence[bytes]) -> 'Response':
        """Decode a Response from client_id and 3 payload frames."""
        request_id, response_type, content = payload
        return Response.create(client_id, request_id, response_type, content)

    @staticmethod
    def create(
        client_id: bytes,
        request_id: bytes,
        response_type: bytes,
        content: bytes,
    ) -> 'Response':
        """Decode a Response from wire frames."""
        return Response(
            client_id,
            request_id,
            ResponseType(msgpack.loads(response_type)),
            msgpack.loads(content),
        )

    @staticmethod
    def heartbeat_message(
        system_id: bytes = b'',
        methods: Sequence[str] = (),
        metrics: dict[str, Any] | None = None,
    ) -> 'Response':
        """Create a HEARTBEAT Response object."""
        content: Any = methods if metrics is None else {'methods': list(methods), 'metrics': metrics}
        return Response(system_id, b'heartbeat', ResponseType.HEARTBEAT, content)

    @staticmethod
    def heartbeat(
        system_id: bytes = b'',
        methods: Sequence[str] = (),
        metrics: dict[str, Any] | None = None,
    ) -> list[bytes]:
        """Backwards-compatible helper returning encoded heartbeat frames."""
        return Response.heartbeat_message(system_id=system_id, methods=methods, metrics=metrics).parts()

    def __str__(self) -> str:
        req_id = '-'.join(self.request_id.decode('ascii').split('-')[-2:])
        size = sys.getsizeof(self.content)
        return f'req[{req_id}] - {self.type.name} {size} Bytes'
