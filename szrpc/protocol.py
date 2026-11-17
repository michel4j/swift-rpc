from __future__ import annotations

import queue
import sys
from enum import Enum
from typing import Sequence

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

    __slots__ = ('client_id', 'request_id', 'method', 'kwargs', 'reply_to', 'identity')

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
    def heartbeat_message(system_id: bytes = b'', methods: Sequence[str] = ()) -> 'Response':
        """Create a HEARTBEAT Response object."""
        return Response(system_id, b'heartbeat', ResponseType.HEARTBEAT, methods)

    @staticmethod
    def heartbeat(system_id: bytes = b'', methods: Sequence[str] = ()) -> list[bytes]:
        """Backwards-compatible helper returning encoded heartbeat frames."""
        return Response.heartbeat_message(system_id=system_id, methods=methods).parts()

    def __str__(self) -> str:
        req_id = '-'.join(self.request_id.decode('ascii').split('-')[-2:])
        size = sys.getsizeof(self.content)
        return f'req[{req_id}] - {self.type.name} {size} Bytes'
