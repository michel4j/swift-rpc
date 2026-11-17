from __future__ import annotations

from collections import defaultdict
from threading import Event, Lock
from typing import Any, Callable, Protocol

from szrpc.protocol import ResponseType


class Dispatcher(Protocol):
    """Port for marshaling Result signal callbacks onto a target thread or main loop."""

    def dispatch(self, fn: Callable[[], None]) -> None: ...


class DirectDispatcher:
    """Default in-process dispatcher that invokes callbacks immediately."""

    def dispatch(self, fn: Callable[[], None]) -> None:
        fn()


class Result:
    """Deep result state machine with thread-safe wait() and dispatcher-backed signals."""

    __slots__ = (
        'identity',
        'result_id',
        'parts',
        'results',
        'ready_state',
        'failed_state',
        'errors',
        'error_info',
        '_event',
        '_slots',
        '_next_token',
        '_lock',
        '_dispatcher',
        '_on_cancel',
    )

    def __init__(
        self,
        result_id: bytes,
        dispatcher: Dispatcher | None = None,
        on_cancel: Callable[[], None] | None = None,
    ):
        self.identity = result_id.decode('utf-8')
        self.result_id = result_id
        self.parts: list[Any] = []
        self.results: Any = None
        self.errors: str = ''
        self.error_info: dict[str, Any] | None = None
        self.ready_state = False
        self.failed_state = False
        self._event = Event()
        self._slots: dict[str, dict[int, tuple[Callable, tuple, dict]]] = defaultdict(dict)
        self._next_token = 1
        self._lock = Lock()
        self._dispatcher: Dispatcher = dispatcher if dispatcher is not None else DirectDispatcher()
        self._on_cancel = on_cancel

    def cancel(self) -> None:
        """Request cancellation of the remote call if not already finished."""
        if not self.is_ready() and self._on_cancel is not None:
            self._on_cancel()

    def connect(self, signal: str, slot: Callable, *args, **kwargs) -> int:
        """Connect a handler to `signal`; returns a stable integer token for disconnect()."""
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._slots[signal][token] = (slot, args, kwargs)
            return token

    def disconnect(self, signal: str, slot: int | Callable, *args, **kwargs) -> None:
        """Disconnect a handler by connection token or callable."""
        with self._lock:
            handlers = self._slots.get(signal)
            if not handlers:
                return
            if isinstance(slot, int):
                handlers.pop(slot, None)
                return
            for token, entry in list(handlers.items()):
                if entry == (slot, args, kwargs) or (not args and not kwargs and entry[0] == slot):
                    handlers.pop(token)
                    return

    def trigger(self, signal: str, *args, **kwargs) -> None:
        """Dispatch handlers for `signal` through the configured Dispatcher."""
        with self._lock:
            handlers = list(self._slots.get(signal, {}).values())
        if not handlers:
            return

        def _invoke() -> None:
            for slot, xargs, xkwargs in handlers:
                slot(self, *args, *xargs, **kwargs, **xkwargs)

        self._dispatcher.dispatch(_invoke)

    def process(self, response) -> None:
        """Apply an incoming Response to state, wake waiters if terminal, and trigger signals."""
        if response.type == ResponseType.UPDATE:
            info = response.content
            self.parts.append(info)
            self.trigger('update', info)
        elif response.type == ResponseType.DONE:
            info = response.content
            self.results = info if info is not None else self.parts
            self.ready_state = True
            self._event.set()
            self.trigger('done', info)
        elif response.type == ResponseType.ERROR:
            if isinstance(response.content, dict):
                self.error_info = response.content
                err_type = response.content.get('type')
                msg = response.content.get('message', '')
                self.errors = f'{err_type}: {msg}' if err_type else str(msg)
            else:
                self.error_info = {'type': None, 'message': str(response.content), 'code': None}
                self.errors = str(response.content)
            self.failed_state = True
            self.ready_state = True
            self._event.set()
            self.trigger('failed', self.errors)

    def is_ready(self) -> bool:
        """Check if the result has completed or failed."""
        return self.ready_state or self.failed_state

    def wait(self, timeout: float = 0) -> bool:
        """Wait for result completion; timeout <= 0 waits indefinitely."""
        return self._event.wait(timeout=None if not timeout else timeout)

    def __str__(self) -> str:
        token = self.identity[:4]
        ready_text = {
            (True, False): 'DONE',
            (False, False): 'UPDATE',
            (False, True): 'FAILED',
            (True, True): 'FAILED',
        }[(self.ready_state, self.failed_state)]
        return f'rep[{token}..] - {ready_text}'
