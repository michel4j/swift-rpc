from __future__ import annotations

import functools
import socket
import time
import uuid
import importlib
from queue import Empty, Queue
from threading import Event, Lock, Thread, current_thread
from typing import Any, Callable

from . import log
from .protocol import Request
from .result import DirectDispatcher, Dispatcher, Result
from .transport import ClientTransport, ZmqClientTransport

logger = log.get_module_logger('szrpc')


_LEGACY_DISPATCHERS = {
    'szrpc.result.gresult.GResult': ('szrpc.result.gresult', 'GLibDispatcher'),
    'szrpc.result.qresult.QResult': ('szrpc.result.qresult', 'QtDispatcher'),
}


class Client(object):
    """
    Base class for all clients.
    """

    DEFAULT_DISPATCHER: Dispatcher | None = None

    def __init__(
        self,
        address: str = '',
        methods=(),
        heartbeat: int = 0,
        client_id: str | None = None,
        linger: bool = True,
        transport: ClientTransport | None = None,
        dispatcher: Dispatcher | None = None,
        sub_address: str | None = None,
    ):
        """
        :param address: Server address for the client, For example: tcp://localhost:9990
        :param methods: sequence of method names to allow for this client
        :param heartbeat: heartbeat interval in seconds, if 0, no heartbeat is used (default). Allows the client to
        detect server disconnections.
        :param client_id: client identifier slug. If not provided a new one will be generated. Must be unique between
        simultaneously connected clients. use the Client.create_id() class method to generate compatible unique ids.
        :param linger: if True, keep unsent messages in the queue on exit
        :param transport: Optional ClientTransport adapter; defaults to ZmqClientTransport
        :param dispatcher: Optional Dispatcher adapter for Result signal callbacks
        :param sub_address: Optional ZMQ PUB address to subscribe for broadcast events
        """
        if transport is not None:
            self.client_id = transport.client_id
            self.transport = transport
        else:
            self.client_id = client_id.encode('utf-8') if client_id else self.create_id()
            self.transport = ZmqClientTransport(
                address,
                self.client_id,
                linger=linger,
                sub_addr=sub_address,
            )

        self.url = address
        self.heartbeat = heartbeat
        self.requests = Queue()
        self.responses = Queue()
        self.events: Queue[tuple[str, Any]] = Queue()
        self._subscriptions: dict[int, tuple[str, Callable[[str, Any], None]]] = {}
        self._sub_lock = Lock()
        self._next_sub_token = 1
        self.remote_methods = set(methods)
        self.dispatcher: Dispatcher = dispatcher or self.DEFAULT_DISPATCHER or DirectDispatcher()
        self._results: dict[bytes, Result] = {}
        self._results_lock = Lock()
        self.ready = False
        self.linger = linger
        self.starting = True
        self.last_available = time.time()
        self.last_ping = time.time()
        self._stop_event = Event()
        self._threads: list[Thread] = []
        self.start(introspect=(not methods))

    @classmethod
    def create_id(cls) -> bytes:
        """
        Generate a unique client ID
        """
        host = socket.getfqdn().split('.')[0].lower()
        client = str(uuid.uuid1())[:8]
        return f'{host}-{client}'.encode('ascii')

    @classmethod
    def use(cls, target: Dispatcher | type | str):
        """
        Configure the default signal Dispatcher for Clients.

        Accepts a Dispatcher instance/class or legacy GResult/QResult dotted paths.
        """
        if isinstance(target, str):
            if target in _LEGACY_DISPATCHERS:
                mod_name, attr_name = _LEGACY_DISPATCHERS[target]
            else:
                mod_name, attr_name = target.rsplit('.', 1)
            mod = importlib.import_module(mod_name)
            obj = getattr(mod, attr_name)
            cls.DEFAULT_DISPATCHER = obj() if isinstance(obj, type) else obj
        elif isinstance(target, type):
            cls.DEFAULT_DISPATCHER = target()
        else:
            cls.DEFAULT_DISPATCHER = target

    def get_id(self) -> bytes:
        """
        Get the client ID
        """
        return self.client_id

    def create_request_id(self):
        """
        Generate a unique request ID
        """
        call_id = str(uuid.uuid1())[:8]
        return f'{self.client_id.decode("ascii")}-{call_id}'.encode('ascii')

    def start(self, introspect=True):
        """
        Start the client threads
        :param introspect: whether to introspect the server for available methods

        """
        self._threads = [
            Thread(target=self.send_requests, daemon=True),
            Thread(target=self.emit_results, daemon=True),
        ]
        for t in self._threads:
            t.start()
        if introspect:
            res = self.call_remote('client_config')
            res.connect('done', self.setup)
            res.wait()
        else:
            self.ready = True
            logger.debug(f'~> {self.url}... Ready!')

    def stop(self) -> None:
        """Signal client background loops to stop and wait for them to exit."""
        self._stop_event.set()
        caller = current_thread()
        for t in self._threads:
            if t is not caller and t.is_alive():
                t.join(timeout=1.0)

    def close(self) -> None:
        """Alias for stop()."""
        self.stop()

    def __enter__(self) -> 'Client':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def setup(self, result, methods):
        """
        Configure the client with the remote methods
        :param result: result object
        :param methods: sequence of method names returned from the server
        """
        self.remote_methods = methods
        self.ready = True
        logger.debug(f'~> {self.url}... Ready!')

    def is_ready(self) -> bool:
        """
        Check if the server is ready to receive commands
        """
        return self.ready and self.remote_methods

    def call_remote(self, method: str, **kwargs) -> Result:
        """
        Call the remote method on the server
        :param method: method name
        :param kwargs: parameters to pass to server
        :return: Returns a result object for deferred execution.
        """
        request_id = self.create_request_id()
        kwargs = {} if kwargs is None else kwargs
        request = Request(self.client_id, request_id, method, kwargs)
        logger.debug(f'~> {request}')
        result = Result(
            request_id,
            dispatcher=self.dispatcher,
            on_cancel=lambda: self.cancel_request(request_id),
        )
        with self._results_lock:
            self._results[request_id] = result
        self.requests.put(request)
        return result

    def cancel_request(self, request_id: bytes) -> None:
        """Send a cancellation control message for an in-flight request."""
        cancel_req = Request(
            self.client_id,
            self.create_request_id(),
            '__cancel__',
            {'request_id': request_id.decode('utf-8')},
        )
        self.requests.put(cancel_req)

    def subscribe(self, topic: str, callback: Callable[[str, Any], None]) -> int:
        """Subscribe to broadcast events matching `topic` prefix; returns a subscription token."""
        with self._sub_lock:
            token = self._next_sub_token
            self._next_sub_token += 1
            self._subscriptions[token] = (topic, callback)
        if hasattr(self.transport, 'subscribe_topic'):
            self.transport.subscribe_topic(topic)
        return token

    def unsubscribe(self, token_or_topic: int | str) -> None:
        """Unsubscribe by integer token or topic prefix."""
        removed_topics: list[str] = []
        with self._sub_lock:
            if isinstance(token_or_topic, int):
                entry = self._subscriptions.pop(token_or_topic, None)
                if entry is not None:
                    removed_topics.append(entry[0])
            else:
                for tok, (topic, _) in list(self._subscriptions.items()):
                    if topic == token_or_topic:
                        self._subscriptions.pop(tok)
                        removed_topics.append(topic)
            remaining = {t for t, _ in self._subscriptions.values()}

        if hasattr(self.transport, 'unsubscribe_topic'):
            for topic in removed_topics:
                if topic not in remaining:
                    self.transport.unsubscribe_topic(topic)

    def send_requests(self):
        """
        Monitors the request queue and sends pending requests to the server. Also
        receives responses from the server and adds them to the response queue
        """
        self.last_available = time.time()
        self.last_ping = time.time()

        try:
            while not self._stop_event.is_set():
                ping_pending = 0 < self.heartbeat < time.time() - self.last_ping

                response = self.transport.recv(10)
                if response is not None:
                    self.last_available = time.time()
                    self.last_ping = time.time()
                    self.responses.put(response)
                elif self.is_ready() and ping_pending:
                    try:
                        self.ping()
                    except AttributeError:
                        self.client_config()    # ping is not available, use client_config
                    self.last_ping = time.time()

                if (self.is_ready() or self.starting) and not self.requests.empty():
                    request = self.requests.get()
                    self.transport.send(request)
                    self.starting = False

                if hasattr(self.transport, 'recv_event'):
                    event = self.transport.recv_event(0)
                    if event is not None:
                        self.events.put(event)
        finally:
            self.transport.close()

    def emit_results(self):
        """
        Triggers pending result signals and cleans up the results dictionary. Also monitors for connection issues
        """
        while not self._stop_event.is_set():
            try:
                response = self.responses.get(timeout=0.01)
            except Empty:
                response = None

            if response is not None:
                logger.debug(f'<~ {response}')
                with self._results_lock:
                    result = self._results.get(response.request_id)
                if result is not None:
                    result.process(response)
                    if result.is_ready():
                        with self._results_lock:
                            self._results.pop(response.request_id, None)

            while not self.events.empty():
                topic, payload = self.events.get()
                with self._sub_lock:
                    callbacks = [
                        cb for prefix, cb in self._subscriptions.values() if topic.startswith(prefix)
                    ]
                if callbacks:
                    def _dispatch_event(cbs=callbacks, t=topic, p=payload) -> None:
                        for cb in cbs:
                            cb(t, p)

                    self.dispatcher.dispatch(_dispatch_event)

            # check connection
            if self.heartbeat > 0:
                has_heartbeat = self.last_available + 2 * self.heartbeat > time.time()
                if self.ready and not has_heartbeat:
                    self.ready = False
                    logger.error('Server connection lost!')
                elif not self.ready and has_heartbeat and self.remote_methods:
                    self.ready = True
                    logger.info('Server connection restored!')

    def __getattr__(self, name):
        if name in ['client_config', 'ping'] or name in self.remote_methods:
            return functools.partial(self.call_remote, name)
        else:
            raise AttributeError(f'{self.__class__.__name__!r} has no attribute {name!r}')


def use(result_class: type | str):
    """
    Swap out the Result Class

    :param result_class: Class object or dotted string
    """

    Client.use(result_class)
