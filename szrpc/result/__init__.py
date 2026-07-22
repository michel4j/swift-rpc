import time
from queue import Queue
from collections import defaultdict
from typing import Any, Protocol

from szrpc.server import ResponseType


class ResultProtocol(Protocol):
    identity: str
    result_id: bytes
    parts: list
    results: Any
    ready_state: bool
    failed_state: bool
    errors: str

    def trigger(self, signal: str, *args, **kwargs):
        ...

    def post_process(self):
        ...


class SignalObject(object):
    __slots__ = ('signals', 'slots')

    def __init__(self):
        self.signals = Queue()
        self.slots = defaultdict(list)

    def post_process(self):
        """
        Run all handlers for pending signals from the queue
        :return:
        """

        while not self.signals.empty():
            signal, args, kwargs = self.signals.get()
            for slot, xargs, xkwargs in self.slots.get(signal, []):
                slot(self, *args, *xargs, **kwargs, **xkwargs)

    def trigger(self, signal: str, *args, **kwargs):
        """
        Emit a signal

        :param signal: signal name
        :param args: arguments
        :param kwargs: keyword arguments
        """
        self.signals.put((signal, args, kwargs))

    def connect(self, signal, slot, *args, **kwargs):
        """
        Connect a handler to a given signal

        :param signal: signal name
        :param slot: signal handler method, the first argument of the method is the object which emitted the signal
        :param args: extra arguments to pass to the handler
        :param kwargs: extra kwargs to pass to the handler
        :return: a connection id (int) which can be used to disconnect the signal
        """
        self.slots[signal].append((slot, args, kwargs))
        return len(self.slots[signal])

    def disconnect(self, signal, slot, *args, **kwargs):
        """
        Disconnect signal

        :param signal: signal name
        :param slot:  handler. It can be the same signal handler used to connect, or it could be  the integer returned
        when the handler was connected.
        """

        if isinstance(slot, int) and 0 < slot < len(self.slots[signal]):
            self.slots[signal].pop(slot)
        else:
            self.slots[signal].remove((slot, args, kwargs))


class ResultMixin:

    def setup(self: ResultProtocol, result_id: bytes):
        self.identity = result_id.decode('utf-8')
        self.result_id = result_id
        self.parts = []
        self.results = None
        self.errors = ''
        self.ready_state = False
        self.failed_state = False

    def process(self: ResultProtocol, response):
        if response.type == ResponseType.UPDATE:
            info = response.content
            self.parts.append(info)
            self.trigger('update', info)

        elif response.type == ResponseType.DONE:
            info = response.content
            self.results = info if info is not None else self.parts
            self.ready_state = True
            self.trigger('done', info)
        elif response.type == ResponseType.ERROR:
            self.errors = response.content
            self.failed_state = True
            self.ready_state = True
            self.trigger('failed', self.errors)

        self.post_process()

    def is_ready(self) -> bool:
        """
        Check if the result is ready
        """
        return self.ready_state or self.failed_state

    def wait(self, timeout: int = 0):
        """
        Wait for result to be ready

        :param timeout: int, maximum time to wait, 0 means wait forever.
        :return: True if result is ready or False if it timed-out.
        """

        start_time = time.time()
        while not self.is_ready() and (not timeout or time.time() - start_time < timeout):
            time.sleep(0.1)

        return self.is_ready()

    def __str__(self):
        token = self.identity[:4]
        ready_text = {
            (True, False): 'DONE',
            (False, False): 'UPDATE',
            (False, True): 'FAILED',
            (True, True): 'FAILED',
        }[(self.ready_state, self.failed_state)]
        return f'rep[{token}..] - {ready_text}'


class Result(ResultMixin, SignalObject):
    """
    Result object providing methods for managing results
    """
    __slots__ = ('identity', 'parts', 'results', 'ready_state', 'failed_state', 'errors')

    def __init__(self, result_id: bytes):
        self.setup(result_id)
        super().__init__()







