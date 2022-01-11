import hashlib
import time
from queue import Queue
from collections import defaultdict


class SignalObject(object):
    __slots__ = ('signals', 'slots')

    def __init__(self):
        self.signals = Queue()
        self.slots = defaultdict(list)

    def process(self):
        """
        Run all handlers for one signal from the queue
        :return:
        """

        if not self.signals.empty():
            signal, args, kwargs = self.signals.get()
            for slot, xargs, xkwargs in self.slots.get(signal, []):
                slot(self, *args, *xargs, **kwargs, **xkwargs)

    def emit(self, signal: str, *args, **kwargs):
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
        when the hander was connected.
        """

        if isinstance(slot, int) and 0 < slot < len(self.slots[signal]):
            self.slots[signal].pop(slot)
        else:
            self.slots[signal].remove((slot, args, kwargs))


class ResultMixin(object):
    def __init__(self, result_id: str):
        self.result_id = result_id
        self.parts = []
        self.results = None
        self.errors = None
        self.ready = False
        self.failed = False
        super().__init__()

    def update(self, info):
        """
        Update the results and notify that partial results are available.

        :param info: partial results
        """
        self.parts.append(info)
        self.emit('update', info)

    def failure(self, error: str):
        """
        Update the results and notify that partial results are available.

        :param error: error message
        """
        self.errors = error
        self.failed = True
        self.ready = True
        self.emit('failed', error)

    def done(self, info=None):
        """
        Emits the done signal

        :param info: results or None
        """
        self.results = info if info is not None else self.parts
        self.ready = True
        self.emit('done', info)

    def is_ready(self) -> bool:
        """
        Check if the result is ready
        """
        return self.ready or self.failed

    def wait(self, timeout: int = 0):
        """
        Wait for result to be ready

        returns
        :param timeout: int, maximum time to wait, 0 means wait forever.
        :return: True if result is ready or False if it timed-out.
        """

        start_time = time.time()
        while not self.is_ready() and (not timeout or time.time() - start_time < timeout):
            time.sleep(0.1)

        return self.is_ready()

    def __str__(self):
        h = hashlib.blake2b(digest_size=10)
        h.update(f'{self.result_id}'.encode('utf-8'))
        ready_text = {True: 'Ready', False: 'Not Ready'}[self.ready]
        error_text = {True: 'Failed', False: ''}[self.ready]
        return f'REP[{h.hexdigest()}] - {ready_text} {error_text}'


class Result(SignalObject, ResultMixin):
    """
    Result object oviding methods for managing results
    """
    __slots__ = ('result_id', 'parts', 'results', 'ready', 'failed', 'errors')

    def __init__(self, result_id: str):
        ResultMixin.__init__(self, result_id)
        super().__init__()







