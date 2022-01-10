from threading import Thread
from queue import Queue
from collections import defaultdict


class SignalObject(object):
    __slots__ = ('signals', 'slots')
    SIGNALS = ()

    def __init__(self):
        self.signals = Queue()
        self.slots = defaultdict(list)
        print('SignalObject')

    def emission(self):
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
        if signal in self.SIGNALS:
            self.signals.put((signal, args, kwargs))
        else:
            raise ValueError(f'Invalid signal: "{signal}"')

    def connect(self, signal, slot, *args, **kwargs):
        """
        Connect a handler to a given signal

        :param signal: signal name
        :param slot: signal handler method, the first argument of the method is the object which emitted the signal
        :param args: extra arguments to pass to the handler
        :param kwargs: extra kwargs to pass to the handler
        :return: a connection id (int) which can be used to disconnect the signal
        """
        if signal in self.SIGNALS:
            self.slots[signal].append((slot, args, kwargs))
            return len(self.slots[signal])
        else:
            raise ValueError(f'Invalid signal: "{signal}"')

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
    """
    Mixin providing methods for managing results
    """
    def __init__(self, result_id: str):
        self.result_id = result_id
        self.parts = []
        self.results = None
        self.errors = None
        self.ready = False
        self.failed = False
        print('ResultMixin')

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
        self.emit('failed', error)

    def done(self, info=None):
        """
        Emits the done signal

        :param info: results or None
        """
        self.results = info if info is not None else self.parts
        self.ready = True
        self.emit('done')

    def is_ready(self) -> bool:
        """
        Check if the result is ready
        """
        return self.ready


class Result(ResultMixin, SignalObject):
    """
    A pure python result object for handling asynchronous results

    """
    __slots__ = ('result_id', 'parts', 'results', 'ready', 'failed', 'errors')
    SIGNALS = ('update', 'done', 'failed')

    def __init__(self, result_id: str):
        ResultMixin.__init__(self, result_id)
        SignalObject.__init__(self)

