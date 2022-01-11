from gi.repository import GObject, GLib


class GResult(GObject.GObject):
    __gsignals__ = {
        'update': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'done': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'failed': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
    }

    def __init__(self, request_id: str):
        super().__init__()
        self.result_id = request_id
        self.parts = []
        self.results = None
        self.errors = None
        self.ready = False
        self.failed = False

    def emit(self, signal, *args):
        if GLib.main_context_get_thread_default():
            self.emit(signal, *args)
        else:
            GLib.idle_add(super().emit, signal, *args)

    def process(self):
        pass

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
        self.emit('done', info)

    def is_ready(self) -> bool:
        """
        Check if the result is ready
        """
        return self.ready

