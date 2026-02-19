from . import ResultMixin

from gi.repository import GObject, GLib


class GResult(ResultMixin, GObject.GObject):
    __gsignals__ = {
        'update': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'done': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'failed': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
    }

    def __init__(self, request_id: bytes):
        super().__init__()
        self.setup(request_id)

    def trigger(self, signal, *args):
        if GLib.main_context_get_thread_default():
            self.emit(signal, *args)
        else:
            GLib.idle_add(self.emit, signal, *args)

    def process(self):
        pass
