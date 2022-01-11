from . import ResultMixin

from gi.repository import GObject, GLib


class GResult(GObject.GObject, ResultMixin):
    __gsignals__ = {
        'update': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'done': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'failed': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
    }

    def __init__(self, request_id: str):
        ResultMixin.__init__(self, request_id)
        super().__init__()

    def emit(self, signal, *args):
        if GLib.main_context_get_thread_default():
            self.emit(signal, *args)
        else:
            GLib.idle_add(super().emit, signal, *args)

    def process(self):
        pass