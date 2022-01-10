from . import ResultMixin
from gi.repository import GObject, GLib


class GResult(ResultMixin, GObject):
    __gsignals__ = {
        'update': (GObject.SIGNAL_RUN_FIRST, None, (object,)),
        'done': (GObject.SIGNAL_RUN_FIRST, None, ()),
        'failed': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
    }
    SIGNALS = ('update', 'done', 'failed')

    def __init__(self, request_id: str):
        ResultMixin.__init__(self, request_id)
        GObject.__init__(self)

    def emit(self, signal, *args):
        if GLib.main_context_get_thread_default():
            self.emit(signal, *args)
        else:
            GLib.idle_add(super().emit, signal, *args)
