from . import ResultMixin
from PySide6.QtCore import QObject, Signal


class QResult(ResultMixin, QObject):

    done = Signal(object, object, name='done')
    update = Signal(object, object, name='update')
    failed = Signal(object, str, name='failed')

    def __init__(self, result_id: bytes):
        super().__init__()
        self.setup(result_id)
        self.__sig_map = {
            'done': self.done,
            'update': self.update,
            'failed': self.failed
        }

    def connect(self, signal, slot, *args, **kwargs):
        return self.__sig_map[signal].connect(slot)

    def disconnect(self, signal, slot):
        self.__sig_map[signal].disconnect(slot)

    def trigger(self, signal, *args):
        self.__sig_map[signal].emit(self, *args)

    def post_process(self):
        pass
