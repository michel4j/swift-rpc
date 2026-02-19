from . import ResultMixin
from PyQt5.QtCore import QObject, pyqtSignal


class QResult(ResultMixin, QObject):

    sig_done = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='done')
    sig_update = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='update')
    sig_failed = pyqtSignal('PyQt_PyObject', str, name='failed')

    def __init__(self, result_id: bytes):
        super().__init__()
        self.setup(result_id)
        self.__sig_map = {
            'done': self.sig_done,
            'update': self.sig_update,
            'failed': self.sig_failed
        }

    def connect(self, signal, slot, *args, **kwargs):
        return self.__sig_map[signal].connect(slot)

    def disconnect(self, signal, slot):
        return self.__sig_map[signal].disconnect(slot)

    def trigger(self, signal, *args):
        if signal == 'done':
            return self.sig_done.emit(self, *args)
        else:
            return self.__sig_map[signal].emit(self, *args)

    def process(self):
        pass
