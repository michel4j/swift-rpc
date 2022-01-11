from . import ResultMixin
from PyQt5.QtCore import QObject, pyqtSignal


class QResult(QObject, ResultMixin):

    sig_done = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='done')
    sig_update = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='update')
    sig_failed = pyqtSignal('PyQt_PyObject', str, name='failed')

    def __init__(self, result_id: str):
        ResultMixin.__init__(self, result_id)
        QObject.__init__(self)
        self.__sig_map = {
            'done': self.sig_done,
            'update': self.sig_update,
            'failed': self.sig_failed
        }

    def connect(self, signal, slot, *args, **kwargs):
        return self.__sig_map[signal].connect(slot)

    def disconnect(self, signal, slot):
        return self.__sig_map[signal].disconnect(slot)

    def emit(self, signal, *args):
        if signal == 'done':
            return self.sig_done.emit(self, *args)
        else:
            return self.__sig_map[signal].emit(self, *args)
