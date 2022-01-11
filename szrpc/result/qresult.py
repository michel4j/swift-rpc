
from PyQt5.QtCore import QObject, pyqtSignal


class QResult(QObject):

    sig_done = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='done')
    sig_update = pyqtSignal('PyQt_PyObject', 'PyQt_PyObject', name='update')
    sig_failed = pyqtSignal('PyQt_PyObject', str, name='failed')

    def __init__(self, result_id: str):
        self.result_id = result_id
        self.parts = []
        self.results = None
        self.errors = None
        self.ready = False
        self.failed = False

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
