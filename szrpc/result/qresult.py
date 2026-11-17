from __future__ import annotations

from typing import Callable

from PySide6.QtCore import QObject, Signal


class QtDispatcher(QObject):
    """Dispatcher adapter that marshals Result callbacks onto the Qt main thread."""

    _invoked = Signal(object)

    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self._invoked.connect(self._run)

    @staticmethod
    def _run(fn: Callable[[], None]) -> None:
        fn()

    def dispatch(self, fn: Callable[[], None]) -> None:
        self._invoked.emit(fn)
