from __future__ import annotations

from typing import Callable

from gi.repository import GLib


class GLibDispatcher:
    """Dispatcher adapter that marshals Result callbacks onto the GLib main loop."""

    def dispatch(self, fn: Callable[[], None]) -> None:
        if GLib.main_context_get_thread_default():
            fn()
        else:
            def _run() -> bool:
                fn()
                return False

            GLib.idle_add(_run)
