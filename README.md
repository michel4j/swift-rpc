# Swift RPC (`szrpc`)

Lightweight ZeroMQ RPC framework in Python using MessagePack serialization, supporting streaming replies, load-balanced workers, request cancellation, PUB/SUB broadcast events, and GUI main-loop integration (GTK / Qt).

## Installation

```bash
pip install szrpc
# Optional extras:
pip install "szrpc[dash]"   # FastAPI/Uvicorn introspection dashboard
pip install "szrpc[gtk]"    # GLib/GTK dispatcher
pip install "szrpc[qt]"     # PySide6/Qt dispatcher
```

## Writing a Service

Subclass `szrpc.server.Service` and prefix exposed methods with `remote__`. The first parameter is always `request`; remaining parameters must be keyword arguments.

```python
from szrpc.server import Service, control

class EngineService(Service):
    def on_start(self) -> None:
        self.running = True

    def on_shutdown(self) -> None:
        self.running = False

    def remote__scan(self, request, steps: int = 5):
        request.ready()  # return worker to balancing pool while streaming
        for step in range(steps):
            if request.is_cancelled():
                return 'cancelled'
            request.reply({'step': step})
            self.publish('engine.step', {'step': step})
        return 'complete'

    @control
    def remote__status(self, request):
        return {'running': self.running}
```

### Key `Service` & `Request` features
- **Lifecycle hooks**: `on_start()` runs when the `Worker` starts; `on_shutdown()` runs in `finally:` on exit.
- **Streaming replies**: call `request.reply(data)` for `UPDATE` chunks; the return value is sent as `DONE`.
- **Streaming-aware balancing**: call `request.ready()` or decorate control methods with `@control` so `instances=1` under `proxy='balancing'` never stalls control RPCs.
- **Structured errors**: unhandled exceptions return `ResponseType.ERROR` with `{'type', 'message', 'code'}` (reading `getattr(exc, 'code', None)`).
- **Cancellation**: check `request.is_cancelled()` or register `request.on_cancel(cb)` to clean up when `res.cancel()` is called or `client_timeout` expires.
- **Broadcast events**: call `self.publish(topic, payload)` to emit unsolicited events to subscribed clients.

## Running a Server

Pass a `Service` class directly or `Service.factory(*args, **kwargs)` to bind constructor arguments:

```python
from szrpc.server import Server

with Server(
    EngineService,
    ports=(9990, 9991),
    instances=1,
    threaded=True,        # run workers in-process in threads (no multiprocessing)
    pub_port=9992,        # optional XPUB/XSUB broadcast channel (uses 9992 & 9993)
    monitor_port=8080,    # optional FastAPI dashboard (requires szrpc[dash])
    client_timeout=None,  # optional seconds before cancelling idle clients' active calls
) as server:
    server.start(proxy='balancing')  # non-blocking; or server.run() to block
```

## Creating a Client

```python
from szrpc.client import Client

with Client('tcp://localhost:9990', sub_address='tcp://localhost:9992') as client:
    client.subscribe('engine.', lambda topic, payload: print(topic, payload))

    res = client.scan(steps=3)
    res.connect('update', lambda r, data: print('update:', data))
    res.connect('done', lambda r, data: print('done:', data))
    res.connect('failed', lambda r, err: print('failed:', err, r.error_info))

    res.wait(timeout=5)
```

For GTK or Qt applications, inject a `Dispatcher` adapter (or use the `Client.use(...)` compatibility shim):

```python
from szrpc.result.gresult import GLibDispatcher
# or: from szrpc.result.qresult import QtDispatcher

client = Client('tcp://localhost:9990', dispatcher=GLibDispatcher())
```

## Testing

Automated tests run in-process over `InMemoryNetwork` (no TCP sockets or subprocesses required):

```bash
poetry run pytest -v
```
