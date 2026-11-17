from threading import Event

from szrpc.client import Client
from szrpc.protocol import Request
from szrpc.server import Server, Service
from szrpc.transport import InMemoryNetwork


class CancellableService(Service):
    def __init__(self, cleanup_event: Event):
        super().__init__()
        self.cleanup_event = cleanup_event
        self.started = Event()

    def remote__watch(self, request):
        request.on_cancel(self.cleanup_event.set)
        request.ready()
        self.started.set()
        request.cancelled.wait(timeout=2)
        return 'cancelled' if request.is_cancelled() else 'timed-out'


def test_request_cancel_unit():
    req = Request(b'c1', b'c1-r1', 'watch', {})
    seen = []
    req.on_cancel(lambda: seen.append('cb1'))
    assert not req.is_cancelled()

    req.cancel()
    assert req.is_cancelled()
    assert seen == ['cb1']

    # Registering after cancel runs immediately
    req.on_cancel(lambda: seen.append('cb2'))
    assert seen == ['cb1', 'cb2']


def test_client_result_cancel_signals_worker():
    net = InMemoryNetwork()
    cleanup_event = Event()

    with Server(
        CancellableService.factory(cleanup_event),
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
    ) as server:
        server.start(proxy='balancing')
        with Client(transport=net.client(b'c-1')) as client:
            res = client.watch()
            assert not res.is_ready()
            res.cancel()
            assert res.wait(timeout=2)
            assert res.results == 'cancelled'
            assert cleanup_event.is_set()


def test_server_client_timeout_cancels_active_requests():
    net = InMemoryNetwork()
    cleanup_event = Event()

    with Server(
        CancellableService.factory(cleanup_event),
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
        client_timeout=0.1,
    ) as server:
        server.start(proxy='balancing')
        with Client(transport=net.client(b'c-2')) as client:
            res = client.watch()
            assert res.wait(timeout=2)
            assert res.results == 'cancelled'
            assert cleanup_event.is_set()
