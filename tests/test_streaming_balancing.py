from threading import Event

from szrpc.client import Client
from szrpc.server import Server, Service, control
from szrpc.transport import InMemoryNetwork


class StreamingEngineService(Service):
    def __init__(self):
        super().__init__()
        self.started = Event()
        self.stop_signal = Event()

    def remote__run_stream(self, request):
        request.ready()
        self.started.set()
        request.reply('tick-1')
        self.stop_signal.wait(timeout=2)
        return 'finished'

    def remote__blocking_job(self, request):
        self.started.set()
        self.stop_signal.wait(timeout=2)
        return 'unblocked'

    @control
    def remote__stop_engine(self, request):
        self.stop_signal.set()
        return 'stopped'

    def remote__status(self, request):
        return 'running' if not self.stop_signal.is_set() else 'stopped'


def test_balancing_ready_allows_concurrent_rpc_with_single_worker():
    net = InMemoryNetwork()

    with Server(
        StreamingEngineService,
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
    ) as server:
        server.start(proxy='balancing')
        with Client(transport=net.client(b'c-1')) as client:
            stream_res = client.run_stream()
            # Even with instances=1 under balancing, status() and stop_engine() do not stall
            status_res = client.status()
            assert status_res.wait(timeout=2)
            assert status_res.results == 'running'

            stop_res = client.stop_engine()
            assert stop_res.wait(timeout=2)
            assert stop_res.results == 'stopped'

            assert stream_res.wait(timeout=2)
            assert stream_res.results == 'finished'


def test_control_decorator_bypasses_busy_worker_in_balancing():
    net = InMemoryNetwork()

    with Server(
        StreamingEngineService,
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
    ) as server:
        server.start(proxy='balancing')
        with Client(transport=net.client(b'c-2')) as client:
            job_res = client.blocking_job()
            # blocking_job did NOT call request.ready(), yet @control stop_engine() still runs
            stop_res = client.stop_engine()
            assert stop_res.wait(timeout=2)
            assert stop_res.results == 'stopped'

            assert job_res.wait(timeout=2)
            assert job_res.results == 'unblocked'
