import socket

from szrpc.client import Client
from szrpc.server import Server, Service
from szrpc.transport import InMemoryNetwork


class SharedStateService(Service):
    def __init__(self, shared_log: list[str]):
        super().__init__()
        self.shared_log = shared_log

    def remote__append(self, request, item=''):
        self.shared_log.append(item)
        return len(self.shared_log)


def _free_port_pair() -> tuple[int, int]:
    socks = []
    ports = []
    for _ in range(2):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', 0))
        ports.append(s.getsockname()[1])
        socks.append(s)
    for s in socks:
        s.close()
    return ports[0], ports[1]


def test_threaded_server_start_and_shared_inprocess_state():
    net = InMemoryNetwork()
    shared_log: list[str] = []

    with Server(
        SharedStateService.factory(shared_log),
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
    ) as server:
        broker_thread = server.start()
        assert broker_thread.is_alive()

        with Client(transport=net.client(b'c-1')) as client:
            r1 = client.append(item='alpha')
            r2 = client.append(item='beta')
            assert r1.wait(timeout=2)
            assert r2.wait(timeout=2)
            assert shared_log == ['alpha', 'beta']

    assert not broker_thread.is_alive()


def test_threaded_server_over_zmq():
    front_port, back_port = _free_port_pair()
    shared_log: list[str] = []

    with Server(
        SharedStateService.factory(shared_log),
        ports=(front_port, back_port),
        instances=1,
        threaded=True,
    ) as server:
        server.start()
        with Client(f'tcp://127.0.0.1:{front_port}', linger=False) as client:
            res = client.append(item='zmq-item')
            assert res.wait(timeout=3)
            assert res.results == 1
            assert shared_log == ['zmq-item']
