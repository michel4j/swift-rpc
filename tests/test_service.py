from threading import Thread

from szrpc.client import Client
from szrpc.server import Server, Service, ServiceFactory, Worker
from szrpc.transport import InMemoryNetwork


class ScaledService(Service):
    """Multiplies inputs by a configured scale."""

    def __init__(self, scale: int = 1):
        super().__init__()
        self.scale = scale

    def remote__mul(self, request, x=1):
        return x * self.scale


def test_service_methods_and_description():
    methods = ScaledService.get_methods()
    assert 'mul' in methods
    assert 'client_config' in methods
    assert 'ping' in methods
    assert ScaledService().allowed_methods == methods
    assert 'ScaledService' in ScaledService.get_description()
    assert 'Multiplies inputs' in ScaledService.get_description()


def test_server_accepts_service_class_directly():
    net = InMemoryNetwork()
    with Server(ScaledService, instances=0, transport=net.broker()) as server:
        with Worker('', ScaledService(), transport=net.worker(b'w-1')) as worker:
            st = Thread(target=server.run, daemon=True)
            wt = Thread(target=worker.run, daemon=True)
            st.start()
            wt.start()

            with Client(transport=net.client(b'c-1')) as client:
                res = client.mul(x=7)
                assert res.wait(timeout=2)
                assert res.results == 7

        wt.join(timeout=1)
    st.join(timeout=1)


def test_service_factory_partial_and_legacy_alias():
    net = InMemoryNetwork()
    factory = ScaledService.factory(scale=10)

    with Server(factory, instances=0, transport=net.broker()) as server:
        with Worker('', factory(), transport=net.worker(b'w-1')) as worker:
            st = Thread(target=server.run, daemon=True)
            wt = Thread(target=worker.run, daemon=True)
            st.start()
            wt.start()

            with Client(transport=net.client(b'c-2')) as client:
                res = client.mul(x=3)
                assert res.wait(timeout=2)
                assert res.results == 30

        wt.join(timeout=1)
    st.join(timeout=1)

    legacy = ServiceFactory(ScaledService, scale=5)
    assert legacy.get_methods() == ScaledService.get_methods()
    assert legacy.new().scale == 5
