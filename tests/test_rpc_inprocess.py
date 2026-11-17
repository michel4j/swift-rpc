from threading import Thread

import pytest

from szrpc.client import Client
from szrpc.server import Server, Service, ServiceFactory, Worker
from szrpc.transport import InMemoryNetwork


class BusyError(RuntimeError):
    code = 'RESOURCE_BUSY'


class MathService(Service):
    def remote__add(self, request, x=0, y=0):
        return x + y

    def remote__stream(self, request, count=2):
        for i in range(count):
            request.reply(i)
        return None

    def remote__fail(self, request):
        raise ValueError('boom')

    def remote__busy(self, request):
        raise BusyError('motor moving')

    def remote__only_a(self, request):
        return 'from-a'

    def remote__only_b(self, request):
        return 'from-b'


def _start_thread(target, *args, **kwargs):
    t = Thread(target=target, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t


@pytest.mark.parametrize('proxy_mode', ['round-robin', 'balancing'])
def test_client_server_worker_roundtrip(proxy_mode):
    net = InMemoryNetwork()
    factory = ServiceFactory(MathService)

    with Server(factory, instances=0, transport=net.broker()) as server:
        with Worker('', factory.new(), transport=net.worker(b'w-1')) as worker:
            server_t = _start_thread(server.run, proxy=proxy_mode)
            worker_t = _start_thread(worker.run)

            with Client(transport=net.client(b'c-1')) as client:
                assert 'add' in client.remote_methods

                res = client.add(x=4, y=5)
                assert res.wait(timeout=2)
                assert res.results == 9

                stream_res = client.stream(count=3)
                assert stream_res.wait(timeout=2)
                assert stream_res.results == [0, 1, 2]

                err_res = client.fail()
                assert err_res.wait(timeout=2)
                assert err_res.failed_state
                assert 'boom' in err_res.errors
                assert err_res.error_info == {'type': 'ValueError', 'message': 'boom', 'code': None}

                busy_res = client.busy()
                assert busy_res.wait(timeout=2)
                assert busy_res.failed_state
                assert busy_res.error_info == {
                    'type': 'BusyError',
                    'message': 'motor moving',
                    'code': 'RESOURCE_BUSY',
                }

        worker_t.join(timeout=1)
    server_t.join(timeout=1)


@pytest.mark.parametrize('proxy_mode', ['round-robin', 'balancing'])
def test_method_filtered_worker_routing(proxy_mode):
    net = InMemoryNetwork()
    factory = ServiceFactory(MathService)

    with Server(factory, instances=0, transport=net.broker()) as server:
        w_a = Worker('', factory.new(), methods=('only_a',), transport=net.worker(b'w-a'))
        w_b = Worker('', factory.new(), methods=('only_b',), transport=net.worker(b'w-b'))
        with w_a, w_b:
            server_t = _start_thread(server.run, proxy=proxy_mode)
            t_a = _start_thread(w_a.run)
            t_b = _start_thread(w_b.run)

            with Client(methods=('only_a', 'only_b'), transport=net.client(b'c-2')) as client:
                res_a = client.only_a()
                res_b = client.only_b()
                assert res_a.wait(timeout=2)
                assert res_b.wait(timeout=2)
                assert res_a.results == 'from-a'
                assert res_b.results == 'from-b'

        t_a.join(timeout=1)
        t_b.join(timeout=1)
    server_t.join(timeout=1)
