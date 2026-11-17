from threading import Thread

from szrpc.client import Client
from szrpc.protocol import Response, ResponseType
from szrpc.result import Result
from szrpc.server import Server, Service, ServiceFactory, Worker
from szrpc.transport import InMemoryNetwork


class RecordingDispatcher:
    def __init__(self):
        self.pending = []

    def dispatch(self, fn):
        self.pending.append(fn)

    def flush(self):
        while self.pending:
            self.pending.pop(0)()


class EchoService(Service):
    def remote__echo(self, request, value=None):
        return value


def test_result_connect_disconnect_stable_tokens():
    res = Result(b'req-1')
    calls = []

    tok1 = res.connect('update', lambda r, v: calls.append(('a', v)))
    tok2 = res.connect('update', lambda r, v: calls.append(('b', v)))
    assert tok1 != tok2

    res.disconnect('update', tok1)
    res.process(Response(b'c1', b'req-1', ResponseType.UPDATE, 42))
    assert calls == [('b', 42)]

    # Disconnect by callable
    cb = lambda r, v: calls.append(('c', v))
    res.connect('done', cb)
    res.disconnect('done', cb)
    res.process(Response(b'c1', b'req-1', ResponseType.DONE, 'ok'))
    assert calls == [('b', 42)]
    assert res.is_ready()
    assert res.results == 'ok'


def test_result_wait_and_error():
    res = Result(b'req-2')
    assert not res.wait(timeout=0.01)

    res.process(Response(b'c1', b'req-2', ResponseType.ERROR, 'bad input'))
    assert res.wait(timeout=0.1)
    assert res.failed_state
    assert res.errors == 'bad input'
    assert res.error_info == {'type': None, 'message': 'bad input', 'code': None}

    res2 = Result(b'req-3')
    res2.process(
        Response(b'c1', b'req-3', ResponseType.ERROR, {'type': 'BusyError', 'message': 'locked', 'code': 409})
    )
    assert res2.failed_state
    assert res2.errors == 'BusyError: locked'
    assert res2.error_info == {'type': 'BusyError', 'message': 'locked', 'code': 409}


def test_client_dispatcher_seam():
    net = InMemoryNetwork()
    factory = ServiceFactory(EchoService)
    dispatcher = RecordingDispatcher()

    with Server(factory, instances=0, transport=net.broker()) as server:
        with Worker('', factory.new(), transport=net.worker(b'w-1')) as worker:
            st = Thread(target=server.run, daemon=True)
            wt = Thread(target=worker.run, daemon=True)
            st.start()
            wt.start()

            with Client(methods=('echo',), transport=net.client(b'c-1'), dispatcher=dispatcher) as client:
                seen = []
                res = client.echo(value='hello')
                res.connect('done', lambda r, val: seen.append(val))
                assert res.wait(timeout=2)
                # Callback is queued in dispatcher, not invoked yet
                assert seen == []
                dispatcher.flush()
                assert seen == ['hello']

        wt.join(timeout=1)
    st.join(timeout=1)


def test_client_use_shim_sets_default_dispatcher():
    prev = Client.DEFAULT_DISPATCHER
    try:
        disp = RecordingDispatcher()
        Client.use(disp)
        assert Client.DEFAULT_DISPATCHER is disp
    finally:
        Client.DEFAULT_DISPATCHER = prev
