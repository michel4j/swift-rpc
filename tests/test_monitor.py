from threading import Thread

import pytest

from szrpc.client import Client
from szrpc.server import Server, Service, Worker
from szrpc.transport import InMemoryNetwork


class SampleService(Service):
    def remote__ping_pong(self, request, msg='hi'):
        return msg


def test_monitor_records_and_clears_workers():
    mon_mod = pytest.importorskip('szrpc.monitor')
    monitor = mon_mod.Monitor(service_info={'description': 'Sample', 'methods': ('ping_pong',)})

    monitor.record_request('w-1', 'c-1', 'req-1', 'ping_pong', {'msg': 'a'})
    monitor.record_response('w-1', 'req-1', 'DONE', 'a')
    data = monitor.get_data()
    assert data['stats']['total_requests'] == 1
    assert len(data['historical_calls']) == 1

    # Active call cleared cleanly when worker disconnects (str ID, no infinite loop)
    monitor.record_request('w-2', 'c-1', 'req-2', 'ping_pong', {})
    monitor.clear_workers(['w-2'])
    data2 = monitor.get_data()
    assert data2['active_calls'] == []
    assert data2['stats']['total_errors'] == 1
    assert 'w-2' not in data2['workers']


def test_monitor_fastapi_routes():
    import asyncio
    mon_mod = pytest.importorskip('szrpc.monitor')

    m1 = mon_mod.Monitor(service_info={'description': 'One', 'methods': ('ping_pong',)})
    m2 = mon_mod.Monitor(service_info={'description': 'Two', 'methods': ()})
    m1.record_request('w-1', 'c-1', 'req-99', 'ping_pong', {'msg': 'ok'})

    app1 = mon_mod.create_app(m1)
    app2 = mon_mod.create_app(m2)
    routes1 = {r.path: r.endpoint for r in app1.routes if hasattr(r, 'endpoint')}
    routes2 = {r.path: r.endpoint for r in app2.routes if hasattr(r, 'endpoint')}

    assert asyncio.run(routes1['/api/data']())['stats']['total_requests'] == 1
    assert asyncio.run(routes2['/api/data']())['stats']['total_requests'] == 0
    assert asyncio.run(routes1['/api/details/{request_id}']('req-99'))['kwargs'] == {'msg': 'ok'}
    assert asyncio.run(routes2['/api/details/{request_id}']('req-99')) == {'error': 'Request not found'}


def test_server_emits_events_to_injected_monitor():
    mon_mod = pytest.importorskip('szrpc.monitor')
    net = InMemoryNetwork()
    monitor = mon_mod.Monitor(service_info={'description': 'Sample', 'methods': ('ping_pong',)})

    with Server(SampleService, instances=0, transport=net.broker(), monitor=monitor) as server:
        with Worker('', SampleService(), transport=net.worker(b'w-1')) as worker:
            st = Thread(target=server.run, daemon=True)
            wt = Thread(target=worker.run, daemon=True)
            st.start()
            wt.start()

            with Client(transport=net.client(b'c-1')) as client:
                res = client.ping_pong(msg='hello')
                assert res.wait(timeout=2)
                assert res.results == 'hello'

        wt.join(timeout=1)
    st.join(timeout=1)

    data = monitor.get_data()
    assert data['stats']['total_requests'] == 1
    assert len(data['historical_calls']) == 1
    assert data['historical_calls'][0]['status'] == 'DONE'
