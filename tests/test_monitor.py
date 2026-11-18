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
    assert len(data['worker_metrics']) == 1
    w_metrics = data['worker_metrics']['w-1']
    assert 'cpu_pct' in w_metrics
    assert 'mem_rss' in w_metrics
    assert 'net_rx_bps' in w_metrics
    assert 'net_tx_bps' in w_metrics
    monitor.close()


def test_sqlite_store_persistence_and_windowed_queries(tmp_path):
    import asyncio
    import time
    mon_mod = pytest.importorskip('szrpc.monitor')
    db_file = tmp_path / 'monitor.db'

    m1 = mon_mod.Monitor(service_info={'description': 'Persist', 'methods': ('fast', 'slow')}, db_path=db_file)
    m1.update_worker('w-1', {'cpu_pct': 25.0, 'mem_rss': 10485760, 'net_rx_bps': 100.0, 'net_tx_bps': 200.0, 'active_tasks': 1})
    m1.update_worker('w-1', {'cpu_pct': 75.0, 'mem_rss': 20971520, 'net_rx_bps': 300.0, 'net_tx_bps': 400.0, 'active_tasks': 2})

    m1.record_request('w-1', 'c-1', 'req-1', 'fast', {'x': 1})
    m1.record_response('w-1', 'req-1', 'DONE', 'ok')
    m1.record_request('w-1', 'c-1', 'req-2', 'slow', {'x': 2})
    m1.record_response('w-1', 'req-2', 'ERROR', 'boom')
    m1.close()

    # Re-open from disk and verify persisted data + windowed queries
    m2 = mon_mod.Monitor(service_info={'description': 'Persist', 'methods': ('fast', 'slow')}, db_path=db_file)
    try:
        summary = m2.get_summary(window='15m')
        assert summary['total_requests'] == 2
        assert summary['total_errors'] == 1
        by_method = {row['method']: row for row in summary['methods']}
        assert by_method['fast']['count'] == 1
        assert by_method['fast']['errors'] == 0
        assert by_method['slow']['count'] == 1
        assert by_method['slow']['errors'] == 1

        metrics = m2.get_worker_metrics(window='15m')
        assert 'w-1' in metrics['series']
        assert len(metrics['series']['w-1']) >= 1
        latest = metrics['series']['w-1'][-1]
        assert latest['cpu_pct'] == pytest.approx(50.0, abs=0.1)
        assert latest['active_tasks'] == 2

        calls = m2.get_calls(window='15m', status='ERROR', limit=10, offset=0)
        assert calls['total'] == 1
        assert calls['calls'][0]['request_id'] == 'req-2'
        assert m2.get_call_details('req-1')['results'] == ['ok']

        # FastAPI routes
        app = mon_mod.create_app(m2)
        routes = {r.path: r.endpoint for r in app.routes if hasattr(r, 'endpoint')}
        assert asyncio.run(routes['/api/summary'](window='15m'))['total_requests'] == 2
        assert 'w-1' in asyncio.run(routes['/api/metrics/workers'](window='15m', worker_id=None))['series']
        assert asyncio.run(routes['/api/calls'](window='15m', status=None, method=None, worker_id=None, limit=10, offset=0))['total'] == 2
        index_resp = asyncio.run(routes['/']())
        html = index_resp.path.read_text(encoding='utf-8')
        assert 'id="worker-sparklines-card"' in html
        assert 'id="worker-sparklines"' in html
        assert "from './sparklines.js'" in html

        sparklines_resp = asyncio.run(routes['/sparklines.js']())
        assert sparklines_resp.media_type == 'text/javascript'
        sparklines_js = sparklines_resp.path.read_text(encoding='utf-8')
        assert 'export function updateWorkerSparklines' in sparklines_js

        # Retention pruning removes expired rows
        old_ts = time.time() - 10 * 86400
        m2.store.WorkerMetric.create(ts=old_ts, worker_id='w-old', cpu_pct=1.0)
        m2.store._prune_old()
        assert m2.store.WorkerMetric.select().where(m2.store.WorkerMetric.worker_id == 'w-old').count() == 0
    finally:
        m2.close()
