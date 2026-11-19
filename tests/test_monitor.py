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
    monitor.update_worker('w-2', methods=('hello_world', 'date'))
    assert monitor.get_data()['worker_methods']['w-2'] == ['hello_world', 'date']
    monitor.record_request('w-2', 'c-1', 'req-2', 'ping_pong', {})
    monitor.clear_workers(['w-2'])
    data2 = monitor.get_data()
    assert data2['active_calls'] == []
    assert data2['stats']['total_errors'] == 1
    assert 'w-2' not in data2['workers']
    assert 'w-2' not in data2['worker_methods']


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
    assert data['worker_methods'] == {'w-1': ['ping_pong']}
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
        worker_tasks = asyncio.run(routes['/api/metrics/tasks'](window='15m', worker_id=None, limit_per_worker=120))['tasks']
        assert [t['status'] for t in worker_tasks['w-1']] == ['DONE', 'ERROR']
        assert [t['method'] for t in worker_tasks['w-1']] == ['fast', 'slow']
        assert all(t['duration'] >= 0.0 for t in worker_tasks['w-1'])
        assert asyncio.run(routes['/api/calls'](window='15m', status=None, method=None, worker_id=None, limit=10, offset=0))['total'] == 2
        index_resp = asyncio.run(routes['/']())
        html = index_resp.path.read_text(encoding='utf-8')
        assert 'data-bs-sidebar="folded-hover"' in html
        assert 'id="page-root"' in html
        assert 'id="request-details-drawer"' in html
        assert 'id="live-announcer"' in html
        assert '/static/css/monitor.css' in html
        assert '/static/js/app.js' in html
        assert 'cdn.jsdelivr.net' not in html.lower()
        assert 'chart.js' not in html.lower()

        assert '/sparklines.js' not in routes
        static_mount = next(r for r in app.routes if getattr(r, 'path', None) == '/static')
        sparklines_resp = asyncio.run(
            static_mount.app.get_response('sparklines.js', {'type': 'http', 'method': 'GET', 'headers': []})
        )
        assert sparklines_resp.status_code == 200
        assert 'javascript' in (sparklines_resp.media_type or '')
        sparklines_js = (mon_mod.DATA_DIR / 'sparklines.js').read_text(encoding='utf-8')
        assert 'export function updateWorkerSparklines' in sparklines_js

        # Retention pruning removes expired rows
        old_ts = time.time() - 10 * 86400
        m2.store.WorkerMetric.create(ts=old_ts, worker_id='w-old', cpu_pct=1.0)
        m2.store._prune_old()
        assert m2.store.WorkerMetric.select().where(m2.store.WorkerMetric.worker_id == 'w-old').count() == 0
    finally:
        m2.close()


class OtherService(Service):
    def remote__echo(self, request, val=1):
        return val


def test_default_db_path_per_service_and_port(monkeypatch, tmp_path):
    mon_mod = pytest.importorskip('szrpc.monitor')
    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path))

    assert mon_mod.default_db_path() == tmp_path / 'szrpc' / 'monitor.db'
    assert mon_mod.default_db_path('MyService', 9990) == tmp_path / 'szrpc' / 'monitor-myservice-9990.db'
    assert mon_mod.default_db_path('My / Odd Service!', 9992) == tmp_path / 'szrpc' / 'monitor-my-odd-service-9992.db'
    assert mon_mod.default_db_path(port=9995) == tmp_path / 'szrpc' / 'monitor-9995.db'


def test_server_uses_distinct_default_db_paths_per_service_instance(monkeypatch, tmp_path):
    mon_mod = pytest.importorskip('szrpc.monitor')
    import szrpc.server as srv_mod

    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path))
    monkeypatch.setattr(mon_mod, 'start_monitor_thread', lambda monitor, port=8080: None)
    monkeypatch.setattr(srv_mod.mon, 'start_monitor_thread', lambda monitor, port=8080: None)

    net = InMemoryNetwork()
    with (
        Server(SampleService, ports=(9990, 9991), instances=0, transport=net.broker(), monitor_port=8080) as s1,
        Server(SampleService, ports=(9992, 9993), instances=0, transport=net.broker(), monitor_port=8081) as s2,
        Server(OtherService, ports=(9994, 9995), instances=0, transport=net.broker(), monitor_port=8082) as s3,
    ):
        assert s1.monitor.store.db_path == str(tmp_path / 'szrpc' / 'monitor-sampleservice-9990.db')
        assert s2.monitor.store.db_path == str(tmp_path / 'szrpc' / 'monitor-sampleservice-9992.db')
        assert s3.monitor.store.db_path == str(tmp_path / 'szrpc' / 'monitor-otherservice-9994.db')

        s1.monitor.record_request('w-1', 'c-1', 'req-s1', 'ping_pong', {})
        s1.monitor.record_response('w-1', 'req-s1', 'DONE', 'ok')
        s2.monitor.record_request('w-2', 'c-2', 'req-s2', 'ping_pong', {})
        s2.monitor.record_response('w-2', 'req-s2', 'DONE', 'ok')
        s1.monitor.flush()
        s2.monitor.flush()

        assert s1.monitor.get_summary()['total_requests'] == 1
        assert s1.monitor.get_calls()['calls'][0]['request_id'] == 'req-s1'
        assert s2.monitor.get_summary()['total_requests'] == 1
        assert s2.monitor.get_calls()['calls'][0]['request_id'] == 'req-s2'
        assert s3.monitor.get_summary()['total_requests'] == 0


def test_memory_monitor_store_persists_across_writer_thread():
    mon_mod = pytest.importorskip('szrpc.monitor')
    monitor = mon_mod.Monitor(service_info={'description': 'Mem', 'methods': ('ping_pong',)}, db_path=':memory:')
    try:
        monitor.record_request('w-1', 'c-1', 'req-mem-1', 'ping_pong', {'a': 1})
        monitor.record_response('w-1', 'req-mem-1', 'DONE', 'pong')
        monitor.update_worker('w-1', {'cpu_pct': 12.5, 'mem_rss': 1024, 'net_rx_bps': 1.0, 'net_tx_bps': 2.0, 'active_tasks': 1})
        monitor.flush()

        summary = monitor.get_summary(window='15m')
        assert summary['total_requests'] == 1
        assert monitor.get_calls()['total'] == 1
        assert 'w-1' in monitor.get_worker_metrics(window='15m')['series']
    finally:
        monitor.close()


def _concurrent_store_worker(db_path_str: str, worker_idx: int, count: int, start_barrier, err_queue) -> None:
    try:
        import time
        from szrpc.monitor.store import SqliteMonitorStore

        start_barrier.wait()
        store = SqliteMonitorStore(db_path=db_path_str, flush_interval=0.05, batch_size=10)
        try:
            now = time.time()
            for i in range(count):
                req_id = f'proc-{worker_idx}-req-{i}'
                store.enqueue_call({
                    'request_id': req_id,
                    'client_id': f'c-{worker_idx}',
                    'method': f'm_{worker_idx}',
                    'signature': '',
                    'worker_id': f'w-{worker_idx}',
                    'start_time': now,
                    'end_time': now + 0.01,
                    'duration': 0.01,
                    'status': 'DONE',
                    'sent_bytes': 16,
                    'updates': 0,
                    'kwargs_json': '{}',
                    'results_json': '["ok"]',
                })
                store.enqueue_metric({
                    'ts': now,
                    'worker_id': f'w-{worker_idx}',
                    'cpu_pct': float(worker_idx),
                    'mem_rss': 1024,
                    'net_rx_bps': 0.0,
                    'net_tx_bps': 0.0,
                    'active_tasks': 1,
                })
            store.flush(timeout=10.0)
            store._prune_old()
        finally:
            store.close()
    except BaseException as exc:
        err_queue.put((worker_idx, repr(exc)))


def test_sqlite_store_multiprocess_concurrent_writes(tmp_path):
    import multiprocessing

    mon_mod = pytest.importorskip('szrpc.monitor')
    db_file = tmp_path / 'shared_monitor.db'
    ctx = multiprocessing.get_context('spawn')
    num_procs = 4
    per_proc = 25
    barrier = ctx.Barrier(num_procs)
    err_queue = ctx.Queue()

    procs = [
        ctx.Process(target=_concurrent_store_worker, args=(str(db_file), idx, per_proc, barrier, err_queue))
        for idx in range(num_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
        assert p.exitcode == 0

    errors = []
    while not err_queue.empty():
        errors.append(err_queue.get_nowait())
    assert errors == []

    reader = mon_mod.SqliteMonitorStore(db_path=db_file)
    try:
        assert reader.get_summary(window='15m')['total_requests'] == num_procs * per_proc
        assert reader.WorkerMetric.select().count() == num_procs * per_proc
    finally:
        reader.close()


def test_sqlite_store_retries_transient_lock(tmp_path):
    import sqlite3
    import time
    from threading import Event

    mon_mod = pytest.importorskip('szrpc.monitor')
    db_file = tmp_path / 'locked.db'

    # Initialize DB first so WAL exists, then briefly hold an EXCLUSIVE lock from another connection
    store = mon_mod.SqliteMonitorStore(db_path=db_file, flush_interval=0.05, busy_timeout_ms=50)
    try:
        locked = Event()

        def _hold_lock():
            blocker = sqlite3.connect(str(db_file), isolation_level=None)
            try:
                blocker.execute('BEGIN EXCLUSIVE')
                locked.set()
                time.sleep(0.15)
                blocker.execute('ROLLBACK')
            finally:
                blocker.close()

        holder = Thread(target=_hold_lock, daemon=True)
        holder.start()
        assert locked.wait(timeout=2)

        now = time.time()
        store._flush_batch(
            calls=[{
                'request_id': 'req-retry-1',
                'client_id': 'c-1',
                'method': 'ping',
                'signature': '',
                'worker_id': 'w-1',
                'start_time': now,
                'end_time': now + 0.01,
                'duration': 0.01,
                'status': 'DONE',
                'sent_bytes': 4,
                'updates': 0,
                'kwargs_json': '{}',
                'results_json': '["ok"]',
            }],
            metrics=[],
        )
        holder.join(timeout=2)
        assert store.get_call_details('req-retry-1') is not None
    finally:
        store.close()


def test_monitor_static_assets_and_spa_routes():
    import asyncio
    import re
    from starlette.exceptions import HTTPException as StarletteHTTPException

    mon_mod = pytest.importorskip('szrpc.monitor')

    monitor = mon_mod.Monitor(service_info={'description': 'Static', 'methods': ()})
    try:
        app = mon_mod.create_app(monitor)
        routes = {r.path: r.endpoint for r in app.routes if hasattr(r, 'endpoint')}

        # All 4 SPA routes serve monitor.html with folded-hover shell & zero external CDNs
        for path in ('/', '/workers', '/methods', '/requests'):
            assert path in routes
            resp = asyncio.run(routes[path]())
            assert resp.path == mon_mod.DATA_DIR / 'monitor.html'
            html = resp.path.read_text(encoding='utf-8')
            assert 'data-bs-sidebar="folded-hover"' in html
            assert '/static/js/app.js' in html
            assert '/static/vendor/tabler.min.css' in html
            assert 'cdn.jsdelivr.net' not in html.lower()
            for tag in re.findall(r'<(?:link|script)\b[^>]*>', html, flags=re.IGNORECASE):
                assert 'http://' not in tag.lower(), tag
                assert 'https://' not in tag.lower(), tag

        static_mount = next(r for r in app.routes if getattr(r, 'path', None) == '/static')
        static_app = static_mount.app

        for asset, expected_ct in (
            ('vendor/tabler.min.css', 'text/css'),
            ('vendor/tabler.min.js', 'javascript'),
            ('vendor/tabler-theme.min.js', 'javascript'),
            ('css/monitor.css', 'text/css'),
            ('js/app.js', 'javascript'),
            ('js/router.js', 'javascript'),
            ('js/format.js', 'javascript'),
            ('js/drawer.js', 'javascript'),
            ('js/pages/overview.js', 'javascript'),
            ('js/pages/workers.js', 'javascript'),
            ('js/pages/methods.js', 'javascript'),
            ('js/pages/requests.js', 'javascript'),
            ('sparklines.js', 'javascript'),
        ):
            scope = {'type': 'http', 'method': 'GET', 'headers': []}
            resp = asyncio.run(static_app.get_response(asset, scope))
            assert resp.status_code == 200, asset
            assert expected_ct in (resp.media_type or '')

        with pytest.raises(StarletteHTTPException) as exc_info:
            asyncio.run(static_app.get_response('vendor/tabler.min.css.map', {'type': 'http', 'method': 'GET', 'headers': []}))
        assert exc_info.value.status_code == 404

        assert '/api/does-not-exist' not in routes
        assert '/sparklines.js' not in routes

        async def _asgi_status(path: str) -> int:
            status_code = 0

            async def receive():
                return {'type': 'http.request', 'body': b'', 'more_body': False}

            async def send(message):
                nonlocal status_code
                if message['type'] == 'http.response.start':
                    status_code = message['status']

            scope = {
                'type': 'http',
                'asgi': {'version': '3.0'},
                'http_version': '1.1',
                'method': 'GET',
                'scheme': 'http',
                'path': path,
                'raw_path': path.encode('ascii'),
                'query_string': b'',
                'headers': [],
                'client': ('127.0.0.1', 12345),
                'server': ('testserver', 80),
            }
            await app(scope, receive, send)
            return status_code

        assert asyncio.run(_asgi_status('/api/does-not-exist')) == 404
        assert asyncio.run(_asgi_status('/sparklines.js')) == 404
        assert asyncio.run(_asgi_status('/static/vendor/tabler.min.css.map')) == 404

        # Total raw static asset footprint must remain strictly < 1 MiB
        total_raw_bytes = sum(p.stat().st_size for p in mon_mod.DATA_DIR.rglob('*') if p.is_file())
        assert total_raw_bytes < 1024 * 1024, f'Monitor static assets exceed 1 MiB: {total_raw_bytes} B'
    finally:
        monitor.close()


def test_monitor_js_unit_tests():
    import shutil
    import subprocess
    from pathlib import Path

    node_bin = shutil.which('node')
    if node_bin is None:
        pytest.skip('node not found')

    js_test = Path(__file__).parent / 'js' / 'monitor.test.mjs'
    result = subprocess.run(
        [node_bin, '--test', str(js_test)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f'node --test failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}'
