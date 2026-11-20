import sqlite3
import time
from threading import Thread

from szrpc.monitor import Monitor
from szrpc.monitor.store import SqliteMonitorStore
from szrpc.protocol import ResponseType
from szrpc.server import Service, Worker, WorkerMetricsSampler, _WorkerMetricsSampler


class _DummyService(Service):
    def remote__echo(self, request, msg='ok'):
        return msg


class _FakeWorkerTransport:
    def __init__(self, worker_id: bytes = b'worker-1'):
        self.worker_id = worker_id
        self.sent = []
        self.closed = False

    def send(self, response):
        self.sent.append(response)

    def recv(self, timeout_ms: int = 10):
        time.sleep(timeout_ms / 1000.0)
        return None

    def close(self):
        self.closed = True


class _CustomSampler(WorkerMetricsSampler):
    def sample(self, active_tasks: int):
        metrics = super().sample(active_tasks)
        metrics['gpu_vram_free_mb'] = 4096
        return metrics


class _CustomWorker(Worker):
    def create_metrics_sampler(self) -> WorkerMetricsSampler:
        return _CustomSampler()


def test_worker_metrics_sampler_public_and_alias():
    assert _WorkerMetricsSampler is WorkerMetricsSampler
    sampler = WorkerMetricsSampler()
    data = sampler.sample(2)
    assert data['active_tasks'] == 2
    for key in ('cpu_pct', 'mem_rss', 'net_rx_bps', 'net_tx_bps', 'active_tasks'):
        assert key in data


def test_worker_subclass_create_metrics_sampler():
    transport = _FakeWorkerTransport(b'custom-w1')
    worker = _CustomWorker('inproc://unused', _DummyService(), transport=transport)
    t = Thread(target=worker.run, daemon=True)
    t.start()
    deadline = time.time() + 2.0
    while not transport.sent and time.time() < deadline:
        time.sleep(0.01)
    worker.stop()
    t.join(timeout=2.0)

    assert transport.sent
    hb = transport.sent[0]
    assert hb.type == ResponseType.HEARTBEAT
    assert hb.content['metrics']['gpu_vram_free_mb'] == 4096
    assert 'cpu_pct' in hb.content['metrics']


def test_worker_explicit_metrics_sampler_kwarg():
    transport = _FakeWorkerTransport(b'custom-w2')
    worker = Worker(
        'inproc://unused',
        _DummyService(),
        transport=transport,
        metrics_sampler=_CustomSampler(),
    )
    t = Thread(target=worker.run, daemon=True)
    t.start()
    deadline = time.time() + 2.0
    while not transport.sent and time.time() < deadline:
        time.sleep(0.01)
    worker.stop()
    t.join(timeout=2.0)

    assert transport.sent
    assert transport.sent[0].content['metrics']['gpu_vram_free_mb'] == 4096


def test_monitor_persists_extra_worker_metrics(tmp_path):
    db_path = tmp_path / 'mon.db'
    monitor = Monitor({'description': 'test', 'methods': []}, db_path=db_path)
    try:
        monitor.update_worker(
            'w1',
            metrics={
                'cpu_pct': 12.5,
                'mem_rss': 1024,
                'net_rx_bps': 10.0,
                'net_tx_bps': 20.0,
                'active_tasks': 1,
                'gpu_vram_free_mb': 8192,
            },
            methods=['echo'],
        )
        monitor.store.flush()

        live = monitor.get_data()
        assert live['worker_metrics']['w1']['gpu_vram_free_mb'] == 8192

        hist = monitor.get_worker_metrics(window='15m', worker_id='w1')
        pts = hist['series']['w1']
        assert len(pts) == 1
        assert pts[0]['extra']['gpu_vram_free_mb'] == 8192
    finally:
        monitor.close()


def test_sqlite_store_migrates_missing_extra_json_column(tmp_path):
    db_path = tmp_path / 'legacy.db'
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE worker_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            worker_id TEXT NOT NULL,
            cpu_pct REAL NOT NULL DEFAULT 0.0,
            mem_rss INTEGER NOT NULL DEFAULT 0,
            net_rx_bps REAL NOT NULL DEFAULT 0.0,
            net_tx_bps REAL NOT NULL DEFAULT 0.0,
            active_tasks INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()

    store = SqliteMonitorStore(db_path=db_path)
    try:
        store.enqueue_metric({
            'ts': time.time(),
            'worker_id': 'w-legacy',
            'cpu_pct': 1.0,
            'mem_rss': 100,
            'net_rx_bps': 0.0,
            'net_tx_bps': 0.0,
            'active_tasks': 0,
            'extra_json': '{"slots_free": 4}',
        })
        store.flush()
        res = store.get_worker_metrics(window='15m', worker_id='w-legacy')
        assert res['series']['w-legacy'][0]['extra'] == {'slots_free': 4}
    finally:
        store.close()


def test_worker_metrics_sampler_availability_kpis():
    sampler = WorkerMetricsSampler(max_tasks=4, disk_path='/')
    data = sampler.sample(1)
    assert data['mem_avail'] > 0
    assert 0.0 <= data['mem_avail_pct'] <= 100.0
    assert 0.0 <= data['cpu_avail_pct'] <= 100.0
    assert data['load_1m'] >= 0.0
    assert data['cpu_count'] >= 1
    assert data['max_tasks'] == 4
    assert data['slots_free'] == 3
    assert data['disk_free'] > 0

    clamped = sampler.sample(10)
    assert clamped['slots_free'] == 0


def test_worker_metrics_sampler_without_psutil(monkeypatch):
    import szrpc.server as server_mod

    monkeypatch.setattr(server_mod, 'psutil', None)
    sampler = WorkerMetricsSampler(max_tasks=2, disk_path='/')
    data = sampler.sample(1)
    assert data['cpu_pct'] == 0.0
    assert data['mem_avail'] == 0
    assert data['mem_avail_pct'] == 0.0
    assert data['cpu_avail_pct'] == 0.0
    assert data['cpu_count'] >= 1
    assert data['max_tasks'] == 2
    assert data['slots_free'] == 1
    assert 'disk_free' not in data


def test_monitor_persists_availability_kpis(tmp_path):
    db_path = tmp_path / 'avail.db'
    monitor = Monitor({'description': 'test', 'methods': []}, db_path=db_path)
    try:
        monitor.update_worker(
            'w-avail',
            metrics={
                'cpu_pct': 20.0,
                'mem_rss': 2048,
                'net_rx_bps': 0.0,
                'net_tx_bps': 0.0,
                'active_tasks': 2,
                'mem_avail': 1_000_000,
                'mem_avail_pct': 62.5,
                'cpu_avail_pct': 80.0,
                'load_1m': 1.25,
                'slots_free': 2,
            },
        )
        monitor.store.flush()
        hist = monitor.get_worker_metrics(window='15m', worker_id='w-avail')
        pt = hist['series']['w-avail'][0]
        assert pt['mem_avail'] == 1_000_000
        assert pt['mem_avail_pct'] == 62.5
        assert pt['cpu_avail_pct'] == 80.0
        assert pt['extra'] == {'load_1m': 1.25, 'slots_free': 2}
    finally:
        monitor.close()
