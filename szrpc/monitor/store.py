from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from peewee import (
    Case,
    CharField,
    FloatField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
    fn,
)

# window -> (duration_seconds, bucket_seconds)
WINDOWS: dict[str, tuple[int, int]] = {
    '15m': (900, 5),
    '1h': (3600, 15),
    '12h': (43200, 180),
    '1d': (86400, 360),
    '7d': (604800, 2400),
}

MAX_JSON_BYTES = 65536


def default_db_path() -> Path:
    """Return the default SQLite database path for monitor persistence."""
    base = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local' / 'share'))
    return base / 'szrpc' / 'monitor.db'


def _safe_json(value: Any, max_bytes: int = MAX_JSON_BYTES) -> str:
    """Serialize value to JSON, truncating oversized payloads safely."""
    try:
        raw = json.dumps(value, default=str)
    except Exception:
        raw = json.dumps(str(value))
    if len(raw) > max_bytes:
        return json.dumps({'_truncated': True, 'preview': raw[: max_bytes - 64]})
    return raw


def _make_models(db: SqliteDatabase):
    """Create isolated Peewee model classes bound to a specific SqliteDatabase instance."""

    class BaseModel(Model):
        class Meta:
            database = db

    class CallRecordModel(BaseModel):
        request_id = CharField(primary_key=True)
        client_id = CharField()
        method = CharField(index=True)
        signature = CharField(default='')
        worker_id = CharField(index=True)
        start_time = FloatField(index=True)
        end_time = FloatField(null=True)
        duration = FloatField(null=True)
        status = CharField(index=True)
        sent_bytes = IntegerField(default=0)
        updates = IntegerField(default=0)
        kwargs_json = TextField(default='{}')
        results_json = TextField(default='[]')

        class Meta:
            table_name = 'call_records'

    class WorkerMetricModel(BaseModel):
        ts = FloatField(index=True)
        worker_id = CharField(index=True)
        cpu_pct = FloatField(default=0.0)
        mem_rss = IntegerField(default=0)
        net_rx_bps = FloatField(default=0.0)
        net_tx_bps = FloatField(default=0.0)
        active_tasks = IntegerField(default=0)

        class Meta:
            table_name = 'worker_metrics'
            indexes = ((('worker_id', 'ts'), False),)

    return CallRecordModel, WorkerMetricModel


class SqliteMonitorStore:
    """Non-blocking SQLite/Peewee persistence store for monitor calls and worker telemetry."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        retention_seconds: float = 7 * 86400,
        flush_interval: float = 0.5,
        batch_size: int = 100,
    ):
        self.retention_seconds = retention_seconds
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self._is_memory = str(db_path) == ':memory:'

        if self._is_memory:
            self._db = SqliteDatabase(':memory:', check_same_thread=False)
            self.db_path = ':memory:'
        else:
            path = Path(db_path) if db_path is not None else default_db_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = SqliteDatabase(
                str(path),
                pragmas={
                    'journal_mode': 'wal',
                    'synchronous': 'normal',
                    'cache_size': -16000,
                },
                check_same_thread=False,
            )
            self.db_path = str(path)

        self._db.connect(reuse_if_open=True)
        self.CallRecord, self.WorkerMetric = _make_models(self._db)
        self._db.create_tables([self.CallRecord, self.WorkerMetric], safe=True)

        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=10000)
        self._stop_event = threading.Event()
        self._last_prune = time.time()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

    def enqueue_call(self, row: dict[str, Any]) -> None:
        """Queue a call record upsert without blocking the caller."""
        try:
            self._queue.put_nowait(('call', row))
        except queue.Full:
            pass

    def enqueue_metric(self, row: dict[str, Any]) -> None:
        """Queue a worker metric sample without blocking the caller."""
        try:
            self._queue.put_nowait(('metric', row))
        except queue.Full:
            pass

    def flush(self, timeout: float = 2.0) -> None:
        """Wait until all currently queued writes are committed to SQLite."""
        if self._stop_event.is_set():
            return
        done = threading.Event()
        self._queue.put(('flush', done))
        done.wait(timeout=timeout)

    def close(self) -> None:
        """Flush pending writes, stop the writer thread, and close the database."""
        if self._stop_event.is_set():
            return
        self.flush(timeout=2.0)
        self._stop_event.set()
        self._queue.put(('stop', None))
        if self._writer_thread.is_alive():
            self._writer_thread.join(timeout=2.0)
        if not self._db.is_closed():
            self._db.close()

    def _flush_batch(self, calls: list[dict[str, Any]], metrics: list[dict[str, Any]]) -> None:
        if not calls and not metrics:
            return
        # Deduplicate call upserts in the same batch by request_id (keep latest)
        deduped_calls: dict[str, dict[str, Any]] = {}
        for row in calls:
            deduped_calls[row['request_id']] = row

        with self._db.atomic():
            if deduped_calls:
                self.CallRecord.insert_many(list(deduped_calls.values())).on_conflict_replace().execute()
            if metrics:
                self.WorkerMetric.insert_many(metrics).execute()

    def _prune_old(self) -> None:
        if self.retention_seconds <= 0:
            return
        cutoff = time.time() - self.retention_seconds
        with self._db.atomic():
            self.WorkerMetric.delete().where(self.WorkerMetric.ts < cutoff).execute()
            self.CallRecord.delete().where(self.CallRecord.start_time < cutoff).execute()

    def _writer_loop(self) -> None:
        while not self._stop_event.is_set():
            calls: list[dict[str, Any]] = []
            metrics: list[dict[str, Any]] = []
            flush_events: list[threading.Event] = []
            should_stop = False

            try:
                kind, payload = self._queue.get(timeout=self.flush_interval)
                items = [(kind, payload)]
            except queue.Empty:
                items = []

            while len(items) < self.batch_size:
                try:
                    items.append(self._queue.get_nowait())
                except queue.Empty:
                    break

            for kind, payload in items:
                if kind == 'call':
                    calls.append(payload)
                elif kind == 'metric':
                    metrics.append(payload)
                elif kind == 'flush':
                    flush_events.append(payload)
                elif kind == 'stop':
                    should_stop = True

            if calls or metrics:
                try:
                    self._flush_batch(calls, metrics)
                except Exception:
                    pass

            now = time.time()
            if now - self._last_prune >= 60.0:
                try:
                    self._prune_old()
                except Exception:
                    pass
                self._last_prune = now

            for ev in flush_events:
                ev.set()

            if should_stop:
                break

    def get_worker_metrics(self, window: str = '15m', worker_id: str | None = None) -> dict[str, Any]:
        """Return time-bucketed CPU, memory, network, and active task series per worker."""
        duration, bucket = WINDOWS.get(window, WINDOWS['15m'])
        cutoff = time.time() - duration
        bucket_expr = ((self.WorkerMetric.ts / bucket).cast('INTEGER') * bucket).alias('bucket')

        query = (
            self.WorkerMetric.select(
                bucket_expr,
                self.WorkerMetric.worker_id,
                fn.AVG(self.WorkerMetric.cpu_pct).alias('cpu_pct'),
                fn.AVG(self.WorkerMetric.mem_rss).alias('mem_rss'),
                fn.AVG(self.WorkerMetric.net_rx_bps).alias('net_rx_bps'),
                fn.AVG(self.WorkerMetric.net_tx_bps).alias('net_tx_bps'),
                fn.MAX(self.WorkerMetric.active_tasks).alias('active_tasks'),
            )
            .where(self.WorkerMetric.ts >= cutoff)
        )
        if worker_id:
            query = query.where(self.WorkerMetric.worker_id == worker_id)
        query = query.group_by(self.WorkerMetric.worker_id, bucket_expr).order_by(bucket_expr.asc())

        series: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in query.dicts():
            series[row['worker_id']].append({
                'ts': row['bucket'],
                'cpu_pct': round(float(row['cpu_pct'] or 0.0), 2),
                'mem_rss': int(row['mem_rss'] or 0),
                'net_rx_bps': round(float(row['net_rx_bps'] or 0.0), 1),
                'net_tx_bps': round(float(row['net_tx_bps'] or 0.0), 1),
                'active_tasks': int(row['active_tasks'] or 0),
            })

        return {
            'window': window if window in WINDOWS else '15m',
            'bucket_seconds': bucket,
            'series': dict(series),
        }

    def get_summary(self, window: str = '15m') -> dict[str, Any]:
        """Return per-method counts, error counts, and duration statistics for the window."""
        duration, _ = WINDOWS.get(window, WINDOWS['15m'])
        cutoff = time.time() - duration

        agg_query = (
            self.CallRecord.select(
                self.CallRecord.method,
                fn.COUNT(self.CallRecord.request_id).alias('count'),
                fn.SUM(Case(None, ((self.CallRecord.status == 'ERROR', 1),), 0)).alias('errors'),
                fn.AVG(self.CallRecord.duration).alias('avg_duration'),
                fn.MAX(self.CallRecord.duration).alias('max_duration'),
                fn.MIN(self.CallRecord.duration).alias('min_duration'),
                fn.SUM(self.CallRecord.sent_bytes).alias('total_bytes'),
            )
            .where(self.CallRecord.start_time >= cutoff)
            .group_by(self.CallRecord.method)
            .order_by(fn.COUNT(self.CallRecord.request_id).desc())
        )

        # Gather sorted durations per method for p95 calculation
        durations_by_method: dict[str, list[float]] = defaultdict(list)
        dur_query = (
            self.CallRecord.select(self.CallRecord.method, self.CallRecord.duration)
            .where((self.CallRecord.start_time >= cutoff) & (self.CallRecord.duration.is_null(False)))
            .order_by(self.CallRecord.duration.asc())
        )
        for row in dur_query.dicts():
            durations_by_method[row['method']].append(float(row['duration']))

        methods: list[dict[str, Any]] = []
        total_requests = 0
        total_errors = 0
        for row in agg_query.dicts():
            m = row['method']
            cnt = int(row['count'] or 0)
            errs = int(row['errors'] or 0)
            total_requests += cnt
            total_errors += errs
            durs = durations_by_method.get(m, [])
            p95 = durs[min(len(durs) - 1, int(0.95 * len(durs)))] if durs else 0.0
            methods.append({
                'method': m,
                'count': cnt,
                'errors': errs,
                'avg_duration': round(float(row['avg_duration'] or 0.0), 4),
                'p95_duration': round(float(p95), 4),
                'max_duration': round(float(row['max_duration'] or 0.0), 4),
                'min_duration': round(float(row['min_duration'] or 0.0), 4),
                'total_bytes': int(row['total_bytes'] or 0),
            })

        return {
            'window': window if window in WINDOWS else '15m',
            'total_requests': total_requests,
            'total_errors': total_errors,
            'methods': methods,
        }

    def get_calls(
        self,
        window: str | None = None,
        status: str | None = None,
        method: str | None = None,
        worker_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Return paginated and filtered call records ordered newest-first."""
        query = self.CallRecord.select()
        if window and window in WINDOWS:
            cutoff = time.time() - WINDOWS[window][0]
            query = query.where(self.CallRecord.start_time >= cutoff)
        if status:
            query = query.where(self.CallRecord.status == status)
        if method:
            query = query.where(self.CallRecord.method == method)
        if worker_id:
            query = query.where(self.CallRecord.worker_id == worker_id)

        total = query.count()
        rows = query.order_by(self.CallRecord.start_time.desc()).limit(limit).offset(offset)

        calls = []
        for rec in rows:
            dt = datetime.fromtimestamp(rec.start_time).astimezone()
            calls.append({
                'request_id': rec.request_id,
                'client_id': rec.client_id,
                'method': f'{rec.method}({rec.signature})',
                'raw_method': rec.method,
                'worker_id': rec.worker_id,
                'date_time': dt.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'start_time': rec.start_time,
                'end_time': rec.end_time,
                'duration': rec.duration,
                'status': rec.status,
                'sent_bytes': rec.sent_bytes,
                'updates': rec.updates,
            })

        return {'total': total, 'limit': limit, 'offset': offset, 'calls': calls}

    def get_call_details(self, request_id: str) -> dict[str, Any] | None:
        """Fetch full call details including kwargs and results from SQLite."""
        rec = self.CallRecord.get_or_none(self.CallRecord.request_id == request_id)
        if rec is None:
            return None
        dt = datetime.fromtimestamp(rec.start_time).astimezone()
        return {
            'request_id': rec.request_id,
            'client_id': rec.client_id,
            'method': f'{rec.method}({rec.signature})',
            'worker_id': rec.worker_id,
            'date_time': dt.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'start_time': rec.start_time,
            'end_time': rec.end_time,
            'duration': rec.duration,
            'status': rec.status,
            'sent_bytes': rec.sent_bytes,
            'updates': rec.updates,
            'kwargs': json.loads(rec.kwargs_json),
            'results': json.loads(rec.results_json),
        }
