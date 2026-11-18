from __future__ import annotations

from collections import deque
from datetime import datetime
from pathlib import Path
import socket
import sys
import threading
import time
from typing import Any, Dict, Optional, Sequence

import msgpack
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse

from .. import log
from .store import SqliteMonitorStore, WINDOWS, _safe_json, default_db_path

logger = log.get_module_logger("server")

MAX_HISTORY_RECORDS = 500
MAX_RESULT_CHUNKS = 100
WORKER_TIMEOUT_SECONDS = 10.0


def _wire_size(value: Any) -> int:
    try:
        return len(msgpack.dumps(value))
    except Exception:
        return sys.getsizeof(value)


class CallRecord:
    """In-memory representation of an active or recently completed RPC call."""

    def __init__(self, request_id: str, client_id: str, method: str, kwargs: Dict[str, Any], worker_id: str):
        self.request_id = request_id
        self.client_id = client_id
        self.method = method
        self.kwargs = kwargs
        self.signature = ''
        self.worker_id = worker_id
        self.start_time = time.time()
        self.date_time = datetime.now().astimezone()
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        self.status = "ACTIVE"
        self.sent_bytes = 0
        self.num_updates = 0
        self.result: list[Any] = []

    def to_dict(self, include_details: bool = False) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "method": f'{self.method}({self.signature})',
            "raw_method": self.method,
            "worker_id": self.worker_id,
            "date_time": self.date_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "status": self.status,
            "sent_bytes": self.sent_bytes,
            "updates": self.num_updates,
        }
        if include_details:
            data["kwargs"] = self.kwargs
            data["results"] = self.result
        return data

    def to_store_row(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "method": self.method,
            "signature": self.signature,
            "worker_id": self.worker_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "status": self.status,
            "sent_bytes": self.sent_bytes,
            "updates": self.num_updates,
            "kwargs_json": _safe_json(self.kwargs),
            "results_json": _safe_json(self.result),
        }


class Monitor:
    """Introspection monitor recording active calls, historical calls, and worker telemetry."""

    def __init__(
        self,
        service_info: dict,
        history_size: int = MAX_HISTORY_RECORDS,
        db_path: str | Path | None = ':memory:',
    ):
        self.lock = threading.Lock()
        self.active_calls: Dict[str, CallRecord] = {}
        self.historical_calls: deque[CallRecord] = deque(maxlen=history_size)
        self.host = socket.getfqdn().split('.')[0].upper()
        self.service_info = service_info
        self.stats = {
            "total_requests": 0,
            "total_errors": 0,
            "start_time": time.time(),
        }
        self.workers: Dict[str, float] = {}
        self.worker_metrics: Dict[str, Dict[str, Any]] = {}
        self.worker_methods: Dict[str, list[str]] = {}
        self.store = SqliteMonitorStore(db_path=db_path)

    def record_request(self, worker_id: str, client_id: str, request_id: str, method: str, kwargs: Dict[str, Any]):
        if method == 'ping':
            return

        with self.lock:
            record = CallRecord(request_id, client_id, method, kwargs, worker_id)
            self.active_calls[request_id] = record
            self.stats["total_requests"] += 1
            self.workers[worker_id] = time.time()
            row = record.to_store_row()
        self.store.enqueue_call(row)

    def record_response(self, worker_id: str, request_id: str, status: str, result: Any):
        row_to_store: Dict[str, Any] | None = None
        with self.lock:
            if request_id in self.active_calls:
                record = self.active_calls[request_id]
                record.end_time = time.time()
                record.duration = record.end_time - record.start_time
                if len(record.result) >= MAX_RESULT_CHUNKS:
                    record.result.pop(0)
                record.result.append(result)

                if status == "UPDATE":
                    record.num_updates += 1

                if status == "ERROR":
                    self.stats["total_errors"] += 1
                elif status in ["DONE", "UPDATE"]:
                    record.sent_bytes += _wire_size(result)

                if status in ['DONE', 'ERROR']:
                    record.status = status
                    rec = self.active_calls.pop(request_id)
                    self.historical_calls.append(rec)
                    row_to_store = rec.to_store_row()

            self.workers[worker_id] = time.time()

        if row_to_store is not None:
            self.store.enqueue_call(row_to_store)

    def update_worker(
        self,
        worker_id: str,
        metrics: dict[str, Any] | None = None,
        methods: Sequence[str] | None = None,
    ):
        now = time.time()
        with self.lock:
            self.workers[worker_id] = now
            if metrics is not None:
                self.worker_metrics[worker_id] = dict(metrics)
            if methods is not None:
                self.worker_methods[worker_id] = list(methods)
        if metrics is not None:
            self.store.enqueue_metric({
                'ts': now,
                'worker_id': worker_id,
                'cpu_pct': float(metrics.get('cpu_pct', 0.0)),
                'mem_rss': int(metrics.get('mem_rss', 0)),
                'net_rx_bps': float(metrics.get('net_rx_bps', 0.0)),
                'net_tx_bps': float(metrics.get('net_tx_bps', 0.0)),
                'active_tasks': int(metrics.get('active_tasks', 0)),
            })

    def clear_workers(self, workers: Sequence[str]):
        """Remove all active tasks associated with disconnected workers."""
        worker_set = set(workers)
        rows_to_store: list[Dict[str, Any]] = []
        with self.lock:
            for wid in worker_set:
                self.workers.pop(wid, None)
                self.worker_metrics.pop(wid, None)
                self.worker_methods.pop(wid, None)

            to_remove = [
                record_id
                for record_id, record in self.active_calls.items()
                if record.worker_id in worker_set
            ]

            for record_id in to_remove:
                record = self.active_calls.pop(record_id, None)
                if record is None:
                    continue
                record.end_time = time.time()
                record.duration = record.end_time - record.start_time
                record.result.append('Worker lost connection')
                record.status = 'ERROR'
                self.stats["total_errors"] += 1
                self.historical_calls.append(record)
                rows_to_store.append(record.to_store_row())

        for row in rows_to_store:
            self.store.enqueue_call(row)

    def get_data(self) -> Dict[str, Any]:
        with self.lock:
            now = time.time()
            active_workers = {
                wid: t for wid, t in self.workers.items() if now - t < WORKER_TIMEOUT_SECONDS
            }
            self.workers = active_workers
            active_metrics = {
                wid: self.worker_metrics[wid]
                for wid in active_workers
                if wid in self.worker_metrics
            }
            active_methods = {
                wid: self.worker_methods[wid]
                for wid in active_workers
                if wid in self.worker_methods
            }

            return {
                **self.service_info,
                "active_calls": [c.to_dict() for c in self.active_calls.values()],
                "historical_calls": [c.to_dict() for c in reversed(self.historical_calls)],
                "stats": {
                    "total_requests": self.stats["total_requests"],
                    "total_errors": self.stats["total_errors"],
                    "uptime": now - self.stats["start_time"],
                    "active_workers_count": len(active_workers),
                },
                "workers": list(active_workers.keys()),
                "worker_metrics": active_metrics,
                "worker_methods": active_methods,
            }

    def get_call_details(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            if request_id in self.active_calls:
                return self.active_calls[request_id].to_dict(include_details=True)

            for call in self.historical_calls:
                if call.request_id == request_id:
                    return call.to_dict(include_details=True)
        return self.store.get_call_details(request_id)

    def get_summary(self, window: str = '15m') -> Dict[str, Any]:
        return self.store.get_summary(window=window)

    def get_worker_metrics(self, window: str = '15m', worker_id: str | None = None) -> Dict[str, Any]:
        return self.store.get_worker_metrics(window=window, worker_id=worker_id)

    def get_worker_tasks(
        self,
        window: str = '15m',
        worker_id: str | None = None,
        limit_per_worker: int = 120,
    ) -> Dict[str, Any]:
        return self.store.get_worker_tasks(
            window=window,
            worker_id=worker_id,
            limit_per_worker=limit_per_worker,
        )

    def get_calls(
        self,
        window: str | None = None,
        status: str | None = None,
        method: str | None = None,
        worker_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self.store.get_calls(
            window=window,
            status=status,
            method=method,
            worker_id=worker_id,
            limit=limit,
            offset=offset,
        )

    def flush(self, timeout: float = 2.0) -> None:
        self.store.flush(timeout=timeout)

    def close(self) -> None:
        self.store.close()


def create_app(monitor: Monitor) -> FastAPI:
    """Build a FastAPI dashboard app bound to a specific Monitor instance."""
    app = FastAPI(title="Swift RPC Introspection")

    @app.get("/api/data")
    async def get_data():
        return monitor.get_data()

    @app.get("/api/details/{request_id}")
    async def get_details(request_id: str):
        details = monitor.get_call_details(request_id)
        if details:
            return details
        return {"error": "Request not found"}

    @app.get("/api/summary")
    async def get_summary(window: str = Query(default="15m")):
        if window not in WINDOWS:
            window = "15m"
        return monitor.get_summary(window=window)

    @app.get("/api/metrics/workers")
    async def get_worker_metrics(
        window: str = Query(default="15m"),
        worker_id: Optional[str] = Query(default=None),
    ):
        if window not in WINDOWS:
            window = "15m"
        return monitor.get_worker_metrics(window=window, worker_id=worker_id)

    @app.get("/api/metrics/tasks")
    async def get_worker_tasks(
        window: str = Query(default="15m"),
        worker_id: Optional[str] = Query(default=None),
        limit_per_worker: int = Query(default=120, ge=1, le=500),
    ):
        if window not in WINDOWS:
            window = "15m"
        return monitor.get_worker_tasks(
            window=window,
            worker_id=worker_id,
            limit_per_worker=limit_per_worker,
        )

    @app.get("/api/calls")
    async def get_calls(
        window: Optional[str] = Query(default=None),
        status: Optional[str] = Query(default=None),
        method: Optional[str] = Query(default=None),
        worker_id: Optional[str] = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return monitor.get_calls(
            window=window,
            status=status,
            method=method,
            worker_id=worker_id,
            limit=limit,
            offset=offset,
        )

    @app.get("/")
    async def index():
        return FileResponse(Path(__file__).parent / "data" / "monitor.html")

    @app.get("/sparklines.js")
    async def sparklines_js():
        return FileResponse(
            Path(__file__).parent / "data" / "sparklines.js",
            media_type="text/javascript",
        )

    return app


def run_introspection_server(monitor: Monitor, host: str = "0.0.0.0", port: int = 8080):
    logger.info(f"Starting dashboard at {host}:{port}")
    uvicorn.run(create_app(monitor), host=host, port=port, log_level="warning")


def start_monitor_thread(monitor: Monitor, port: int = 8080) -> threading.Thread:
    thread = threading.Thread(target=run_introspection_server, args=(monitor, "0.0.0.0", port), daemon=True)
    thread.start()
    return thread


__all__ = [
    "CallRecord",
    "Monitor",
    "SqliteMonitorStore",
    "create_app",
    "default_db_path",
    "run_introspection_server",
    "start_monitor_thread",
]
