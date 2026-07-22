from datetime import datetime, timedelta
import socket
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse

from . import log

logger = log.get_module_logger("server")

MAX_HISTORY_RECORDS = 500


def human_bytes(size: int) -> str:
    """
    Format byte size in human-readable form
    :param size: integer number of bytes
    :return: string representation of size
    """
    units = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    if size < 1000:
        return f"{size}"
    for i, unit in enumerate(units):
        size /= 1024.0
        if size < 1000:
            return f"{size:.1f}{unit}"
    return f"{size:.1f} {units[-1]}"


class CallRecord:
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
        data = {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "method": f'{self.method}({self.signature})',
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


class Monitor:
    def __init__(self, service_info: dict, history_size: int = MAX_HISTORY_RECORDS):
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

    def record_request(self, worker_id: str, client_id: str, request_id: str, method: str, kwargs: Dict[str, Any]):
        # ignore pings
        if method == 'ping':
            return

        with self.lock:
            record = CallRecord(request_id, client_id, method, kwargs, worker_id)
            self.active_calls[request_id] = record
            self.stats["total_requests"] += 1
            self.workers[worker_id] = time.time()

    def record_response(self, worker_id: str, request_id: str, status: str, result: Any):
        with self.lock:
            if request_id in self.active_calls:
                record = self.active_calls[request_id]
                record.end_time = time.time()
                record.duration = record.end_time - record.start_time
                record.result.append(result)

                if status == "UPDATE":
                    record.num_updates += 1

                if status == "ERROR":
                    self.stats["total_errors"] += 1
                elif status in ["DONE", "UPDATE"]:
                    record.sent_bytes += sys.getsizeof(result)

                if status in ['DONE', 'ERROR']:
                    record.status = status
                    rec = self.active_calls.pop(request_id)
                    self.historical_calls.append(rec)

            self.workers[worker_id] = time.time()

    def update_worker(self, worker_id: str):
        with self.lock:
            self.workers[worker_id] = time.time()

    def clear_workers(self, workers):
        """
        Remove all active tasks associated with removed workers (e.g. when they disconnect)
        :param workers: removed workers
        """
        with self.lock:
            to_remove = []
            for record_id, record in self.active_calls.items():
                worker_id = record.worker_id.encode('utf-8')
                if worker_id in workers:
                    to_remove.append(record_id)

            for record_id in to_remove:
                if record_id not in self.active_calls:
                    continue
                record = self.active_calls.pop(record_id)
                record.end_time = time.time()
                record.duration = record.end_time - record.start_time
                record.result.append('Worker lost connection')
                record.status = 'ERROR'
                self.stats["total_errors"] += 1
                to_remove.append(record_id)
                self.historical_calls.append(record)

    def get_data(self) -> Dict[str, Any]:
        with self.lock:
            now = time.time()
            # Clean up old workers (timeout after 10 seconds)
            active_workers = {wid: t for wid, t in self.workers.items() if now - t < 10}
            self.workers = active_workers

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
                "workers": list(active_workers.keys())
            }

    def get_call_details(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            if request_id in self.active_calls:
                return self.active_calls[request_id].to_dict(include_details=True)
            
            for call in self.historical_calls:
                if call.request_id == request_id:
                    return call.to_dict(include_details=True)
            return None


app = FastAPI(title="Swift RPC Introspection")
monitor_instance: Optional[Monitor] = None


@app.get("/api/data")
async def get_data():
    if monitor_instance:
        return monitor_instance.get_data()
    return {"error": "Monitor not initialized"}


@app.get("/api/details/{request_id}")
async def get_details(request_id: str):
    if monitor_instance:
        details = monitor_instance.get_call_details(request_id)
        if details:
            return details
    return {"error": "Request not found"}


@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "data" / "monitor.html")


def run_introspection_server(monitor: Monitor, host: str = "0.0.0.0", port: int = 8080):
    global monitor_instance
    monitor_instance = monitor
    logger.info(f"Starting dashboard at {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")


def start_monitor_thread(monitor: Monitor, port: int = 8080) -> threading.Thread:
    thread = threading.Thread(target=run_introspection_server, args=(monitor, "0.0.0.0", port), daemon=True)
    thread.start()
    return thread
