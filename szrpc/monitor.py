import socket
import sys
import threading
import time
from collections import deque
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from . import log

logger = log.get_module_logger("server")


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
        self.signature = log.log_call(method, (), kwargs)
        self.worker_id = worker_id
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        self.status = "ACTIVE"
        self.sent_bytes = 0
        self.num_updates = 0
        self.result: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "method": self.signature,
            "worker_id": self.worker_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "status": self.status,
            "sent_bytes": self.sent_bytes,
            "updates": self.num_updates,
            "result": str(self.result) if self.result is not None else None
        }


class Monitor:
    def __init__(self, history_size: int = 100):
        self.lock = threading.Lock()
        self.active_calls: Dict[str, CallRecord] = {}
        self.historical_calls: deque[CallRecord] = deque(maxlen=history_size)
        self.host = socket.getfqdn().split('.')[0].upper()
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
                record.result = result

                if status == "UPDATE":
                    record.num_updates += 1

                if status == "ERROR":
                    self.stats["total_errors"] += 1
                elif status in ["DONE", "UPDATE"]:
                    record.sent_bytes += sys.getsizeof(result)
                    result_size = human_bytes(record.sent_bytes)
                    record.result = f'Updates: {record.num_updates}, Bytes: {result_size}'

                if status in ['DONE', 'ERROR']:
                    record.status = status
                    rec = self.active_calls.pop(request_id)
                    self.historical_calls.append(rec)

            self.workers[worker_id] = time.time()

    def update_worker(self, worker_id: str):
        with self.lock:
            self.workers[worker_id] = time.time()

    def get_data(self) -> Dict[str, Any]:
        with self.lock:
            now = time.time()
            # Clean up old workers (timeout after 10 seconds)
            active_workers = {wid: t for wid, t in self.workers.items() if now - t < 10}
            self.workers = active_workers

            return {
                "host": self.host,
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


app = FastAPI(title="Swift RPC Introspection")
monitor_instance: Optional[Monitor] = None


@app.get("/api/data")
async def get_data():
    if monitor_instance:
        return monitor_instance.get_data()
    return {"error": "Monitor not initialized"}


@app.get("/", response_class=HTMLResponse)
async def index():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <title>Swift RPC Server</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css" 
        rel="stylesheet" 
        integrity="sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB" 
        crossorigin="anonymous">
    <style>
        .status-ACTIVE { color: #2196F3; font-weight: bold; }
        .status-DONE { color: #4CAF50; font-weight: bold; }
        .status-ERROR { color: #F44336; font-weight: bold; }
        .table { --bs-table-bg: transparent !important; background-color: transparent !important;}
        td { font-family: monospace; font-size: 0.8em; }
        pre { white-space: pre-wrap; word-wrap: break-word; font-size: 0.8em; margin: 0; max-height: 100px; overflow-y: auto; }
    </style>
</head>
<body class="p-5 bg-tertiary">
    <div class="container">
    <header>
        <div class="d-flex flex-column flex-md-row align-items-center pb-3 mb-4 border-bottom">
        <a href="/" class="d-flex align-items-center text-dark text-decoration-none">
            <svg xmlns="http://www.w3.org/2000/svg" width="40" height="32" class="me-2" viewBox="0 0 118 94" role="img">
              <title>Swift RPC</title>
              <defs>
                <mask id="equilibrium-mask">
                  <rect x="0" y="0" width="118" height="94" fill="white"></rect>
                  <text x="69" y="47" dx="-3" font-size="64" font-weight="bold" font-family="system-ui, -apple-system, sans-serif" text-anchor="middle" dominant-baseline="central" fill="black">⇌</text>
                </mask>
              </defs>
              <rect x="16.5" y="9.5" width="95" height="85" rx="14" fill="currentColor" transform="translate(59 47) skewX(-15) translate(-59 -47)" mask="url(#equilibrium-mask)" fill="currentColor"></rect>
            </svg>
    <span class="fs-4">Swift RPC Server</span>&nbsp;&mdash;&nbsp;<span class='fs-4 fw-thinner text-primary' id='server_name'>localhost</span>
        </a>
        <nav class="d-inline-flex mt-2 mt-md-0 ms-md-auto">
        <a class="me-3 py-2 text-dark text-decoration-none" href="https://github.com/michel4j/swift-rpc/blob/master/README.rst">Docs</a>
        <a class="me-3 py-2 text-dark text-decoration-none" href="https://github.com/michel4j/swift-rpc">Source Code</a>
        <a class="me-3 py-2 text-dark text-decoration-none" href="https://pypi.org/project/szrpc/">Releases</a>
        <a class="py-2 text-dark text-decoration-none" href="https://github.com/michel4j/swift-rpc/issues">Issues</a>
        </nav>
        </div>
    </header>
    <div class="row row-cols-1 row-cols-md-4 mb-3 text-center" id="stats">
        <div class="col">
            <div class="card mb-4 rounded-3 shadow-sm">
                <div class="card-header py-2"><h4 class="my-0 fw-normal">Uptime</h4></div>
                <div class="card-body"><h3 class="card-title pricing-card-title" id="uptime">-</h3></div>
            </div>
        </div>
        <div class="col">
            <div class="card mb-4 rounded-3 shadow-sm">
                <div class="card-header py-2"><h4 class="my-0 fw-normal">Requests</h4></div>
                <div class="card-body"><h3 class="card-title pricing-card-title" id="total_requests">-</h3></div>
            </div>
        </div>
        <div class="col">
            <div class="card mb-4 rounded-3 shadow-sm">
                <div class="card-header py-2"><h4 class="my-0 fw-normal">Errors</h4></div>
                <div class="card-body"><h3 class="card-title pricing-card-title" id="total_errors">-</h3></div>
            </div>
        </div>
        <div class="col">
            <div class="card mb-4 rounded-3 shadow-sm">
                <div class="card-header py-2"><h4 class="my-0 fw-normal">Workers</h4></div>
                <div class="card-body"><h3 class="card-title pricing-card-title" id="active_workers">-</h3></div>
            </div>
        </div>
    </div>

    <div class="row">
        <h2 class="col-12">Call History</h2>
    </div>
    <div class="row mb-3">        
        <table class="col-12 table">
            <thead>
                <tr>
                    <th>Request ID</th>
                    <th>Method</th>
                    <th>Worker</th>
                    <th>Status</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody id="active-calls-body"></tbody>
            <tbody id="historical-calls-body"></tbody>
        </table>
    </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/js/bootstrap.bundle.min.js" 
        integrity="sha384-FKyoEForCGlyvwx9Hj09JcYn3nv7wiPVlz7YYwJrWVcXK/BmnVDxM+D2scQbITxI" 
        crossorigin="anonymous">
    </script>
    <script>
        function formatDuration(seconds) {
            // Ensure the input is a non-negative number
            seconds = Math.abs(Number(seconds));
        
            const days = Math.floor(seconds / (24 * 3600));
            seconds %= (24 * 3600);
            const hours = Math.floor(seconds / 3600);
            seconds %= 3600;
            const minutes = Math.floor(seconds / 60);
            const remainingSeconds = Math.floor(seconds % 60); // Use Math.floor for whole seconds
        
            const parts = [];
        
            if (days > 0) {
                parts.push(`${days}d`);
            }
            if (hours > 0) {
                parts.push(`${hours}h`);
            }
            if (minutes > 0) {
                parts.push(`${minutes}m`);
            }
            if (remainingSeconds > 0 || parts.length === 0) {
                parts.push(`${remainingSeconds}s`);
            }
        
            return parts.join(' ');
        }

        async function updateData() {
            try {
                const response = await fetch('/api/data');
                const data = await response.json();
                document.getElementById('server_name').innerText = data.host;
                document.getElementById('uptime').innerText = formatDuration(data.stats.uptime);
                document.getElementById('total_requests').innerText = data.stats.total_requests;
                document.getElementById('total_errors').innerText = data.stats.total_errors;
                document.getElementById('active_workers').innerText = data.stats.active_workers_count;

                const activeBody = document.getElementById('active-calls-body');
                activeBody.innerHTML = data.active_calls.map(c => `
                    <tr>
                        <td>${c.request_id}</td>
                        <td>${c.method}</td>
                        <td>${c.worker_id}</td>
                        <td><span class="badge bg-light border status-${c.status}">${c.status}</span></td>
                        <td>${formatDuration((Date.now() / 1000) - c.start_time)}</td>
                    </tr>
                `).join('');

                const historicalBody = document.getElementById('historical-calls-body');
                historicalBody.innerHTML = data.historical_calls.map(c => `
                    <tr>
                        <td>${c.request_id}</td>
                        <td>${c.method}</td>
                        <td>${c.worker_id}</td>
                        <td><span class="badge bg-light border status-${c.status}" title="${c.result}">${c.status}</span></td>
                        <td>${formatDuration(c.duration)}</td>
                    </tr>
                `).join('');
            } catch (e) {
                console.error("Failed to fetch data", e);
            }
        }

        setInterval(updateData, 1000);
        updateData();
    </script>
</body>
</html>
    """


def run_introspection_server(monitor: Monitor, host: str = "0.0.0.0", port: int = 8080):
    global monitor_instance
    monitor_instance = monitor
    logger.info(f"Starting introspection at {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")


def start_monitor_thread(monitor: Monitor, port: int = 8080) -> threading.Thread:
    thread = threading.Thread(target=run_introspection_server, args=(monitor, "0.0.0.0", port), daemon=True)
    thread.start()
    return thread
