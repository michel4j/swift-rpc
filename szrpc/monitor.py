import threading
import time
from collections import deque
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from . import log

logger = log.get_module_logger("server")


class CallRecord:
    def __init__(self, request_id: str, client_id: str, method: str, kwargs: Dict[str, Any], worker_id: str):
        self.request_id = request_id
        self.client_id = client_id
        self.method = method
        self.kwargs = kwargs
        self.worker_id = worker_id
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        self.status = "ACTIVE"
        self.result: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "method": self.method,
            "kwargs": self.kwargs,
            "worker_id": self.worker_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "status": self.status,
            "result": str(self.result) if self.result is not None else None
        }


class Monitor:
    def __init__(self, history_size: int = 100):
        self.lock = threading.Lock()
        self.active_calls: Dict[str, CallRecord] = {}
        self.historical_calls: deque[CallRecord] = deque(maxlen=history_size)
        self.stats = {
            "total_requests": 0,
            "total_errors": 0,
            "start_time": time.time(),
        }
        self.workers: Dict[str, float] = {}

    def record_request(self, worker_id: str, client_id: str, request_id: str, method: str, kwargs: Dict[str, Any]):
        with self.lock:
            record = CallRecord(request_id, client_id, method, kwargs, worker_id)
            self.active_calls[request_id] = record
            self.stats["total_requests"] += 1
            self.workers[worker_id] = time.time()

    def record_response(self, worker_id: str, request_id: str, status: str, result: Any):
        with self.lock:
            if request_id in self.active_calls:
                record = self.active_calls.pop(request_id)
                record.end_time = time.time()
                record.duration = record.end_time - record.start_time
                record.status = status
                record.result = result
                if status == "ERROR":
                    self.stats["total_errors"] += 1
                self.historical_calls.append(record)
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


app = FastAPI(title="szrpc Introspection")
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
        .stats { display: flex; gap: 20px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); flex: 1; text-align: center; }
        .stat-card h3 { margin: 0; font-size: 0.9em; color: #666;}
        .stat-card p { margin: 10px 0 0; font-size: 1.5em; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background-color: #6200ee; color: white; }
        tr:hover { background-color: #f1f1f1; }
        .status-ACTIVE { color: #2196F3; font-weight: bold; }
        .status-DONE { color: #4CAF50; font-weight: bold; }
        .status-ERROR { color: #F44336; font-weight: bold; }
        pre { white-space: pre-wrap; word-wrap: break-word; font-size: 0.8em; margin: 0; max-height: 100px; overflow-y: auto; }
    </style>
</head>
<body class="p-5">
    <h1>Swift RPC Server Introspection</h1>
    
    <div class="stats" id="stats">
        <div class="stat-card"><h3>Uptime</h3><p id="uptime">-</p></div>
        <div class="stat-card"><h3>Total Requests</h3><p id="total_requests">-</p></div>
        <div class="stat-card"><h3>Errors</h3><p id="total_errors">-</p></div>
        <div class="stat-card"><h3>Active Workers</h3><p id="active_workers">-</p></div>
    </div>

    <h2>Active Calls</h2>
    <table>
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
    </table>

    <h2>Historical Calls</h2>
    <table>
        <thead>
            <tr>
                <th>Request ID</th>
                <th>Method</th>
                <th>Worker</th>
                <th>Status</th>
                <th>Duration</th>
                <th>Result/Error</th>
            </tr>
        </thead>
        <tbody id="historical-calls-body"></tbody>
    </table>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/js/bootstrap.bundle.min.js" 
        integrity="sha384-FKyoEForCGlyvwx9Hj09JcYn3nv7wiPVlz7YYwJrWVcXK/BmnVDxM+D2scQbITxI" 
        crossorigin="anonymous"></script>
    <script>
    
        function formatDuration(totalSeconds) {
            const days = Math.floor(totalSeconds / 86400);
            totalSeconds -= days * 86400;
        
            const hours = Math.floor(totalSeconds / 3600);
            totalSeconds -= hours * 3600;
        
            const minutes = Math.floor(totalSeconds / 60);
            totalSeconds -= minutes * 60;
        
            const seconds = Math.floor(totalSeconds);
        
            // Use padStart to ensure two digits for HH, MM, SS
            const pad = (num) => num.toString().padStart(2, '0');
        
            return `${days}:${pad(hours)}:${pad(minutes)}:${pad(seconds)}`;
        }

        async function updateData() {
            try {
                const response = await fetch('/api/data');
                const data = await response.json();
                
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
                        <td class="status-${c.status}">${c.status}</td>
                        <td>${formatDuration((Date.now() / 1000) - c.start_time)}</td>
                    </tr>
                `).join('');

                const historicalBody = document.getElementById('historical-calls-body');
                historicalBody.innerHTML = data.historical_calls.map(c => `
                    <tr>
                        <td>${c.request_id}</td>
                        <td>${c.method}</td>
                        <td>${c.worker_id}</td>
                        <td class="status-${c.status}">${c.status}</td>
                        <td>${formatDuration(c.duration)}</td>
                        <td><pre>${c.result}</pre></td>
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
