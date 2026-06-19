import asyncio
import argparse
import time
from datetime import datetime
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.websockets import WebSocketDisconnect
import httpx
import uvicorn
import ssl
from collections import deque
import uuid
import json

# TLS trust handling for environments with antivirus interception
try:
    import truststore
    SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
except Exception:  
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

app = FastAPI()

# Global configuration variables
RATE_LIMIT_REQUESTS = 35
RATE_LIMIT_WINDOW_SECONDS = 60
UPSTREAM_TIMEOUT_SECONDS = 500
MAX_429_RETRIES = 5
MAX_CONCURRENT_REQUESTS = 3
ENABLE_CONTEXT_PRUNING = False

INFLIGHT_REQUESTS = 0
inflight_lock = asyncio.Lock()
concurrency_semaphore = None

# Sliding window history for connection closures
completed_timestamps = deque()
completion_lock = asyncio.Lock()

def get_concurrency_semaphore():
    global concurrency_semaphore
    if concurrency_semaphore is None:
        concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return concurrency_semaphore

async def record_completion():
    """Records the exact millisecond a request closes its stream to update the window."""
    async with completion_lock:
        completed_timestamps.append(time.monotonic())
    
    async with inflight_lock:
        global INFLIGHT_REQUESTS
        INFLIGHT_REQUESTS = max(0, INFLIGHT_REQUESTS - 1)
    get_concurrency_semaphore().release()

async def wait_for_completion_window():
    """Blocks the request pipeline dynamically if the completion-based ceiling is met."""
    while True:
        now = time.monotonic()
        
        async with completion_lock:
            # Drop entries older than 60 seconds
            while completed_timestamps and completed_timestamps[0] < (now - 60.0):
                completed_timestamps.popleft()
            recent_completed = len(completed_timestamps)

        async with inflight_lock:
            current_active = INFLIGHT_REQUESTS

        total_tracked_load = recent_completed + current_active

        if total_tracked_load < RATE_LIMIT_REQUESTS:
            return  # Within safety threshold
        
        async with completion_lock:
            if completed_timestamps:
                oldest_completion = completed_timestamps[0]
                sleep_needed = max(0.1, (oldest_completion + 60.0) - now)
            else:
                sleep_needed = 1.0

        print(f"[pacing-engine] Capped by completed RPM. Delaying execution path for {sleep_needed:.2f}s...")
        await asyncio.sleep(sleep_needed)


NVIDIA_BASE_URL = "https://integrate.api.nvidia.com"
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}

request_log = deque(maxlen=100)
queue = deque()
queue_lock = asyncio.Lock()

stats = {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limited_requests": 0,
    "failed_requests": 0,
    "context_prunings": 0,
    "start_time": datetime.now().isoformat(),
}

class RequestInfo:
    def __init__(self, method: str, path: str, status: str = "pending", wait_time: float = 0):
        self.id = str(uuid.uuid4())[:8]
        self.method = method
        self.path = path
        self.status = status
        self.wait_time = wait_time
        self.timestamp = datetime.now()
        self.response_time = None
    
    def to_dict(self):
        return {
            "id": self.id, "method": self.method, "path": self.path, "status": self.status,
            "wait_time": round(self.wait_time, 2), "timestamp": self.timestamp.isoformat(), "response_time": self.response_time
        }

def get_current_rpm():
    now = datetime.now()
    completed = sum(1 for r in request_log if (now - r.timestamp).total_seconds() < 60)
    return completed + INFLIGHT_REQUESTS + len(queue)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the fully styled interactive web dashboard interface."""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NVIDIA API Rate Limit Proxy</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e0e0e0; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; color: #00ff88; font-size: 2rem; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 20px; border: 1px solid rgba(255, 255, 255, 0.1); }
        .stat-card h3 { font-size: 0.9rem; color: #888; margin-bottom: 10px; }
        .stat-card .value { font-size: 2rem; font-weight: bold; color: #00ff88; }
        .stat-card .value.warning { color: #ffaa00; }
        .stat-card .value.danger { color: #ff4444; }
        .rate-limit-info {
            background: rgba(0, 255, 136, 0.1); border: 1px solid #00ff88; border-radius: 12px;
            padding: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: center;
        }
        .rate-limit-info h2 { color: #00ff88; font-size: 1.2rem; }
        .rate-limit-info .limit { font-size: 1.5rem; font-weight: bold; }
        .section { background: rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid rgba(255, 255, 255, 0.1); }
        .section h2 { margin-bottom: 15px; color: #00ff88; font-size: 1.2rem; }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid rgba(255, 255, 255, 0.1); }
        th { color: #888; font-weight: normal; font-size: 0.9rem; }
        .status-queued { color: #00aaff; }
        .status-forwarding { color: #00ff88; }
        .status-success { color: #00ff88; }
        .status-failed { color: #ff4444; }
        .status-rate-limited { color: #ff4444; }
        .queue-item { background: rgba(0, 170, 255, 0.1); padding: 10px 15px; border-radius: 8px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
        .live-indicator { display: inline-block; width: 8px; height: 8px; background: #00ff88; border-radius: 50%; margin-right: 10px; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
    </style>
</head>
<body>
    <div class="container">
        <h1><span class="live-indicator"></span>NVIDIA API Rate Limit Proxy</h1>
        
        <div class="rate-limit-info">
            <div>
                <h2>Rate Limit Configuration</h2>
                <div class="limit" id="rateLimitDisplay">-- RPM</div>
            </div>
            <div style="text-align: center;">
                <h2>Concurrency</h2>
                <div class="limit" id="concurrencyDisplay">0 / 3</div>
            </div>
            <div style="text-align: right;">
                <h2>Current RPM</h2>
                <div class="limit" id="currentRpmDisplay">0</div>
            </div>
        </div>

        <div class="stats-grid">
             <div class="stat-card"><h3>Total Requests</h3><div class="value" id="totalRequests">0</div></div>
             <div class="stat-card"><h3>Successful</h3><div class="value" id="successfulRequests">0</div></div>
             <div class="stat-card"><h3>Rate Limited</h3><div class="value warning" id="rateLimitedRequests">0</div></div>
             <div class="stat-card"><h3>Failed</h3><div class="value danger" id="failedRequests">0</div></div>
             <div class="stat-card"><h3>Context Pruned</h3><div class="value" id="contextPrunings">0</div></div>
        </div>

        <div class="section">
            <h2>Request Queue</h2>
            <div id="queueContainer"><p style="color: #888;">No requests in queue</p></div>
        </div>

        <div class="section">
            <h2>Recent API Calls</h2>
            <table>
                <thead>
                    <tr><th>ID</th><th>Method</th><th>Path</th><th>Status</th><th>Wait Time</th><th>Timestamp</th></tr>
                </thead>
                <tbody id="requestLogTable"></tbody>
            </table>
        </div>
    </div>

    <script>
        let ws = null;
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            ws.onclose = () => { setTimeout(connectWebSocket, 2000); };
        }
        
        function updateDashboard(data) {
            document.getElementById('rateLimitDisplay').textContent = `${data.rate_limit_rpm} RPM`;
            document.getElementById('currentRpmDisplay').textContent = data.current_rpm;
            document.getElementById('concurrencyDisplay').textContent = `${data.current_inflight} / ${data.max_concurrency}`;
            document.getElementById('totalRequests').textContent = data.stats.total_requests;
            document.getElementById('successfulRequests').textContent = data.stats.successful_requests;
            document.getElementById('rateLimitedRequests').textContent = data.stats.rate_limited_requests;
            document.getElementById('failedRequests').textContent = data.stats.failed_requests;
            document.getElementById('contextPrunings').textContent = data.stats.context_prunings;
            
            const queueContainer = document.getElementById('queueContainer');
            if (data.queue.length > 0) {
                queueContainer.innerHTML = data.queue.map(item => 
                    `<div class="queue-item"><span>${item.method} ${item.path}</span><span class="status-queued">Waiting...</span></div>`
                ).join('');
            } else {
                queueContainer.innerHTML = '<p style="color: #888;">No requests in queue</p>';
            }
            
            const tableBody = document.getElementById('requestLogTable');
            tableBody.innerHTML = data.request_log.slice(-20).reverse().map(req => {
                const statusClass = `status-${req.status.toLowerCase()}`;
                return `<tr>
                    <td>#${req.id}</td><td>${req.method}</td><td>${req.path}</td>
                    <td class="${statusClass}">${req.status}</td><td>${req.wait_time}s</td>
                    <td>${new Date(req.timestamp).toLocaleTimeString()}</td>
                </tr>`;
            }).join('');
        }
        connectWebSocket();
    </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = {
                "rate_limit_rpm": RATE_LIMIT_REQUESTS, "current_rpm": get_current_rpm(),
                "upstream_timeout": UPSTREAM_TIMEOUT_SECONDS, "max_concurrency": MAX_CONCURRENT_REQUESTS,
                "current_inflight": INFLIGHT_REQUESTS, "stats": stats, "queue": [r.to_dict() for r in queue],
                "request_log": [r.to_dict() for r in request_log][-50:]
            }
            await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_handler(request: Request, path: str):
    stats["total_requests"] += 1
    req_info = RequestInfo(request.method, f"/{path}")
    print(f"[proxy] intercepted {request.method} /{path}")

    async with queue_lock:
        req_info.status = "queued"
        queue.append(req_info)

    wait_start = time.time()
    
    # Establish concurrency seat placement
    await get_concurrency_semaphore().acquire()
    
    # Apply modern dynamic completion padding
    await wait_for_completion_window()
    
    req_info.wait_time = round(time.time() - wait_start, 2)

    async with inflight_lock:
        global INFLIGHT_REQUESTS
        INFLIGHT_REQUESTS += 1
        
    async with queue_lock:
        if req_info in queue:
            queue.remove(req_info)
    req_info.status = "forwarding"

    body = await request.body()
    fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() not in ("host", "content-length")}
    fwd_headers["host"] = "integrate.api.nvidia.com"

    target_url = f"{NVIDIA_BASE_URL}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=UPSTREAM_TIMEOUT_SECONDS), verify=SSL_CONTEXT)
    request_start = time.time()
    
    try:
        async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
            for attempt in range(MAX_429_RETRIES + 1):
                upstream_req = client.build_request(method=request.method, url=target_url, headers=fwd_headers, content=body)
                upstream = await client.send(upstream_req, stream=True)
                if upstream.status_code != 429 or attempt == MAX_429_RETRIES:
                    break
                retry_after = min(2 ** attempt, 30)
                await upstream.aclose()
                req_info.status = "rate-limited"
                print(f"[proxy] NVIDIA 429 encountered; holding {retry_after:.1f}s")
                await asyncio.sleep(retry_after)
    except Exception as exc:
        await record_completion()
        await client.aclose()
        req_info.status = "failed"
        stats["failed_requests"] += 1
        request_log.append(req_info)
        return Response(content=f"Error connecting to NVIDIA backend: {exc}", status_code=502)

    if upstream.status_code >= 400:
        await record_completion()
        err_body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        req_info.status = "failed"
        stats["failed_requests"] += 1
        request_log.append(req_info)
        return Response(content=err_body, status_code=upstream.status_code)

    resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
    finalised = False

    async def relay():
        nonlocal finalised
        try:
            async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                async for chunk in upstream.aiter_raw():
                    yield chunk
            req_info.status = "success"
            stats["successful_requests"] += 1
            finalised = True
        except Exception:
            req_info.status = "failed"
            stats["failed_requests"] += 1
            finalised = True
        finally:
            request_log.append(req_info)
            await upstream.aclose()
            await client.aclose()
            await record_completion()

    return StreamingResponse(relay(), status_code=upstream.status_code, headers=resp_headers)

def main():
    parser = argparse.ArgumentParser(description="NVIDIA API Rate Limiting Proxy")
    parser.add_argument("--rpm", "-r", type=int, default=35)
    parser.add_argument("--port", "-p", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--timeout", "-t", type=int, default=500)
    parser.add_argument("--concurrency", "-c", type=int, default=3)
    parser.add_argument("--no-context-pruning", dest="context_pruning", action="store_false")
    parser.set_defaults(context_pruning=False)

    args = parser.parse_args()

    global RATE_LIMIT_REQUESTS, UPSTREAM_TIMEOUT_SECONDS, MAX_CONCURRENT_REQUESTS, ENABLE_CONTEXT_PRUNING
    RATE_LIMIT_REQUESTS = args.rpm
    UPSTREAM_TIMEOUT_SECONDS = args.timeout
    MAX_CONCURRENT_REQUESTS = args.concurrency
    ENABLE_CONTEXT_PRUNING = args.context_pruning

    print(f"\n{'='*60}")
    print(f"  NVIDIA API Completion-Based Rate Limiter")
    print(f"{'='*60}")
    print(f"  Running on:   http://{args.host}:{args.port}/")
    print(f"  Concurrency:  {MAX_CONCURRENT_REQUESTS} parallel tracks")
    print(f"  Max Goal:     {RATE_LIMIT_REQUESTS} completed requests/min")
    print(f"{'='*60}\n")
    
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()