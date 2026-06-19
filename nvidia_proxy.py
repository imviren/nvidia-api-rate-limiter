import asyncio
import argparse
import time
from datetime import datetime
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.websockets import WebSocketDisconnect
from starlette.requests import ClientDisconnect
import httpx
import uvicorn
import ssl
from collections import deque
import uuid
import json
import os
import sys

# TLS trust handling for environments running antivirus certificate interception
try:
    import truststore
    SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
except Exception:  
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

app = FastAPI()

# Global configuration variables managed via command-line arguments
RATE_LIMIT_REQUESTS = 40
UPSTREAM_TIMEOUT_SECONDS = 500
MAX_429_RETRIES = 0  # No retries — if we hit 429 despite pacing, fail immediately
MAX_CONCURRENT_REQUESTS = 1
MAX_CONTEXT_TOKENS = 160000
KEEP_LAST_MESSAGES = 30
ENABLE_CONTEXT_PRUNING = True
MAX_RETRIES_NETWORK_ERRORS = 2

INFLIGHT_REQUESTS = 0
inflight_lock = asyncio.Lock()
concurrency_semaphore = None
pruning_lock = asyncio.Lock()

# --- AIR-TIGHT PACING ENGINE STATE ---
SEQUENTIAL_LOCK = asyncio.Lock()
last_completion_time = 0.0  
last_429_time = 0.0  # When we last got a 429
COOLDOWN_BUFFER = 1.67  

def get_concurrency_semaphore():
    global concurrency_semaphore
    if concurrency_semaphore is None:
        concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return concurrency_semaphore

async def release_slot_and_decrement():
    async with inflight_lock:
        global INFLIGHT_REQUESTS
        INFLIGHT_REQUESTS = max(0, INFLIGHT_REQUESTS - 1)
    get_concurrency_semaphore().release()

async def wait_for_holdout():
    """
    Wait for holdout period BEFORE starting. Does NOT set the completion
    timestamp — that must be done by the caller when the request finishes.
    Returns the actual holdout gap in seconds.
    """
    global last_completion_time, last_429_time
    
    # If we got a 429 recently, extend holdout to back off
    now = time.monotonic()
    since_429 = now - last_429_time
    if since_429 < 10.0:
        extra_wait = 10.0 - since_429
        print(f"[pacing-engine] Rate-limited recently, extending holdout by {extra_wait:.2f}s")
        await asyncio.sleep(extra_wait)
    
    now = time.monotonic()
    elapsed = now - last_completion_time
    
    if elapsed < COOLDOWN_BUFFER:
        wait_needed = COOLDOWN_BUFFER - elapsed
        print(f"[pacing-engine] Strict holdout active. Waiting {wait_needed:.2f}s...")
        await asyncio.sleep(wait_needed)
        return wait_needed
    
    return 0.0


# --- SLIDING-WINDOW RPM ENFORCEMENT ---
rate_limit_window = deque()  # monotonic timestamps of forwarded request starts
rate_limit_lock = asyncio.Lock()

async def enforce_rate_limit():
    """Ensures we never exceed RATE_LIMIT_REQUESTS forwarded starts in any rolling 60s window."""
    while True:
        async with rate_limit_lock:
            now = time.monotonic()
            while rate_limit_window and now - rate_limit_window[0] > 60:
                rate_limit_window.popleft()
            if len(rate_limit_window) < RATE_LIMIT_REQUESTS:
                return
            wait = rate_limit_window[0] + 60 - now
        print(f"[rate-limit] Window full ({len(rate_limit_window)}/{RATE_LIMIT_REQUESTS} RPM). Waiting {wait:.2f}s...")
        await asyncio.sleep(min(wait, 1.0))

async def record_rate_limit_usage():
    """Records a request in the sliding window after successful forwarding."""
    async with rate_limit_lock:
        rate_limit_window.append(time.monotonic())

def record_rate_limit_usage_sync():
    """Synchronous version for use in finally blocks."""
    rate_limit_window.append(time.monotonic())


# --- SPEC-SAFE CONTEXT PRUNING LOGIC ---
def estimate_tokens(text) -> int:
    if not text:
        return 0
    if isinstance(text, list):
        return sum(estimate_tokens(block.get("text", "")) for block in text if isinstance(block, dict))
    return max(1, len(text) // 4)

def estimate_messages_tokens(messages: list) -> int:
    total = 0
    for msg in messages:
        total += estimate_tokens(msg.get("role", ""))
        total += estimate_tokens(msg.get("content", ""))
        total += 3
    return total

def safe_prune_chat_messages(messages: list, max_tokens: int, keep_last: int) -> list:
    if not messages or estimate_messages_tokens(messages) <= max_tokens:
        return messages
    
    system_messages = [msg for msg in messages if msg.get("role") == "system"]
    other_messages = [msg for msg in messages if msg.get("role") != "system"]
    
    if len(other_messages) <= keep_last:
        return messages
        
    sliced_messages = other_messages[-keep_last:]
    
    while sliced_messages and sliced_messages[0].get("role") == "tool":
        current_idx_in_original = len(other_messages) - len(sliced_messages)
        if current_idx_in_original > 0:
            sliced_messages.insert(0, other_messages[current_idx_in_original - 1])
        else:
            break
            
    final_messages = system_messages + sliced_messages
    
    if estimate_messages_tokens(final_messages) > max_tokens and keep_last > 2:
        return safe_prune_chat_messages(messages, max_tokens, keep_last - 2)
        
    return final_messages

async def maybe_prune_chat_context(body: bytes, path: str) -> bytes:
    if not ENABLE_CONTEXT_PRUNING or "chat/completions" not in path:
        return body
    try:
        if not body:
            return body
        data = json.loads(body.decode("utf-8"))
        if "messages" not in data or not isinstance(data["messages"], list):
            return body
            
        original_msgs = data["messages"]
        orig_tokens = estimate_messages_tokens(original_msgs)
        
        if orig_tokens <= max(MAX_CONTEXT_TOKENS, 8000):
            return body
            
        pruned_msgs = safe_prune_chat_messages(original_msgs, MAX_CONTEXT_TOKENS, KEEP_LAST_MESSAGES)
        if len(pruned_msgs) == len(original_msgs):
            return body
            
        data["messages"] = pruned_msgs
        new_tokens = estimate_messages_tokens(pruned_msgs)
        print(f"[context-pruning] Reduced sliding token footprint from {orig_tokens} to {new_tokens} ({len(original_msgs)} -> {len(pruned_msgs)} messages)")
        stats["context_prunings"] += 1
        return json.dumps(data).encode("utf-8")
    except Exception as e:
        print(f"[context-pruning] Soft warning - skipped evaluation: {e}")
        return body


NVIDIA_BASE_URL = "https://integrate.api.nvidia.com"
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}

shared_client = None

async def get_shared_client():
    global shared_client
    if shared_client is None:
        shared_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, read=UPSTREAM_TIMEOUT_SECONDS),
            verify=SSL_CONTEXT,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)
        )
    return shared_client

request_log = deque(maxlen=100)
queue = deque()
queue_lock = asyncio.Lock()

stats = {
    "total_requests": 0, "successful_requests": 0, "rate_limited_requests": 0, "failed_requests": 0, "context_prunings": 0, "network_retries": 0,
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
        self.request_sent_time = None
        self.request_complete_time = None
        self.holdout_waited = 0.0
        self.gap_from_previous = 0.0
        self.holdout_compliant = False
    
    def to_dict(self):
        return {
            "id": self.id, "method": self.method, "path": self.path, "status": self.status,
            "wait_time": round(self.wait_time, 2), "timestamp": self.timestamp.isoformat(), 
            "response_time": self.response_time, "request_sent_time": self.request_sent_time,
            "request_complete_time": self.request_complete_time,
            "holdout_waited": round(self.holdout_waited, 2),
            "gap_from_previous": round(self.gap_from_previous, 2),
            "holdout_compliant": self.holdout_compliant
        }

def get_current_rpm():
    now = datetime.now()
    completed = sum(1 for r in request_log if (now - r.timestamp).total_seconds() < 60)
    return completed + INFLIGHT_REQUESTS + len(queue)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>NVIDIA Proxy — Pacing Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 100%); color: #e0e0e0; padding: 20px; }
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; color: #00ff88; font-weight: 300; letter-spacing: 2px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .stat-card { background: rgba(255,255,255,0.04); border-radius: 12px; padding: 16px; border: 1px solid rgba(255,255,255,0.06); }
        .stat-card h3 { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
        .stat-card .value { font-size: 1.6rem; font-weight: 600; }
        .rate-limit-info { background: rgba(0,255,136,0.06); border: 1px solid #00ff8844; border-radius: 12px; padding: 16px 24px; margin-bottom: 24px; display: flex; gap: 40px; flex-wrap: wrap; }
        .rate-limit-info > div { min-width: 120px; }
        .rate-limit-info h3 { font-size: 0.7rem; color: #888; text-transform: uppercase; letter-spacing: 1px; }
        .rate-limit-info .limit { font-size: 1.3rem; font-weight: 600; color: #00ff88; }
        .section { background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 16px; border: 1px solid rgba(255,255,255,0.05); }
        .section h2 { font-size: 0.95rem; color: #aaa; margin-bottom: 12px; font-weight: 400; letter-spacing: 1px; }
        table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
        th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,0.06); white-space: nowrap; }
        th { color: #666; font-weight: 500; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; }
        .status-queued { color: #00aaff; } .status-forwarding { color: #00ff88; } .status-success { color: #00ff88; } .status-failed { color: #ff4444; } .status-rate-limited { color: #ff8800; } .status-disconnected { color: #ffaa00; }
        .badge-ok { display: inline-block; background: #00ff8822; color: #00ff88; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
        .badge-fail { display: inline-block; background: #ff444422; color: #ff4444; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
        .mono { font-family: 'Consolas', monospace; font-size: 0.75rem; }
    </style>
</head>
<body>
    <div class="container">
        <h1>NVIDIA API — Pacing Monitor</h1>
        <div class="rate-limit-info">
            <div><h3>Rate Limit</h3><div class="limit" id="rateLimitDisplay">40 RPM</div></div>
            <div><h3>Cooldown</h3><div class="limit" id="cooldownDisplay">1.67s</div></div>
            <div><h3>Active</h3><div class="limit" id="concurrencyDisplay">0 / 1</div></div>
            <div><h3>Window RPM</h3><div class="limit" id="currentRpmDisplay">0</div></div>
        </div>
        <div class="stats-grid">
            <div class="stat-card"><h3>Total</h3><div class="value" id="totalRequests">0</div></div>
            <div class="stat-card"><h3>Success</h3><div class="value" style="color:#00ff88;" id="successfulRequests">0</div></div>
            <div class="stat-card"><h3>Failed</h3><div class="value" style="color:#ff4444;" id="failedRequests">0</div></div>
            <div class="stat-card"><h3>Rate-Limited</h3><div class="value" style="color:#ff8800;" id="rateLimitedRequests">0</div></div>
            <div class="stat-card"><h3>Prunings</h3><div class="value" style="color:#00aaff;" id="contextPrunings">0</div></div>
            <div class="stat-card"><h3>Holdout OK</h3><div class="value" style="color:#00ff88;" id="holdoutCompliantCount">0</div></div>
        </div>
        <div class="section"><h2>Queue</h2><div id="queueContainer"><p style="color:#555; font-size:0.85rem;">Idle</p></div></div>
        <div class="section">
            <h2>Request Log</h2>
            <div style="overflow-x:auto;">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th><th>Method</th><th>Path</th><th>Status</th>
                            <th>Sent</th><th>Completed</th><th>Duration</th>
                            <th>Holdout Waited</th><th>Gap Prev→Sent</th><th>OK</th>
                        </tr>
                    </thead>
                    <tbody id="requestLogTable"></tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
        let ws = null;
        let holdoutOk = 0;
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            ws.onmessage = (event) => {
                const d = JSON.parse(event.data);
                document.getElementById('rateLimitDisplay').textContent = d.rate_limit_rpm + ' RPM';
                document.getElementById('cooldownDisplay').textContent = d.cooldown_buffer + 's';
                document.getElementById('concurrencyDisplay').textContent = d.current_inflight + ' / ' + d.max_concurrency;
                document.getElementById('currentRpmDisplay').textContent = d.current_rpm;
                document.getElementById('totalRequests').textContent = d.stats.total_requests;
                document.getElementById('successfulRequests').textContent = d.stats.successful_requests;
                document.getElementById('failedRequests').textContent = d.stats.failed_requests;
                document.getElementById('rateLimitedRequests').textContent = d.stats.rate_limited_requests;
                document.getElementById('contextPrunings').textContent = d.stats.context_prunings;
                holdoutOk = d.request_log.filter(r => r.holdout_compliant).length;
                document.getElementById('holdoutCompliantCount').textContent = holdoutOk;
                const q = document.getElementById('queueContainer');
                q.innerHTML = d.queue.length > 0
                    ? d.queue.map(i => '<div style="padding:4px 0;font-size:0.85rem;">' + i.method + ' ' + i.path + ' <span class="status-queued">Queued</span> <span class="mono">(' + i.wait_time + 's)</span></div>').join('')
                    : '<p style="color:#555;font-size:0.85rem;">Idle</p>';
                const t = document.getElementById('requestLogTable');
                t.innerHTML = d.request_log.slice(-30).reverse().map(r => {
                    const st = new Date(r.timestamp).toLocaleTimeString();
                    const sent = r.request_sent_time ? new Date(r.request_sent_time).toLocaleTimeString() : '-';
                    const comp = r.request_complete_time ? new Date(r.request_complete_time).toLocaleTimeString() : '-';
                    const dur = r.response_time ? r.response_time.toFixed(2) + 's' : '-';
                    const hw = (r.holdout_waited || 0).toFixed(2) + 's';
                    const gap = (r.gap_from_previous || 0).toFixed(2) + 's';
                    const ok = r.holdout_compliant ? '<span class="badge-ok">OK</span>' : '<span class="badge-fail">FAIL</span>';
                    return '<tr><td class="mono">#' + r.id + '</td><td>' + r.method + '</td><td>' + r.path + '</td><td class="status-' + r.status.toLowerCase() + '">' + r.status + '</td><td class="mono">' + sent +
                        '</td><td class="mono">' + comp + '</td><td class="mono">' + dur + '</td><td class="mono">' + hw + '</td><td class="mono">' + gap + '</td><td>' + ok + '</td></tr>';
                }).join('');
            };
            ws.onclose = () => { setTimeout(connectWebSocket, 2000); };
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
                "rate_limit_rpm": RATE_LIMIT_REQUESTS, "cooldown_buffer": round(COOLDOWN_BUFFER, 2), "current_rpm": get_current_rpm(),
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
    global last_completion_time, last_429_time
    if request.method == "OPTIONS":
        return Response(status_code=204)
    stats["total_requests"] += 1
    req_info = RequestInfo(request.method, f"/{path}")

    async with queue_lock:
        req_info.status = "queued"
        queue.append(req_info)

    wait_start = time.time()
    
    async with SEQUENTIAL_LOCK:
        now_before_holdout = time.monotonic()
        gap_from_previous = now_before_holdout - last_completion_time
        holdout_waited = await wait_for_holdout()
        req_info.holdout_waited = holdout_waited
        req_info.gap_from_previous = gap_from_previous
        req_info.holdout_compliant = gap_from_previous >= COOLDOWN_BUFFER
        
        await enforce_rate_limit()
        await get_concurrency_semaphore().acquire()
        
        req_info.wait_time = round(time.time() - wait_start, 2)

        async with inflight_lock:
            global INFLIGHT_REQUESTS
            INFLIGHT_REQUESTS += 1
            
        async with queue_lock:
            if req_info in queue:
                queue.remove(req_info)
        req_info.status = "forwarding"

        try:
            body = await request.body()
            body = await maybe_prune_chat_context(body, f"/{path}")
        except ClientDisconnect:
            print(f"[proxy] Client disconnected while queued.")
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "Disconnected"
            stats["failed_requests"] += 1
            request_log.append(req_info)
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            return Response(status_code=244, content="Client disconnected from proxy channel early.")

        fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() not in ("host",)}
        fwd_headers["host"] = "integrate.api.nvidia.com"
        if body:
            fwd_headers["content-length"] = str(len(body))

        target_url = f"{NVIDIA_BASE_URL}/{path}"
        if request.url.query:
            target_url += f"?{request.url.query}"

        client = await get_shared_client()
        
        try:
            async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                upstream_req = client.build_request(method=request.method, url=target_url, headers=fwd_headers, content=body)
                req_info.request_sent_time = datetime.now().isoformat()
                upstream = await client.send(upstream_req, stream=True)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.NetworkError, ssl.SSLError) as exc:
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "failed"
            stats["failed_requests"] += 1
            request_log.append(req_info)
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            print(f"[proxy] Network error: {type(exc).__name__}: {exc}")
            return Response(content=f"NVIDIA connection failed: {type(exc).__name__}", status_code=502)
        except asyncio.TimeoutError:
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "failed"
            stats["failed_requests"] += 1
            request_log.append(req_info)
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            print(f"[proxy] Upstream timeout after {UPSTREAM_TIMEOUT_SECONDS}s")
            return Response(content="NVIDIA upstream timeout", status_code=504)
        except Exception as exc:
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "failed"
            stats["failed_requests"] += 1
            request_log.append(req_info)
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            print(f"[proxy] Upstream error: {type(exc).__name__}: {exc}")
            return Response(content=f"NVIDIA upstream error: {type(exc).__name__}", status_code=502)

        # --- 429: FAIL IMMEDIATELY, DON'T RETRY ---
        if upstream.status_code == 429:
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "rate-limited"
            stats["rate_limited_requests"] += 1
            request_log.append(req_info)
            last_429_time = time.monotonic()
            err_body = b""
            try:
                err_body = await upstream.aread()
            except Exception:
                pass
            await upstream.aclose()
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            print(f"[proxy] NVIDIA 429 - rate limited despite pacing. Aborting request.")
            return Response(content=err_body, status_code=429)

        if upstream.status_code >= 400:
            req_info.request_complete_time = datetime.now().isoformat()
            req_info.response_time = round(time.time() - wait_start, 2)
            req_info.status = "failed"
            stats["failed_requests"] += 1
            request_log.append(req_info)
            try:
                err_body = await upstream.aread()
            except Exception:
                err_body = b""
            await upstream.aclose()
            await release_slot_and_decrement()
            last_completion_time = time.monotonic()
            return Response(content=err_body, status_code=upstream.status_code)

        resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
        record_rate_limit_usage_sync()

        async def relay():
            try:
                async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                    async for chunk in upstream.aiter_raw():
                        yield chunk
                req_info.status = "success"
                stats["successful_requests"] += 1
            except Exception:
                req_info.status = "failed"
                stats["failed_requests"] += 1
            finally:
                req_info.request_complete_time = datetime.now().isoformat()
                req_info.response_time = round(time.time() - wait_start, 2)
                request_log.append(req_info)
                await upstream.aclose()
                await release_slot_and_decrement()
                global last_completion_time
                last_completion_time = time.monotonic()

        return StreamingResponse(relay(), status_code=upstream.status_code, headers=resp_headers)

def main():
    parser = argparse.ArgumentParser(description="NVIDIA API Token & Pacing Proxy")
    parser.add_argument("--rpm", "-r", type=int, default=25, help="Max requests per minute (sliding window)")
    parser.add_argument("--port", "-p", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--timeout", "-t", type=int, default=500)
    parser.add_argument("--cooldown", "-c", type=float, default=1.67, help="Post-completion holdout buffer in seconds")
    parser.add_argument("--max-context-tokens", type=int, default=170000)
    parser.add_argument("--keep-last-messages", type=int, default=30)
    parser.add_argument("--no-context-pruning", dest="context_pruning", action="store_false")
    parser.set_defaults(context_pruning=True)

    args = parser.parse_args()

    global RATE_LIMIT_REQUESTS, UPSTREAM_TIMEOUT_SECONDS, MAX_CONCURRENT_REQUESTS, ENABLE_CONTEXT_PRUNING
    global MAX_CONTEXT_TOKENS, KEEP_LAST_MESSAGES, COOLDOWN_BUFFER
    
    RATE_LIMIT_REQUESTS = args.rpm
    UPSTREAM_TIMEOUT_SECONDS = args.timeout
    COOLDOWN_BUFFER = args.cooldown
    MAX_CONCURRENT_REQUESTS = 1  
    ENABLE_CONTEXT_PRUNING = args.context_pruning
    MAX_CONTEXT_TOKENS = args.max_context_tokens
    KEEP_LAST_MESSAGES = args.keep_last_messages

    print(f"\n{'='*60}")
    print(f"  NVIDIA API Intelligent Token & Pacing Proxy")
    print(f"{'='*60}")
    print(f"  Running on:         http://{args.host}:{args.port}/")
    print(f"  Rate Limit:         {RATE_LIMIT_REQUESTS} RPM (sliding window)")
    print(f"  Cooldown Buffer:    {COOLDOWN_BUFFER}s")
    print(f"  Sequential Lock:    1 request at a time (held for full lifecycle)")
    print(f"  Pruning Logic:      Enabled (Ceiling: {MAX_CONTEXT_TOKENS} tokens)")
    print(f"  Message Window:     Always preserve last {KEEP_LAST_MESSAGES} loops")
    print(f"  429 Handling:       Fail immediately - no retries (circuit breaker)")
    print(f"{'='*60}\n")
    
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()