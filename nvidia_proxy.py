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

# TLS trust handling for environments running antivirus certificate interception
try:
    import truststore
    SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
except Exception:  
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

app = FastAPI()

# Global configuration variables managed via command-line arguments
RATE_LIMIT_REQUESTS = 30
RATE_LIMIT_WINDOW_SECONDS = 60
UPSTREAM_TIMEOUT_SECONDS = 500
MAX_429_RETRIES = 5
MAX_CONCURRENT_REQUESTS = 1  
MAX_CONTEXT_TOKENS = 160000
KEEP_LAST_MESSAGES = 30
ENABLE_CONTEXT_PRUNING = True

INFLIGHT_REQUESTS = 0
inflight_lock = asyncio.Lock()
concurrency_semaphore = None

# --- AIR-TIGHT FIXED PACING ENGINE STATE ---
SEQUENTIAL_LOCK = asyncio.Lock()
last_completion_time = time.monotonic()  
COOLDOWN_BUFFER = 2.20  

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

async def enforce_completion_holdout():
    """Guarantees a strict minimum gap of 2.20s has passed since the last completion closed."""
    global last_completion_time
    now = time.monotonic()
    elapsed = now - last_completion_time
    
    if elapsed < COOLDOWN_BUFFER:
        wait_needed = COOLDOWN_BUFFER - elapsed
        print(f"[pacing-engine] Strict holdout active. Waiting {wait_needed:.2f}s after previous completion...")
        await asyncio.sleep(wait_needed)
    
    # Set the predictive baseline to the current time AFTER any required sleep concludes
    last_completion_time = time.monotonic()


# --- SPEC-SAFE CONTEXT PRUNING LOGIC ---
def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)

def estimate_messages_tokens(messages: list) -> int:
    total = 0
    for msg in messages:
        total += estimate_tokens(msg.get("role", ""))
        total += estimate_tokens(msg.get("content", ""))
        total += 3
    return total

def safe_prune_chat_messages(messages: list, max_tokens: int, keep_last: int) -> list:
    """Prunes messages to stay under the TPM token limit without breaking role orders."""
    if not messages or estimate_messages_tokens(messages) <= max_tokens:
        return messages
    
    system_messages = [msg for msg in messages if msg.get("role") == "system"]
    other_messages = [msg for msg in messages if msg.get("role") != "system"]
    
    if len(other_messages) <= keep_last:
        return messages
        
    # Start by taking the requested trailing window slice
    sliced_messages = other_messages[-keep_last:]
    
    # CRITICAL SECURITY FIX: Never allow a 'tool' role to be the very first 
    # message in our chopped conversation array. If it is a tool message, 
    # look backwards and pull in its associated assistant call.
    while sliced_messages and sliced_messages[0].get("role") == "tool":
        current_idx_in_original = len(other_messages) - len(sliced_messages)
        if current_idx_in_original > 0:
            # Prepend the preceding message (the assistant frame)
            sliced_messages.insert(0, other_messages[current_idx_in_original - 1])
        else:
            break
            
    final_messages = system_messages + sliced_messages
    
    # If it is still over budget after slicing, truncate more aggressively recursively
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

request_log = deque(maxlen=100)
queue = deque()
queue_lock = asyncio.Lock()

stats = {
    "total_requests": 0, "successful_requests": 0, "rate_limited_requests": 0, "failed_requests": 0, "context_prunings": 0,
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
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>NVIDIA Proxy (Token Guard Active)</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: sans-serif; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: #e0e0e0; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; color: #00ff88; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 20px; border: 1px solid rgba(255, 255, 255, 0.1); }
        .stat-card h3 { font-size: 0.9rem; color: #888; margin-bottom: 10px; }
        .stat-card .value { font-size: 2rem; font-weight: bold; color: #00ff88; }
        .rate-limit-info { background: rgba(0, 255, 136, 0.1); border: 1px solid #00ff88; border-radius: 12px; padding: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; }
        .limit { font-size: 1.5rem; font-weight: bold; color: #00ff88; }
        .section { background: rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 20px; margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid rgba(255, 255, 255, 0.1); }
        .status-queued { color: #00aaff; } .status-forwarding { color: #00ff88; } .status-success { color: #00ff88; } .status-failed { color: #ff4444; }
    </style>
</head>
<body>
    <div class="container">
        <h1>NVIDIA API Token & Pacing Monitor</h1>
        <div class="rate-limit-info">
            <div><h3>Pacing Layout</h3><div class="limit">Sequential Fixed Cooldown</div></div>
            <div><h3>Active Connections</h3><div class="limit" id="concurrencyDisplay">0 / 1</div></div>
            <div><h3>Current Window RPM</h3><div class="limit" id="currentRpmDisplay">0</div></div>
        </div>
        <div class="stats-grid">
             <div class="stat-card"><h3>Total Enters</h3><div class="value" id="totalRequests">0</div></div>
             <div class="stat-card"><h3>Success</h3><div class="value" id="successfulRequests">0</div></div>
             <div class="stat-card"><h3>Failed/Limited</h3><div class="value" style="color:#ff4444;" id="failedRequests">0</div></div>
             <div class="stat-card"><h3>Prunings Executed</h3><div class="value" style="color:#00aaff;" id="contextPrunings">0</div></div>
        </div>
        <div class="section"><h2>Queue Stream</h2><div id="queueContainer"><p style="color: #888;">Idle</p></div></div>
        <div class="section"><h2>Recent Telemetry Calls</h2><table><thead><tr><th>ID</th><th>Method</th><th>Path</th><th>Status</th><th>Wait Time</th><th>Timestamp</th></tr></thead><tbody id="requestLogTable"></tbody></table></div>
    </div>
    <script>
        let ws = null;
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                document.getElementById('currentRpmDisplay').textContent = data.current_rpm;
                document.getElementById('concurrencyDisplay').textContent = `${data.current_inflight} / ${data.max_concurrency}`;
                document.getElementById('totalRequests').textContent = data.stats.total_requests;
                document.getElementById('successfulRequests').textContent = data.stats.successful_requests;
                document.getElementById('failedRequests').textContent = data.stats.failed_requests + data.stats.rate_limited_requests;
                document.getElementById('contextPrunings').textContent = data.stats.context_prunings;
                
                const q = document.getElementById('queueContainer');
                q.innerHTML = data.queue.length > 0 ? data.queue.map(i => `<div>${i.method} ${i.path} <span class="status-queued">Queued</span></div>`).join('') : '<p style="color:#888;">Idle</p>';
                
                const t = document.getElementById('requestLogTable');
                t.innerHTML = data.request_log.slice(-15).reverse().map(r => `<tr><td>#${r.id}</td><td>${r.method}</td><td>${r.path}</td><td class="status-${r.status.toLowerCase()}">${r.status}</td><td>${r.wait_time}s</td><td>${new Date(r.timestamp).toLocaleTimeString()}</td></tr>`).join('');
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
    global last_completion_time
    stats["total_requests"] += 1
    req_info = RequestInfo(request.method, f"/{path}")

    async with queue_lock:
        req_info.status = "queued"
        queue.append(req_info)

    wait_start = time.time()
    
    # 1. Acquire the lock to cleanly control request sequencing
    await SEQUENTIAL_LOCK.acquire()
    
    try:
        # 2. Enforce the time pacing delay window cleanly
        await enforce_completion_holdout()
        
        # 3. Secure connection slot allocation marker
        await get_concurrency_semaphore().acquire()
        
        req_info.wait_time = round(time.time() - wait_start, 2)

        async with inflight_lock:
            global INFLIGHT_REQUESTS
            INFLIGHT_REQUESTS += 1
            
        async with queue_lock:
            if req_info in queue:
                queue.remove(req_info)
        req_info.status = "forwarding"

    finally:
        # RELEASE IMMEDIATELY AFTER STEP 3: Decouples the lock from long streaming download times!
        SEQUENTIAL_LOCK.release()

    body = await request.body()
    body = await maybe_prune_chat_context(body, f"/{path}")
    
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
        await release_slot_and_decrement()
        await client.aclose()
        req_info.status = "failed"
        stats["failed_requests"] += 1
        request_log.append(req_info)
        last_completion_time = time.monotonic()
        return Response(content=f"Error connecting to NVIDIA backend: {exc}", status_code=502)

    if upstream.status_code >= 400:
        await release_slot_and_decrement()
        err_body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        req_info.status = "failed"
        stats["failed_requests"] += 1
        request_log.append(req_info)
        last_completion_time = time.monotonic()
        return Response(content=err_body, status_code=upstream.status_code)

    resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
    finalised = False

    async def relay():
        global last_completion_time
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
            await release_slot_and_decrement()
            
            # Reset completion time marker to track exactly when connection closed!
            last_completion_time = time.monotonic()

    return StreamingResponse(relay(), status_code=upstream.status_code, headers=resp_headers)

def main():
    parser = argparse.ArgumentParser(description="NVIDIA API Token & Pacing Proxy")
    parser.add_argument("--rpm", "-r", type=int, default=30)
    parser.add_argument("--port", "-p", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--timeout", "-t", type=int, default=500)
    parser.add_argument("--max-context-tokens", type=int, default=160000)
    parser.add_argument("--keep-last-messages", type=int, default=30)
    parser.add_argument("--no-context-pruning", dest="context_pruning", action="store_false")
    parser.set_defaults(context_pruning=True)

    args = parser.parse_args()

    global RATE_LIMIT_REQUESTS, UPSTREAM_TIMEOUT_SECONDS, MAX_CONCURRENT_REQUESTS, ENABLE_CONTEXT_PRUNING
    global MAX_CONTEXT_TOKENS, KEEP_LAST_MESSAGES
    
    RATE_LIMIT_REQUESTS = args.rpm
    UPSTREAM_TIMEOUT_SECONDS = args.timeout
    MAX_CONCURRENT_REQUESTS = 1  
    ENABLE_CONTEXT_PRUNING = args.context_pruning
    MAX_CONTEXT_TOKENS = args.max_context_tokens
    KEEP_LAST_MESSAGES = args.keep_last_messages

    print(f"\n{'='*60}")
    print(f"  NVIDIA API Intelligent Token & Pacing Proxy")
    print(f"{'='*60}")
    print(f"  Running on:         http://{args.host}:{args.port}/")
    print(f"  Pacing Setup:       1 Upstream Connection Stream Max")
    print(f"  Pruning Logic:      Enabled (Ceiling: {MAX_CONTEXT_TOKENS} tokens)")
    print(f"  Message Window:     Always preserve last {KEEP_LAST_MESSAGES} loops")
    print(f"{'='*60}\n")
    
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()