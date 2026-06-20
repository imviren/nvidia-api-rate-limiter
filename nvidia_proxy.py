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

try:
    import truststore
    SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
except Exception:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

app = FastAPI()

RATE_LIMIT_REQUESTS = 40
UPSTREAM_TIMEOUT_SECONDS = 500
MAX_CONCURRENT_REQUESTS = 1
MAX_CONTEXT_TOKENS = 160000
KEEP_LAST_MESSAGES = 30
ENABLE_CONTEXT_PRUNING = True

INFLIGHT_REQUESTS = 0
inflight_lock = asyncio.Lock()
concurrency_semaphore = None

SEQUENTIAL_LOCK = asyncio.Lock()
last_completion_time = 0.0      # monotonic clock — when the previous request FULLY completed
last_send_time = 0.0           # monotonic clock — when the previous request was SENT to NVIDIA
COOLDOWN_BUFFER = 1.67
stream_complete_event = asyncio.Event()
stream_complete_event.set()

# ---- PER-MODEL COOLDOWN ----
# A gateway 429 from NVIDIA is a per-model quota, not a global rate the proxy can
# pace under. So instead of locking the WHOLE proxy (the old hard circuit breaker),
# we bench ONLY the offending model for MODEL_COOLDOWN_SECONDS. Requests for every
# other model keep flowing, and the bench clears automatically — no restart needed.
MODEL_COOLDOWN_SECONDS = 7200            # how long a model stays benched after a 429 (2h default)
model_cooldowns = {}                     # model name -> epoch (time.time()) when the bench expires
model_cooldown_lock = asyncio.Lock()

def log(msg, req_info=None, tag="proxy"):
    """Timestamped console logger for pacing/debugging. flush=True so lines
    appear immediately in the proxy window."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    rid = f"#{req_info.id} " if req_info is not None else ""
    print(f"[{ts}] [{tag}] {rid}{msg}", flush=True)

def get_concurrency_semaphore():
    global concurrency_semaphore
    if concurrency_semaphore is None:
        concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return concurrency_semaphore

def release_slot_sync():
    """Release the concurrency slot WITHOUT awaiting. Safe to call from a
    finally block even while the task is being cancelled — no await means the
    cancellation cannot interrupt it half-way and leak the slot."""
    global INFLIGHT_REQUESTS
    INFLIGHT_REQUESTS = max(0, INFLIGHT_REQUESTS - 1)
    get_concurrency_semaphore().release()

def extract_model(body: bytes):
    """Pull the 'model' field out of a JSON request body, if present. Returns
    None for empty/non-JSON bodies or bodies without a usable string model —
    in which case the per-model cooldown gate simply lets the request pass."""
    if not body:
        return None
    try:
        data = json.loads(body)
    except Exception:
        return None
    if isinstance(data, dict):
        m = data.get("model")
        if isinstance(m, str) and m:
            return m
    return None

async def get_model_cooldown(model):
    """If `model` is currently benched, return the epoch time its cooldown
    expires; otherwise return None. Expired entries are evicted on read."""
    if not model:
        return None
    async with model_cooldown_lock:
        expiry = model_cooldowns.get(model)
        if expiry is None:
            return None
        if time.time() >= expiry:
            model_cooldowns.pop(model, None)
            return None
        return expiry

async def put_model_cooldown(model, seconds):
    """Bench `model` for `seconds` from now. Returns the epoch expiry, or None
    if we couldn't identify the model (nothing to key the cooldown on)."""
    if not model:
        return None
    expiry = time.time() + seconds
    async with model_cooldown_lock:
        model_cooldowns[model] = expiry
    return expiry

def get_cooldowns_snapshot():
    """Live snapshot of benched models for the dashboard (no lock — read only)."""
    now = time.time()
    out = []
    for m, expiry in list(model_cooldowns.items()):
        remaining = int(round(expiry - now))
        if remaining > 0:
            out.append({
                "model": m,
                "remaining_seconds": remaining,
                "until": datetime.fromtimestamp(expiry).strftime("%H:%M:%S"),
            })
    return out

async def wait_for_holdout(req_info=None):
    """
    Block until it is safe to issue the next NVIDIA request. THREE gates:
      1) the previous request's response stream has fully completed (event), and
      2) >= COOLDOWN_BUFFER seconds since the previous COMPLETION, and
      3) >= COOLDOWN_BUFFER seconds since the previous SEND.

    Gate 3 is the hard anti-429 floor: it guarantees two requests can never hit
    NVIDIA inside the cooldown window even if completion tracking is ever
    disturbed (cancelled stream, error path, etc.). We loop because sleeping on
    one clock can let the other fall behind.

    Returns (total_holdout_waited, gap_since_previous_completion) in seconds.
    """
    global last_completion_time, last_send_time

    if not stream_complete_event.is_set():
        log("previous stream still streaming — waiting for it to finish before pacing", req_info, tag="pacing")
    await stream_complete_event.wait()

    total_waited = 0.0
    while True:
        now = time.monotonic()
        gap_completion = (now - last_completion_time) if last_completion_time > 0 else COOLDOWN_BUFFER
        gap_send = (now - last_send_time) if last_send_time > 0 else COOLDOWN_BUFFER
        wait_needed = max(COOLDOWN_BUFFER - gap_completion, COOLDOWN_BUFFER - gap_send)
        if wait_needed <= 0.0005:
            return total_waited, gap_completion
        log(f"holdout: sleeping {wait_needed:.3f}s "
            f"(since completion {gap_completion:.3f}s / since send {gap_send:.3f}s, need {COOLDOWN_BUFFER}s)",
            req_info, tag="pacing")
        await asyncio.sleep(wait_needed)
        total_waited += wait_needed

rate_limit_window = deque()
rate_limit_lock = asyncio.Lock()

async def enforce_rate_limit(req_info=None):
    while True:
        async with rate_limit_lock:
            now = time.monotonic()
            while rate_limit_window and now - rate_limit_window[0] > 60:
                rate_limit_window.popleft()
            if len(rate_limit_window) < RATE_LIMIT_REQUESTS:
                return
            wait = rate_limit_window[0] + 60 - now
        log(f"RPM window full ({len(rate_limit_window)}/{RATE_LIMIT_REQUESTS} in 60s) — waiting {min(wait, 1.0):.2f}s",
            req_info, tag="rpm")
        await asyncio.sleep(min(wait, 1.0))

async def record_rate_limit_usage():
    async with rate_limit_lock:
        rate_limit_window.append(time.monotonic())

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
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    return shared_client

async def shutdown_shared_client():
    global shared_client
    if shared_client:
        await shared_client.aclose()
        shared_client = None

request_log = deque(maxlen=100)
queue = deque()
queue_lock = asyncio.Lock()

stats = {
    "total_requests": 0, "successful_requests": 0, "rate_limited_requests": 0,
    "failed_requests": 0, "context_prunings": 0,
    "start_time": datetime.now().isoformat(),
}

class RequestInfo:
    def __init__(self, method: str, path: str, status: str = "pending", wait_time: float = 0):
        self.id = str(uuid.uuid4())[:8]
        self.method = method
        self.path = path
        self.model = None
        self.status = status
        self.wait_time = wait_time
        self.timestamp = datetime.now()
        self.response_time = None
        self.request_sent_time = None
        self.request_complete_time = None
        self.holdout_waited = 0.0
        self.gap_from_previous = 0.0
        self.holdout_compliant = None

    def to_dict(self):
        return {
            "id": self.id, "method": self.method, "path": self.path, "model": self.model, "status": self.status,
            "wait_time": round(self.wait_time, 2), "timestamp": self.timestamp.isoformat(),
            "response_time": self.response_time, "request_sent_time": self.request_sent_time,
            "request_complete_time": self.request_complete_time,
            "holdout_waited": round(self.holdout_waited, 2),
            "gap_from_previous": round(self.gap_from_previous, 2),
            "holdout_compliant": self.holdout_compliant
        }

def get_current_rpm():
    now = time.monotonic()
    count = 0
    for t in rate_limit_window:
        if now - t <= 60:
            count += 1
    return count

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
        .rate-limit-info > div { min-width: 100px; }
        .rate-limit-info h3 { font-size: 0.7rem; color: #888; text-transform: uppercase; letter-spacing: 1px; }
        .rate-limit-info .limit { font-size: 1.3rem; font-weight: 600; color: #00ff88; }
        .section { background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 16px; border: 1px solid rgba(255,255,255,0.05); }
        .section h2 { font-size: 0.95rem; color: #aaa; margin-bottom: 12px; font-weight: 400; letter-spacing: 1px; }
        table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
        th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,0.06); white-space: nowrap; }
        th { color: #666; font-weight: 500; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; }
        .status-success { color: #00ff88; } .status-failed { color: #ff4444; } .status-rate-limited { color: #ff8800; } .status-cooldown { color: #ff8800; } .status-disconnected { color: #ffaa00; } .status-forwarding { color: #00aaff; } .status-queued { color: #666; }
        .badge-ok { display: inline-block; background: #00ff8822; color: #00ff88; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
        .badge-fail { display: inline-block; background: #ff444422; color: #ff4444; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
        .badge-na { display: inline-block; background: #66666622; color: #666; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; }
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
            <div><h3>Stream</h3><div class="limit" id="streamDisplay" style="color:#00ff88;">idle</div></div>
            <div><h3>Cooldowns</h3><div class="limit" id="cooldownCountDisplay" style="color:#00ff88;">0</div></div>
        </div>
        <div class="stats-grid">
            <div class="stat-card"><h3>Total</h3><div class="value" id="totalRequests">0</div></div>
            <div class="stat-card"><h3>Success</h3><div class="value" style="color:#00ff88;" id="successfulRequests">0</div></div>
            <div class="stat-card"><h3>Failed</h3><div class="value" style="color:#ff4444;" id="failedRequests">0</div></div>
            <div class="stat-card"><h3>429</h3><div class="value" style="color:#ff8800;" id="rateLimitedRequests">0</div></div>
            <div class="stat-card"><h3>Holdout OK</h3><div class="value" style="color:#00ff88;" id="holdoutOkCount">0</div></div>
            <div class="stat-card"><h3>Holdout FAIL</h3><div class="value" style="color:#ff4444;" id="holdoutFailCount">0</div></div>
        </div>
        <div class="section"><h2>Model Cooldowns</h2><div id="cooldownContainer"><p style="color:#555;font-size:0.85rem;">None — all models available</p></div></div>
        <div class="section"><h2>Queue</h2><div id="queueContainer"><p style="color:#555;font-size:0.85rem;">Idle</p></div></div>
        <div class="section">
            <h2>Request Log</h2>
            <div style="overflow-x:auto;">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th><th>Model</th><th>Status</th>
                            <th>Sent to NVIDIA</th><th>Completed</th><th>Duration</th>
                            <th>Holdout Waited</th><th>Gap Prev→Sent</th><th>Holdout OK</th>
                        </tr>
                    </thead>
                    <tbody id="requestLogTable"></tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
        let ws = null;
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            ws.onmessage = (event) => {
                const d = JSON.parse(event.data);
                document.getElementById('rateLimitDisplay').textContent = d.rate_limit_rpm + ' RPM';
                document.getElementById('cooldownDisplay').textContent = d.cooldown_buffer + 's';
                document.getElementById('concurrencyDisplay').textContent = d.current_inflight + ' / ' + d.max_concurrency;
                document.getElementById('currentRpmDisplay').textContent = d.current_rpm;
                const cd = d.model_cooldowns || [];
                document.getElementById('cooldownCountDisplay').textContent = cd.length;
                document.getElementById('cooldownCountDisplay').style.color = cd.length > 0 ? '#ff8800' : '#00ff88';
                document.getElementById('streamDisplay').textContent = d.stream_in_progress ? 'active' : 'idle';
                document.getElementById('streamDisplay').style.color = d.stream_in_progress ? '#00aaff' : '#00ff88';
                document.getElementById('totalRequests').textContent = d.stats.total_requests;
                document.getElementById('successfulRequests').textContent = d.stats.successful_requests;
                document.getElementById('failedRequests').textContent = d.stats.failed_requests;
                document.getElementById('rateLimitedRequests').textContent = d.stats.rate_limited_requests;

                const completed = d.request_log.filter(r => r.status === 'success' || r.status === 'failed' || r.status === 'rate-limited' || r.status === 'Disconnected');
                const holdoutOk = completed.filter(r => r.holdout_compliant === true).length;
                const holdoutFail = completed.filter(r => r.holdout_compliant === false).length;
                document.getElementById('holdoutOkCount').textContent = holdoutOk;
                document.getElementById('holdoutFailCount').textContent = holdoutFail;

                const cc = document.getElementById('cooldownContainer');
                cc.innerHTML = cd.length > 0
                    ? cd.map(c => '<div style="padding:4px 0;font-size:0.85rem;"><span style="color:#ff8800;">' + c.model + '</span> <span class="mono">benched ' + c.remaining_seconds + 's (until ' + c.until + ')</span></div>').join('')
                    : '<p style="color:#555;font-size:0.85rem;">None — all models available</p>';

                const q = document.getElementById('queueContainer');
                q.innerHTML = d.queue.length > 0
                    ? d.queue.map(i => '<div style="padding:4px 0;font-size:0.85rem;">' + i.method + ' ' + i.path + ' <span class="status-queued">Queued</span> <span class="mono">(' + i.wait_time + 's)</span></div>').join('')
                    : '<p style="color:#555;font-size:0.85rem;">Idle</p>';

                const t = document.getElementById('requestLogTable');
                t.innerHTML = d.request_log.slice(-40).reverse().map(r => {
                    const sent = r.request_sent_time ? new Date(r.request_sent_time).toLocaleTimeString() : '-';
                    const comp = r.request_complete_time ? new Date(r.request_complete_time).toLocaleTimeString() : '-';
                    const dur = r.response_time ? r.response_time.toFixed(2) + 's' : '-';
                    const hw = (r.holdout_waited || 0).toFixed(2) + 's';
                    const gap = (r.gap_from_previous || 0).toFixed(2) + 's';
                    const model = r.model ? r.model.split('/').pop() : '-';
                    let ok;
                    if (r.holdout_compliant === true) ok = '<span class="badge-ok">OK</span>';
                    else if (r.holdout_compliant === false) ok = '<span class="badge-fail">FAIL</span>';
                    else ok = '<span class="badge-na">N/A</span>';
                    return '<tr><td class="mono">#' + r.id + '</td><td class="mono">' + model + '</td><td class="status-' + r.status.toLowerCase() + '">' + r.status + '</td><td class="mono">' + sent +
                        '</td><td class="mono">' + comp + '</td><td class="mono">' + dur + '</td><td class="mono">' + hw +
                        '</td><td class="mono">' + gap + '</td><td>' + ok + '</td></tr>';
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
                "rate_limit_rpm": RATE_LIMIT_REQUESTS, "cooldown_buffer": round(COOLDOWN_BUFFER, 2),
                "current_rpm": get_current_rpm(), "upstream_timeout": UPSTREAM_TIMEOUT_SECONDS,
                "max_concurrency": MAX_CONCURRENT_REQUESTS, "current_inflight": INFLIGHT_REQUESTS,
                "stats": stats, "queue": [r.to_dict() for r in queue],
                "request_log": [r.to_dict() for r in request_log][-50:],
                "model_cooldowns": get_cooldowns_snapshot(),
                "stream_in_progress": not stream_complete_event.is_set()
            }
            await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_handler(request: Request, path: str):
    global last_completion_time, last_send_time, INFLIGHT_REQUESTS
    if request.method == "OPTIONS":
        return Response(status_code=204)

    stats["total_requests"] += 1
    req_info = RequestInfo(request.method, f"/{path}")

    # ---- read the body up front: needed both to detect the model (for the
    #      per-model cooldown gate) and to prune oversized chat context ----
    try:
        body = await request.body()
    except ClientDisconnect:
        req_info.status = "Disconnected"
        stats["failed_requests"] += 1
        log("client disconnected before send — no NVIDIA call made", req_info, tag="disconnect")
        return Response(status_code=244, content="Client disconnected from proxy channel early.")

    model = extract_model(body)
    req_info.model = model

    # ---- PER-MODEL COOLDOWN GATE: a benched model is rejected instantly here,
    #      with no queueing or pacing, while every other model passes through ----
    cooldown_until = await get_model_cooldown(model)
    if cooldown_until is not None:
        remaining = max(0, int(round(cooldown_until - time.time())))
        avail = datetime.fromtimestamp(cooldown_until).strftime("%H:%M:%S")
        req_info.status = "cooldown"
        req_info.request_complete_time = datetime.now().isoformat()
        request_log.append(req_info)
        log(f"REJECTED /{path} model={model} — benched {remaining}s more (until {avail}); other models OK",
            req_info, tag="cooldown")
        return Response(
            content=json.dumps({"error": {
                "message": (f"Model '{model}' is in local cooldown after an NVIDIA gateway 429 "
                            f"(per-model quota). Retry after ~{remaining}s (around {avail}). "
                            f"Other models are unaffected — no proxy restart needed."),
                "type": "model_cooldown", "code": 429,
                "model": model, "retry_after_seconds": remaining}}),
            status_code=429, media_type="application/json",
            headers={"Retry-After": str(remaining)})

    async with queue_lock:
        queue.append(req_info)
    log(f"RECEIVED {request.method} /{path}  model={model or '-'}  (queue depth: {len(queue)})", req_info)

    wait_start = time.time()

    async with SEQUENTIAL_LOCK:
        # ---- PACING GATE: previous stream done + cooldown since completion AND since last send ----
        holdout_waited, gap_from_completion = await wait_for_holdout(req_info)
        req_info.holdout_waited = holdout_waited
        req_info.gap_from_previous = gap_from_completion

        await enforce_rate_limit(req_info)
        await get_concurrency_semaphore().acquire()
        INFLIGHT_REQUESTS += 1

        req_info.wait_time = round(time.time() - wait_start, 2)

        async with queue_lock:
            if req_info in queue:
                queue.remove(req_info)

        upstream = None
        handed_off_to_relay = False
        try:
            body = await maybe_prune_chat_context(body, f"/{path}")

            fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() not in ("host",)}
            fwd_headers["host"] = "integrate.api.nvidia.com"
            if body:
                fwd_headers["content-length"] = str(len(body))

            target_url = f"{NVIDIA_BASE_URL}/{path}"
            if request.url.query:
                target_url += f"?{request.url.query}"

            client = await get_shared_client()

            # ---- record the SEND moment and the REAL gaps (this is the anti-429 invariant) ----
            now_send = time.monotonic()
            gap_since_send = (now_send - last_send_time) if last_send_time > 0 else None
            gap_since_completion = (now_send - last_completion_time) if last_completion_time > 0 else None
            last_send_time = now_send
            req_info.holdout_compliant = None if gap_since_send is None else (gap_since_send >= COOLDOWN_BUFFER - 0.01)
            req_info.request_sent_time = datetime.now().isoformat()

            gs = f"{gap_since_send:.3f}s" if gap_since_send is not None else "n/a (first request)"
            gc = f"{gap_since_completion:.3f}s" if gap_since_completion is not None else "n/a (first request)"
            log(f"→ SENDING to NVIDIA  {request.method} /{path}  "
                f"gap_since_prev_SEND={gs}  gap_since_prev_COMPLETION={gc}  queued_for={req_info.wait_time}s",
                req_info, tag="send")
            if gap_since_send is not None and gap_since_send < COOLDOWN_BUFFER - 0.01:
                log(f"!! SPACING VIOLATION: only {gap_since_send:.3f}s since previous send (need {COOLDOWN_BUFFER}s)",
                    req_info, tag="WARN")

            try:
                async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                    upstream_req = client.build_request(method=request.method, url=target_url, headers=fwd_headers, content=body)
                    upstream = await client.send(upstream_req, stream=True)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.NetworkError, ssl.SSLError) as exc:
                req_info.status = "failed"
                stats["failed_requests"] += 1
                log(f"network error: {type(exc).__name__}: {exc}", req_info, tag="error")
                return Response(content=f"NVIDIA connection failed: {type(exc).__name__}", status_code=502)
            except asyncio.TimeoutError:
                req_info.status = "failed"
                stats["failed_requests"] += 1
                log(f"upstream connect timeout after {UPSTREAM_TIMEOUT_SECONDS}s", req_info, tag="error")
                return Response(content="NVIDIA upstream timeout", status_code=504)
            except Exception as exc:
                req_info.status = "failed"
                stats["failed_requests"] += 1
                log(f"upstream error: {type(exc).__name__}: {exc}", req_info, tag="error")
                return Response(content=f"NVIDIA upstream error: {type(exc).__name__}", status_code=502)

            log(f"NVIDIA responded status={upstream.status_code}", req_info, tag="recv")

            if upstream.status_code == 429:
                expiry = await put_model_cooldown(model, MODEL_COOLDOWN_SECONDS)
                req_info.status = "rate-limited"
                stats["rate_limited_requests"] += 1
                try: err_body = await upstream.aread()
                except Exception: err_body = b""
                if expiry is not None:
                    avail = datetime.fromtimestamp(expiry).strftime("%H:%M:%S")
                    log(f"NVIDIA 429 for model={model} — benching it {MODEL_COOLDOWN_SECONDS}s "
                        f"(until {avail}); other models keep working, no restart needed",
                        req_info, tag="cooldown")
                else:
                    log("NVIDIA 429 but no model field in body — cannot bench a specific model; "
                        "passing the 429 through unchanged", req_info, tag="cooldown")
                return Response(content=err_body, status_code=429)

            if upstream.status_code >= 400:
                req_info.status = "failed"
                stats["failed_requests"] += 1
                try: err_body = await upstream.aread()
                except Exception: err_body = b""
                log(f"NVIDIA error status={upstream.status_code}", req_info, tag="error")
                return Response(content=err_body, status_code=upstream.status_code)

            resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
            await record_rate_limit_usage()

            stream_complete_event.clear()   # close the pacing gate; relay() re-opens it
            handed_off_to_relay = True       # relay() now owns slot release + gate re-open

            async def relay():
                global last_completion_time
                try:
                    async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                        async for chunk in upstream.aiter_raw():
                            yield chunk
                    req_info.status = "success"
                    stats["successful_requests"] += 1
                except Exception as exc:
                    req_info.status = "failed"
                    stats["failed_requests"] += 1
                    log(f"stream relay error: {type(exc).__name__}: {exc}", req_info, tag="error")
                finally:
                    # --- cancellation-PROOF critical section: no awaits before the gate re-opens ---
                    release_slot_sync()
                    last_completion_time = time.monotonic()
                    stream_complete_event.set()                 # re-open the pacing gate
                    req_info.request_complete_time = datetime.now().isoformat()
                    req_info.response_time = round(time.time() - wait_start, 2)
                    request_log.append(req_info)
                    log(f"✓ COMPLETED  status={req_info.status}  duration={req_info.response_time}s  "
                        f"(next send gated +{COOLDOWN_BUFFER}s)", req_info, tag="done")
                    # awaitable cleanup LAST — safe even if interrupted; gate is already open
                    try:
                        await upstream.aclose()
                    except BaseException:
                        pass

            return StreamingResponse(relay(), status_code=upstream.status_code, headers=resp_headers)

        finally:
            if not handed_off_to_relay:
                # Request never started streaming (error / disconnect / cancellation).
                # Release the slot, re-open the pacing gate, and stamp completion so the
                # NEXT request paces from now. All synchronous => cancellation-proof.
                release_slot_sync()
                last_completion_time = time.monotonic()
                # the event is only cleared on the success path, so the gate is still open here
                if req_info.request_complete_time is None:
                    req_info.request_complete_time = datetime.now().isoformat()
                if req_info.response_time is None:
                    req_info.response_time = round(time.time() - wait_start, 2)
                if req_info not in request_log:
                    request_log.append(req_info)
                if upstream is not None:
                    try:
                        await upstream.aclose()
                    except BaseException:
                        pass

@app.on_event("shutdown")
async def shutdown_event():
    await shutdown_shared_client()

def main():
    parser = argparse.ArgumentParser(description="NVIDIA API Token & Pacing Proxy")
    parser.add_argument("--rpm", "-r", type=int, default=25, help="Max requests per minute (sliding window)")
    parser.add_argument("--port", "-p", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--timeout", "-t", type=int, default=500)
    parser.add_argument("--cooldown", "-c", type=float, default=1.67, help="Post-completion holdout buffer in seconds")
    parser.add_argument("--model-cooldown", type=int, default=7200, help="Seconds to bench a model after it returns a 429 (default 7200 = 2h)")
    parser.add_argument("--max-context-tokens", type=int, default=170000)
    parser.add_argument("--keep-last-messages", type=int, default=30)
    parser.add_argument("--no-context-pruning", dest="context_pruning", action="store_false")
    parser.set_defaults(context_pruning=True)

    args = parser.parse_args()

    global RATE_LIMIT_REQUESTS, UPSTREAM_TIMEOUT_SECONDS, MAX_CONCURRENT_REQUESTS, ENABLE_CONTEXT_PRUNING
    global MAX_CONTEXT_TOKENS, KEEP_LAST_MESSAGES, COOLDOWN_BUFFER, MODEL_COOLDOWN_SECONDS

    RATE_LIMIT_REQUESTS = args.rpm
    UPSTREAM_TIMEOUT_SECONDS = args.timeout
    COOLDOWN_BUFFER = args.cooldown
    MODEL_COOLDOWN_SECONDS = args.model_cooldown
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
    print(f"  Pacing Gates:       (1) prev stream finished  (2) >={COOLDOWN_BUFFER}s since completion  (3) >={COOLDOWN_BUFFER}s since send")
    print(f"  Console Logging:    Per-request RECEIVED / SEND / RECV / COMPLETED with timestamps + real gaps")
    print(f"  Pruning Logic:      Enabled (Ceiling: {MAX_CONTEXT_TOKENS} tokens)")
    print(f"  Message Window:     Always preserve last {KEEP_LAST_MESSAGES} loops")
    print(f"  429 Handling:       Per-model cooldown — bench the offending model {MODEL_COOLDOWN_SECONDS}s ({MODEL_COOLDOWN_SECONDS/3600:.2g}h), other models keep flowing, no restart")
    print(f"{'='*60}\n")

    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()