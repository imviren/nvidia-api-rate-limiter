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
import re

# TLS trust: this machine runs AVG Antivirus, which intercepts HTTPS and
# re-signs certs with a private root CA that OpenSSL/certifi reject. Verify via
# the OS trust store (Windows SChannel) using truststore so the AVG-signed cert
# is accepted exactly as the browser accepts it. Fall back to certifi if needed.
try:
    import truststore
    SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
except Exception:  # pragma: no cover - fallback for non-intercepted environments
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

app = FastAPI()


def estimate_tokens(text: str) -> int:
    """Estimate token count for text. Rough approximation: 4 chars per token for English."""
    if not text:
        return 0
    # Simple approximation: count characters and divide by 4
    # This is rough but works for most English text without needing tiktoken
    return max(1, len(text) // 4)


def estimate_messages_tokens(messages: list) -> int:
    """Estimate total tokens in a list of chat messages."""
    total = 0
    for msg in messages:
        # Add tokens for role and content
        total += estimate_tokens(msg.get("role", ""))
        total += estimate_tokens(msg.get("content", ""))
        # Add overhead for message formatting (approximately 3 tokens per message)
        total += 3
    return total


def prune_chat_messages(messages: list, max_tokens: int = 8000, keep_last: int = 10) -> list:
    """
    Prune chat messages to stay under token limit using sliding window.
    
    Strategy:
    1. Always keep the system message (if present) at the beginning
    2. Keep the last `keep_last` user/assistant turns
    3. If still over limit, summarize older messages into a summary message
    4. If still over limit, truncate from the beginning
    
    Returns pruned list of messages.
    """
    if not messages:
        return messages
    
    # Separate system messages from others
    system_messages = [msg for msg in messages if msg.get("role") == "system"]
    other_messages = [msg for msg in messages if msg.get("role") != "system"]
    
    # If we have no system messages, just work with all messages
    if not system_messages:
        system_messages = []
        other_messages = messages
    
    # Estimate current token usage
    current_tokens = estimate_messages_tokens(messages)
    
    # If already under limit, return as-is
    if current_tokens <= max_tokens:
        return messages
    
    # Strategy 1: Keep system messages + last N messages
    if len(other_messages) <= keep_last:
        # We have few messages, just return all (shouldn't happen if over limit, but safe)
        return messages
    
    # Keep last `keep_last` messages from other_messages
    kept_other = other_messages[-keep_last:]
    pruned_messages = system_messages + kept_other
    
    # Check if we're under limit now
    pruned_tokens = estimate_messages_tokens(pruned_messages)
    if pruned_tokens <= max_tokens:
        return pruned_messages
    
    # Strategy 2: Create a summary of removed messages
    removed_messages = other_messages[:-keep_last]
    if removed_messages:
        # Create summary content
        summary_parts = []
        for msg in removed_messages:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if content:
                summary_parts.append(f"{role}: {content[:100]}{'...' if len(content) > 100 else ''}")
        
        summary_content = "Previous conversation summary: " + " | ".join(summary_parts)
        summary_message = {
            "role": "system",
            "content": summary_content
        }
        
        # Put summary at the beginning, after system messages
        summarized_messages = system_messages + [summary_message] + kept_other
        
        # Check if we're under limit now
        summarized_tokens = estimate_messages_tokens(summarized_messages)
        if summarized_tokens <= max_tokens:
            return summarized_messages
    
    # Strategy 3: If still over limit, keep fewer messages and try again
    # Recursively try with fewer kept messages
    if keep_last > 1:
        return prune_chat_messages(messages, max_tokens, keep_last - 1)
    
    # Strategy 4: Last resort - truncate aggressively
    # Keep only system messages and the very last message
    if other_messages:
        return system_messages + [other_messages[-1]]
    else:
        return system_messages


async def maybe_prune_chat_context(body: bytes, path: str) -> bytes:
    """
    If this is a chat completion request, attempt to prune the context to reduce tokens.
    Returns the (possibly modified) body as bytes.
    """
    # Only process if context pruning is enabled
    if not ENABLE_CONTEXT_PRUNING:
        return body
    
    # Only process chat completion endpoints
    if "chat/completions" not in path:
        return body
    
    try:
        # Try to parse as JSON
        if not body:
            return body
            
        text_body = body.decode("utf-8")
        data = json.loads(text_body)
        
        # Check if this looks like a chat completion request
        if "messages" not in data or not isinstance(data["messages"], list):
            return body
        
        original_messages = data["messages"]
        if not original_messages:
            return body
        
        # Estimate tokens
        estimated_tokens = estimate_messages_tokens(original_messages)
        
        # If under reasonable limit (MAX_CONTEXT_TOKENS + 4000 for response), don't prune
        if estimated_tokens < (MAX_CONTEXT_TOKENS + 4000):
            return body
        
        # Prune messages to target MAX_CONTEXT_TOKENS tokens (leaving room for response)
        pruned_messages = prune_chat_messages(original_messages, max_tokens=MAX_CONTEXT_TOKENS, keep_last=KEEP_LAST_MESSAGES)
        
        # If pruning didn't change anything, return original
        if pruned_messages == original_messages:
            return body
        
        # Update the messages and re-serialize
        data["messages"] = pruned_messages
        new_body = json.dumps(data).encode("utf-8")
        
        # Log the reduction for debugging
        original_tokens = estimate_messages_tokens(original_messages)
        pruned_tokens = estimate_messages_tokens(pruned_messages)
        print(f"[context-pruning] Reduced tokens from {original_tokens} to {pruned_tokens} "
              f"({len(original_messages)} -> {len(pruned_messages)} messages)")
        stats["context_prunings"] += 1
        
        return new_body
        
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError):
        # If we can't parse or process, return original body
        return body
    except Exception as e:
        # Log unexpected errors but don't break the request
        print(f"[context-pruning] Error processing chat context: {e}")
        return body

# Global configuration (set via command-line args)
RATE_LIMIT_REQUESTS = 40
RATE_LIMIT_WINDOW_SECONDS = 60
UPSTREAM_TIMEOUT_SECONDS = 500
MAX_429_RETRIES = 5
MAX_CONCURRENT_REQUESTS = 4
MAX_CONTEXT_TOKENS = 8000
KEEP_LAST_MESSAGES = 10
ENABLE_CONTEXT_PRUNING = True

INFLIGHT_REQUESTS = 0
inflight_lock = asyncio.Lock()
concurrency_semaphore = None


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

# Pacing (leaky-bucket) state.  A token bucket that starts full would let a
# burst of RATE_LIMIT_REQUESTS requests hit NVIDIA at once and trip its 429
# limiter.  Instead we hand out evenly-spaced send slots: every forwarded
# request is held until at least RATE_LIMIT_WINDOW_SECONDS / RATE_LIMIT_REQUESTS
# seconds after the previous one (e.g. 30 RPM -> one request every 2s).
pace_lock = asyncio.Lock()
next_slot_time = 0.0  # monotonic timestamp of the next free send slot

# Target URL for NVIDIA API
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com"

# Hop-by-hop headers must not be forwarded (RFC 7230 6.1). We also drop
# host/content-length (recomputed) on the way out and content-length on the
# way back, since we stream with chunked transfer encoding.
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}

# Request tracking for dashboard
request_log = deque(maxlen=100)
queue = deque()
queue_lock = asyncio.Lock()

# Statistics
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
            "id": self.id,
            "method": self.method,
            "path": self.path,
            "status": self.status,
            "wait_time": round(self.wait_time, 2),
            "timestamp": self.timestamp.isoformat(),
            "response_time": self.response_time
        }


def pacing_interval() -> float:
    """Minimum seconds between two requests forwarded upstream."""
    return RATE_LIMIT_WINDOW_SECONDS / max(1, RATE_LIMIT_REQUESTS)


async def acquire_slot() -> float:
    """Reserve the next evenly-spaced send slot.

    Returns how many seconds the caller must wait before forwarding so its
    request lands exactly one interval after the previously reserved one.
    Reserving and waiting are split so the lock is held only briefly.
    """
    global next_slot_time
    async with pace_lock:
        now = time.monotonic()
        slot = max(now, next_slot_time)
        next_slot_time = slot + pacing_interval()
        return slot - now


async def back_off(seconds: float):
    """Push every pending slot back when NVIDIA still answers 429."""
    global next_slot_time
    async with pace_lock:
        next_slot_time = max(next_slot_time, time.monotonic() + seconds)


def parse_retry_after(headers) -> float:
    """Seconds to wait from a Retry-After header, or 0 if absent/unparseable."""
    value = headers.get("retry-after")
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        # HTTP-date form is not parsed here; caller falls back to backoff.
        return 0.0


def get_current_rpm():
    """Calculate current requests per minute accurately.

    Counts completed requests from the log PLUS currently in-flight
    and queued requests.  Without the latter the dashboard shows a
    stale number while 30+ requests are hitting NVIDIA at once.
    """
    now = datetime.now()
    completed = sum(1 for r in request_log if (now - r.timestamp).total_seconds() < 60)
    in_flight = INFLIGHT_REQUESTS
    queued = len(queue)
    return completed + in_flight + queued


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the modern lightweight dashboard."""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NVIDIA API Rate Limit Proxy</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e0e0e0;
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 {
            text-align: center;
            margin-bottom: 30px;
            color: #00ff88;
            font-size: 2rem;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .stat-card h3 {
            font-size: 0.9rem;
            color: #888;
            margin-bottom: 10px;
        }
        .stat-card .value {
            font-size: 2rem;
            font-weight: bold;
            color: #00ff88;
        }
        .stat-card .value.warning {
            color: #ffaa00;
        }
        .stat-card .value.danger {
            color: #ff4444;
        }
        .rate-limit-info {
            background: rgba(0, 255, 136, 0.1);
            border: 1px solid #00ff88;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .rate-limit-info h2 {
            color: #00ff88;
            font-size: 1.2rem;
        }
        .rate-limit-info .limit {
            font-size: 1.5rem;
            font-weight: bold;
        }
        .section {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .section h2 {
            margin-bottom: 15px;
            color: #00ff88;
            font-size: 1.2rem;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }
        th {
            color: #888;
            font-weight: normal;
            font-size: 0.9rem;
        }
        .status-pending {
            color: #ffaa00;
        }
        .status-forwarding {
            color: #00ff88;
        }
        .status-queued {
            color: #00aaff;
        }
        .status-rate-limited {
            color: #ff4444;
        }
        .status-success {
            color: #00ff88;
        }
        .status-failed {
            color: #ff4444;
        }
        .queue-item {
            background: rgba(0, 170, 255, 0.1);
            padding: 10px 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .live-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            background: #00ff88;
            border-radius: 50%;
            margin-right: 10px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .progress-bar {
            width: 100%;
            height: 8px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #00ff88, #00aaff);
            transition: width 0.5s ease;
        }
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
                <div class="limit" id="concurrencyDisplay">0 / 4</div>
            </div>
            <div style="text-align: right;">
                <h2>Current RPM</h2>
                <div class="limit" id="currentRpmDisplay">0</div>
            </div>
        </div>

<div class="stats-grid">
             <div class="stat-card">
                 <h3>Total Requests</h3>
                 <div class="value" id="totalRequests">0</div>
             </div>
             <div class="stat-card">
                 <h3>Successful</h3>
                 <div class="value" id="successfulRequests">0</div>
             </div>
             <div class="stat-card">
                 <h3>Rate Limited</h3>
                 <div class="value warning" id="rateLimitedRequests">0</div>
             </div>
             <div class="stat-card">
                 <h3>Failed</h3>
                 <div class="value danger" id="failedRequests">0</div>
             </div>
             <div class="stat-card">
                 <h3>Context Pruned</h3>
                 <div class="value" id="contextPrunings">0</div>
             </div>
         </div>

        <div class="section">
            <h2>Request Queue</h2>
            <div id="queueContainer">
                <p style="color: #888;">No requests in queue</p>
            </div>
        </div>

        <div class="section">
            <h2>Recent API Calls</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Method</th>
                        <th>Path</th>
                        <th>Status</th>
                        <th>Wait Time</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody id="requestLogTable">
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let ws = null;
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                console.log('Connected to dashboard');
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            ws.onclose = () => {
                setTimeout(connectWebSocket, 2000);
            };
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
            
            // Update queue
            const queueContainer = document.getElementById('queueContainer');
            if (data.queue.length > 0) {
                queueContainer.innerHTML = data.queue.map(item => 
                    `<div class="queue-item">
                        <span>${item.method} ${item.path}</span>
                        <span class="status-queued">Waiting...</span>
                    </div>`
                ).join('');
            } else {
                queueContainer.innerHTML = '<p style="color: #888;">No requests in queue</p>';
            }
            
            // Update request log table
            const tableBody = document.getElementById('requestLogTable');
            tableBody.innerHTML = data.request_log.slice(-20).reverse().map(req => {
                const statusClass = `status-${req.status.toLowerCase()}`;
                return `<tr>
                    <td>#${req.id}</td>
                    <td>${req.method}</td>
                    <td>${req.path}</td>
                    <td class="${statusClass}">${req.status}</td>
                    <td>${req.wait_time}s</td>
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
    """WebSocket endpoint for real-time dashboard updates."""
    await websocket.accept()
    try:
        while True:
            data = {
                "rate_limit_rpm": RATE_LIMIT_REQUESTS,
                "current_rpm": get_current_rpm(),
                "upstream_timeout": UPSTREAM_TIMEOUT_SECONDS,
                "max_concurrency": MAX_CONCURRENT_REQUESTS,
                "current_inflight": INFLIGHT_REQUESTS,
                "stats": stats,
                "queue": [r.to_dict() for r in queue],
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
    """Intercept every request, rate-limit it, then forward it unchanged.

    The response is streamed through byte-for-byte so SSE/streaming chat
    completions work and compressed (gzip) bodies are passed through intact.
    A 500-second overall timeout covers both the upstream wait and the
    response body delivery; if exceeded the request is counted as failed.
    """
    stats["total_requests"] += 1
    req_info = RequestInfo(request.method, f"/{path}")
    print(f"[proxy] intercepted {request.method} /{path}")

    async with queue_lock:
        req_info.status = "queued"
        queue.append(req_info)

    # Reserve the next evenly-spaced send slot and hold the request here until
    # it is due.  This is what stops bursts from ever reaching NVIDIA.
    wait_start = time.time()
    wait_seconds = await acquire_slot()

    if wait_seconds > UPSTREAM_TIMEOUT_SECONDS:
        # The queue is backed up further than we are willing to wait.
        async with queue_lock:
            if req_info in queue:
                queue.remove(req_info)
        req_info.status = "failed"
        req_info.wait_time = round(wait_seconds, 2)
        req_info.response_time = req_info.wait_time
        stats["failed_requests"] += 1
        request_log.append(req_info)
        print(f"[proxy] queue too deep ({wait_seconds:.1f}s) for {request.method} /{path}")
        return Response(
            content="Rate limit: request waited too long in queue",
            status_code=504,
        )

    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)

    req_info.wait_time = round(time.time() - wait_start, 2)

    await get_concurrency_semaphore().acquire()
    async with inflight_lock:
        global INFLIGHT_REQUESTS
        INFLIGHT_REQUESTS += 1
    async with queue_lock:
        if req_info in queue:
            queue.remove(req_info)
    req_info.status = "forwarding"

    # Read the body and copy headers verbatim, minus hop-by-hop/recomputed ones.
    body = await request.body()
    # Apply context pruning for chat completion requests to reduce token usage
    body = await maybe_prune_chat_context(body, f"/{path}")
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in ("host", "content-length")
    }
    fwd_headers["host"] = "integrate.api.nvidia.com"

    target_url = f"{NVIDIA_BASE_URL}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, read=UPSTREAM_TIMEOUT_SECONDS),
        verify=SSL_CONTEXT,
    )
    request_start = time.time()
    try:
        async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
            # Forward, retrying on an upstream 429 with backoff so a transient
            # rate-limit answer is absorbed here instead of failing the client.
            # Each 429 also pushes every other pending slot back (back_off) so
            # the whole proxy slows down until NVIDIA stops complaining.
            for attempt in range(MAX_429_RETRIES + 1):
                upstream_req = client.build_request(
                    method=request.method,
                    url=target_url,
                    headers=fwd_headers,
                    content=body,
                )
                upstream = await client.send(upstream_req, stream=True)
                if upstream.status_code != 429 or attempt == MAX_429_RETRIES:
                    break
                retry_after = parse_retry_after(upstream.headers) or min(2 ** attempt, 30)
                await upstream.aclose()
                req_info.status = "rate-limited"
                print(f"[proxy] NVIDIA 429 (attempt {attempt + 1}/{MAX_429_RETRIES}); "
                      f"holding {retry_after:.1f}s for {request.method} /{path}")
                await back_off(retry_after)
                await asyncio.sleep(retry_after)
    except TimeoutError:
        await release_slot_and_decrement()
        await client.aclose()
        req_info.status = "failed"
        req_info.response_time = round(time.time() - request_start, 2)
        stats["failed_requests"] += 1
        request_log.append(req_info)
        print(f"[proxy] upstream timed out for {request.method} /{path}")
        return Response(
            content="Upstream NVIDIA API timed out (500s)",
            status_code=504,
        )
    except httpx.RequestError as exc:
        await release_slot_and_decrement()
        await client.aclose()
        req_info.status = "failed"
        req_info.response_time = round(time.time() - request_start, 2)
        stats["failed_requests"] += 1
        request_log.append(req_info)
        print(f"[proxy] upstream error: {exc}")
        return Response(
            content=f"Error connecting to NVIDIA backend: {exc}",
            status_code=502,
        )

    # Non-2xx from NVIDIA: read the (small) error body in full so we can log
    # EXACTLY why it was rejected.  This is the only place NVIDIA tells us which
    # limit was hit (RPM vs tokens-per-minute vs concurrency vs credits), so we
    # surface its status, every rate-limit header, and the body to the console.
    if upstream.status_code >= 400:
        await release_slot_and_decrement()
        err_body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()

        rl_headers = {
            k: v for k, v in upstream.headers.items()
            if "ratelimit" in k.lower() or k.lower() in ("retry-after", "x-request-id", "nvcf-reqid")
        }
        snippet = err_body[:800].decode("utf-8", "replace")
        print(
            f"[proxy] NVIDIA {upstream.status_code} for {request.method} /{path}\n"
            f"        rate-limit headers: {rl_headers or '(none)'}\n"
            f"        body: {snippet}"
        )

        if upstream.status_code == 429:
            req_info.status = "rate-limited"
            stats["rate_limited_requests"] += 1
        else:
            req_info.status = "failed"
        stats["failed_requests"] += 1
        req_info.response_time = round(time.time() - request_start, 2)
        request_log.append(req_info)

        resp_headers = {
            k: v for k, v in upstream.headers.items()
            if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"
        }
        return Response(
            content=err_body,
            status_code=upstream.status_code,
            headers=resp_headers,
        )

    # Forward status + headers as-is (keep content-encoding; drop content-length
    # and hop-by-hop since we re-emit the body as a chunked stream).
    resp_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"
    }

    # Defer final success/fail accounting until the body has been fully
    # streamed to the caller.  If the relay times out or errors we flip
    # the status to "failed" and adjust stats.
    finalised = False

    async def relay():
        nonlocal finalised
        try:
            async with asyncio.timeout(UPSTREAM_TIMEOUT_SECONDS):
                async for chunk in upstream.aiter_raw():
                    yield chunk
            # Body fully streamed — mark success/fail now.
            if upstream.status_code < 400:
                req_info.status = "success"
                stats["successful_requests"] += 1
            else:
                req_info.status = "failed"
                stats["failed_requests"] += 1
            req_info.response_time = round(time.time() - request_start, 2)
            finalised = True
        except TimeoutError:
            req_info.status = "failed"
            req_info.response_time = round(time.time() - request_start, 2)
            stats["failed_requests"] += 1
            finalised = True
            print(f"[proxy] upstream body stream timed out for {request.method} /{path}")
        except Exception as exc:
            req_info.status = "failed"
            req_info.response_time = round(time.time() - request_start, 2)
            stats["failed_requests"] += 1
            finalised = True
            print(f"[proxy] upstream body stream error for {request.method} /{path}: "
                  f"{type(exc).__name__}: {exc}")
        finally:
            if not finalised:
                req_info.status = "failed"
                req_info.response_time = round(time.time() - request_start, 2)
                stats["failed_requests"] += 1
            request_log.append(req_info)
            await upstream.aclose()
            await client.aclose()
            await release_slot_and_decrement()

    return StreamingResponse(
        relay(),
        status_code=upstream.status_code,
        headers=resp_headers,
    )


def reset_stats():
    """Reset all stats and logs for a fresh start."""
    global stats
    request_log.clear()
    queue.clear()
    stats = {
        "total_requests": 0,
        "successful_requests": 0,
        "rate_limited_requests": 0,
        "failed_requests": 0,
        "context_prunings": 0,
        "start_time": datetime.now().isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="NVIDIA API Rate Limiting Proxy")
    parser.add_argument(
        "--rpm", "-r",
        type=int,
        default=40,
        help="Maximum requests per minute (default: 40)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8000,
        help="Port to run the proxy on (default: 8000)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=500,
        help="Upstream timeout in seconds (default: 500)"
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=4,
        help="Maximum concurrent upstream requests (default: 4)"
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Force sequential processing (max concurrency = 1)"
    )
    parser.add_argument(
        "--max-context-tokens",
        type=int,
        default=8000,
        help="Maximum tokens to allow in chat context (default: 8000)"
    )
    parser.add_argument(
        "--keep-last-messages",
        type=int,
        default=10,
        help="Number of recent messages to always keep in chat context (default: 10)"
    )
    parser.add_argument(
        "--context-pruning",
        dest="context_pruning",
        action="store_true",
        help="Enable context pruning for chat requests (default: True)"
    )
    parser.add_argument(
        "--no-context-pruning",
        dest="context_pruning",
        action="store_false",
        help="Disable context pruning for chat requests"
    )
    parser.set_defaults(context_pruning=True)

    args = parser.parse_args()

    global RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW_SECONDS, UPSTREAM_TIMEOUT_SECONDS, MAX_CONCURRENT_REQUESTS
    global ENABLE_CONTEXT_PRUNING, MAX_CONTEXT_TOKENS, KEEP_LAST_MESSAGES

    RATE_LIMIT_REQUESTS = args.rpm
    RATE_LIMIT_WINDOW_SECONDS = 60
    UPSTREAM_TIMEOUT_SECONDS = args.timeout
    if args.sequential:
        MAX_CONCURRENT_REQUESTS = 1
    else:
        MAX_CONCURRENT_REQUESTS = args.concurrency
    MAX_CONTEXT_TOKENS = args.max_context_tokens
    KEEP_LAST_MESSAGES = args.keep_last_messages
    ENABLE_CONTEXT_PRUNING = args.context_pruning

    reset_stats()

    print(f"\n{'='*60}")
    print(f"  NVIDIA API Rate Limiting Proxy")
    print(f"{'='*60}")
    print(f"  Dashboard:    http://{args.host}:{args.port}/")
    print(f"  Proxy URL:    http://{args.host}:{args.port}/v1")
    print(f"  Rate Limit:   {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW_SECONDS}s")
    print(f"  Spacing:      1 request every {RATE_LIMIT_WINDOW_SECONDS / max(1, RATE_LIMIT_REQUESTS):.2f}s")
    if args.sequential:
        concurrency_note = " (sequential mode)"
    else:
        concurrency_note = ""
    print(f"  Concurrency:  {MAX_CONCURRENT_REQUESTS} simultaneous upstream requests{concurrency_note}")
    print(f"  Timeout:      {UPSTREAM_TIMEOUT_SECONDS}s")
    print(f"{'='*60}\n")
    
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()