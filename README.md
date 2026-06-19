# NVIDIA API Rate Limiting Proxy

A lightweight reverse proxy that intercepts NVIDIA API calls and applies **sliding-window RPM enforcement + sequential holdout pacing** to stay within NVIDIA rate limits. Includes a real-time WebSocket dashboard.

## Features

- **Sliding-Window RPM Enforcement**: Enforces a true 60s rolling window (default 40 RPM to match NVIDIA free tier). The `--rpm` flag actually controls the limit — not just cosmetic
- **Completion-Based Holdout**: Secondary pacing engine that enforces a configurable cooldown gap (default 1.67s) between request completions, smoothing burst edges
- **Single-Stream Concurrency**: One request at a time through the upstream to guarantee pacing integrity
- **429 Retry Logic**: Exponential backoff (up to 5 retries, capped at 30s) on NVIDIA rate-limit responses
- **Context Pruning (Opt-in)**: Optionally prune chat history to stay under token limits — opt-in via `--no-context-pruning` (default: on with 160K ceiling)
- **Real-time Dashboard**: Dark-themed web UI with live stats, queue, and request log via WebSocket at `http://127.0.0.1:8000/`
- **Request Queuing**: Requests wait in a FIFO queue behind the sequential lock
- **Live Statistics**: Track total, successful, rate-limited, failed, and context-pruned requests
- **Client Disconnect Guard**: Graceful handling of early client disconnects without blocking the pacing engine

## Installation

```bash
pip install fastapi uvicorn httpx truststore certifi python-multipart websockets
```

> `truststore` lets the proxy verify TLS using the OS (Windows) trust store. This
> is required on machines where antivirus/corporate software (e.g. **AVG Web/Mail
> Shield**) intercepts HTTPS and re-signs certs with a private root CA that
> OpenSSL/certifi reject.

## Usage

### Basic Usage (Default: 40 RPM, 1.67s cooldown)

```bash
python nvidia_proxy.py
```

### Custom Rate Limit (match NVIDIA free tier)

```bash
python nvidia_proxy.py --rpm 40
```

### Full Options

```bash
python nvidia_proxy.py --rpm 40 --port 8000 --host 127.0.0.1 --timeout 500 --cooldown 1.0
```

## Context Pruning

Context pruning is **enabled by default** (ceiling: 160K tokens) to keep chat history manageable. Disable it when message role integrity is critical:

```bash
# Disable context pruning
python nvidia_proxy.py --no-context-pruning
```

> **Why disable?** Aggressive truncation can split `assistant` tool-call messages from their corresponding `tool` responses, causing NVIDIA to reject the request with `Unexpected role 'tool' after role 'system'`.

### Command-Line Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--rpm` | `-r` | 40 | Max requests per rolling 60s window |
| `--port` | `-p` | 8000 | Port to run the proxy on |
| `--host` | | 127.0.0.1 | Host to bind to |
| `--timeout` | `-t` | 500 | Upstream timeout in seconds |
| `--cooldown` | `-c` | 1.67 | Post-completion holdout buffer (seconds) |
| `--max-context-tokens` | | 160000 | Token ceiling for context pruning |
| `--keep-last-messages` | | 30 | Messages to preserve when pruning |
| `--no-context-pruning` | | | Disable context pruning |

## Configuration

### OpenCode Configuration

If you authenticated NVIDIA via `opencode auth login`, OpenCode already has a
built-in `nvidia` provider pointing at `https://integrate.api.nvidia.com/v1`. You
only need to **override its `baseURL`** to send traffic through the proxy — models
and the API key are inherited. Edit `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "nvidia": {
      "options": {
        "baseURL": "http://127.0.0.1:8000/v1"
      }
    }
  }
}
```

> **Restart OpenCode after editing this file** so the new baseURL takes effect.
> Verify interception by watching the proxy console — every forwarded call prints
> `[proxy] intercepted POST /v1/chat/completions`.

## Dashboard

Access the dashboard at: **http://127.0.0.1:8000/**

The dashboard displays:
- Rate limit configuration and current RPM
- Concurrency status (e.g., `2 / 3` concurrent requests)
- Total, successful, rate-limited, failed, and context-pruned requests
- Request queue with waiting requests
- Recent API calls with status, wait time, and timestamps

## How It Works

1. **Intercept**: OpenCode sends requests to the local proxy instead of directly to NVIDIA
2. **Queue**: Requests enter a FIFO queue behind the sequential lock
3. **RPM Gate**: `enforce_rate_limit()` checks a sliding window of forwarded request starts — if `RATE_LIMIT_REQUESTS` starts exist in the last 60s, it delays until a slot opens
4. **Holdout Gate**: `enforce_completion_holdout()` checks the elapsed time since the last response stream closed — if less than `COOLDOWN_BUFFER` (default 1.67s), it sleeps the difference. This smooths burst edges
5. **Forward**: Approved requests stream to the NVIDIA API with 429 retry logic (exponential backoff, up to 5 retries, 30s cap)
6. **Track Completion**: When the upstream response stream fully closes, the completion timestamp is recorded, enabling the next request's holdout calculation
7. **Dashboard**: All stats are pushed to the browser dashboard via WebSocket every second

### Architecture

```
OpenCode → Queue → Sequential Lock → RPM Window → Holdout Gate → NVIDIA API
                                          ↓
                               deque of start timestamps
                                    (60s rolling)
```

## Recommended Settings

```bash
# Default (40 RPM, matches NVIDIA free tier)
python nvidia_proxy.py

# Conservative (lower RPM for safety margin)
python nvidia_proxy.py --rpm 30 --cooldown 2.0

# Tighter cooldown + max RPM (higher 429 risk)
python nvidia_proxy.py --rpm 40 --cooldown 1.0
```

## Quick Setup (Windows)

Run the automated setup script as **Administrator**:

```bat
initial_setup.bat
```

This will:
1. Check/install Python dependencies
2. Test and fix certificate issues (auto-detects AVG, other AVs)
3. Configure OpenCode to use the proxy
4. Start the proxy service
5. Create a log file (`initial_setup.log`) for debugging

**Logs are reset each run.** Check `initial_setup.log` if something fails.

---

## Manual Certificate Handling

If the proxy fails with SSL errors (e.g., `SSL: CERTIFICATE_VERIFY_FAILED`), your antivirus is intercepting HTTPS traffic. You need to add its root certificate to the Windows trust store.

### Auto-Detect (Recommended)

Run `initial_setup.bat` as Administrator — it will attempt to auto-detect and import common antivirus certificates (AVG, etc.).

### Manual Steps (AVG example)

1. **Export the AVG root certificate:**
   - Open AVG Settings → General → Manage exceptions
   - Or search Windows for "AVG certificate export"
   - Save as `.crt` file (e.g., `avg_root_ca.crt`)

2. **Import to Windows Root store:**
   ```cmd
   :: Run as Administrator
   certutil -addstore Root "C:\Users\YourUsername\avg_root_ca.crt"
   ```

3. **Verify it works:**
   ```cmd
   python -c "import httpx, truststore, ssl; ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT); print(httpx.get('https://integrate.api.nvidia.com/', verify=ctx).status_code)"
   ```

### Common Certificate Locations

| Antivirus | Possible Certificate Locations |
|-----------|-------------------------------|
| AVG | `C:\Program Files\AVG\`, `C:\ProgramData\AVG\` |
| Avast | `C:\Program Files\Avast\`, `C:\ProgramData\Avast\` |
| Kaspersky | `C:\Program Files\Kaspersky Lab\` |
| Norton | `C:\Program Files\Norton\` |

Search for `.crt` or `.cer` files in the antivirus installation directory.

---

## Manual Configuration (Without Script)

### 1. Configure OpenCode

Edit `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "nvidia": {
      "options": {
        "baseURL": "http://127.0.0.1:8000/v1"
      }
    }
  }
}
```

Restart OpenCode GUI for changes to take effect.

### 2. Start Proxy Manually

```cmd
:: Install dependencies
pip install fastapi uvicorn httpx truststore websockets

:: Run proxy (default ~36 RPM, sequential)
python nvidia_proxy.py

:: Or with custom settings
python nvidia_proxy.py --rpm 30 --port 8000 --host 127.0.0.1 --cooldown 2.0
```

### 3. Verify Setup

- Dashboard: http://127.0.0.1:8000/
- Test API: http://127.0.0.1:8000/v1/models

---

## Changelog

### v1.2 — Real RPM Sliding-Window Enforcement

- **New**: `--rpm` flag now enforces a real 60s sliding window instead of being cosmetic. Requests are delayed when the window is full.
- **New**: `--cooldown` flag made configurable via CLI (was hardcoded).
- **Changed**: Default RPM from 30 → 40 to match NVIDIA free tier limit.

### v1.1 — Pacing Engine Fixes

- **Fixed**: Pacing holdout now uses elapsed-time checks (`now - last_completion_time`) instead of future-wall-clock projection. Previously, the relay's completion handler would overwrite the forward-projected holdout time, collapsing the cooldown gap after long requests.
- **Fixed**: `stats["rate_limited_requests"]` counter is now incremented on each 429 retry (was stuck at 0).
- **Fixed**: `estimate_tokens()` now handles multi-modal content arrays (`content` as a list, not just a string).
- **Fixed**: `response_time` is now populated for all request outcomes (success, failure, disconnect).
- **Removed**: Dead `finalised` variable, dead `RATE_LIMIT_WINDOW_SECONDS` constant.

---

## License

MIT