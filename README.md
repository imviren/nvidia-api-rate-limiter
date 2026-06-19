# NVIDIA API Rate Limiting Proxy

A lightweight reverse proxy that intercepts NVIDIA API calls and applies **completion-based pacing** to stay within NVIDIA rate limits. Includes a real-time WebSocket dashboard.

## Features

- **Completion-Based Pacing**: Sliding-window tracker that paces based on when upstream response streams _finish_ (not when requests are sent), preventing 429 drops under multi-channel conditions
- **Multi-Track Concurrency**: Async semaphore for up to 3 (configurable) parallel streaming requests
- **429 Retry Logic**: Exponential backoff and automatic retry on NVIDIA rate-limit responses
- **Context Pruning (Opt-in)**: Optionally prune chat history to stay under token limits — disabled by default to preserve message role hierarchies for agentic workflows
- **Real-time Dashboard**: Dark-themed web UI with live stats, queue, and request log via WebSocket at `http://127.0.0.1:8000/`
- **Request Queuing**: Requests wait in queue when the completion ceiling is reached
- **Live Statistics**: Track total, successful, rate-limited, failed, and context-pruned requests

## Installation

```bash
pip install fastapi uvicorn httpx truststore certifi python-multipart websockets
```

> `truststore` lets the proxy verify TLS using the OS (Windows) trust store. This
> is required on machines where antivirus/corporate software (e.g. **AVG Web/Mail
> Shield**) intercepts HTTPS and re-signs certs with a private root CA that
> OpenSSL/certifi reject.

## Usage

### Basic Usage (Default: 35 RPM, 3 concurrent)

```bash
python nvidia_proxy.py
```

### Custom Rate Limit & Concurrency

```bash
python nvidia_proxy.py --rpm 20 --concurrency 2
```

### Full Options

```bash
python nvidia_proxy.py --rpm 30 --port 8000 --host 127.0.0.1 --timeout 500
```

## Context Pruning

Context pruning is **disabled by default** to prevent message role corruption in agentic workflows (e.g., OpenCode). Enable it explicitly when needed:

```bash
# Enable context pruning
python nvidia_proxy.py --context-pruning
```

> **Why disabled by default?** Aggressive truncation can split `assistant` tool-call messages from their corresponding `tool` responses, causing NVIDIA to reject the request with `Unexpected role 'tool' after role 'system'`. For long-running agent sessions, rely on the completion-based pacing engine instead.

### Command-Line Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--rpm` | `-r` | 35 | Maximum completed requests per minute |
| `--port` | `-p` | 8000 | Port to run the proxy on |
| `--host` | | 127.0.0.1 | Host to bind to |
| `--timeout` | `-t` | 500 | Upstream timeout in seconds |
| `--concurrency` | `-c` | 3 | Maximum concurrent in-flight requests |
| `--no-context-pruning` | | (default) | Disable context pruning (preserves message integrity) |

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
2. **Queue**: Requests enter a queue and acquire a concurrency semaphore slot
3. **Pace**: The proxy checks its sliding completion window — if the rolling 60s count of completed + in-flight requests hits the limit, the pipeline delays until a slot opens
4. **Forward**: Approved requests stream to the NVIDIA API
5. **Track Completion**: When the upstream response stream fully closes, the completion timestamp is recorded, freeing pacing capacity
6. **Dashboard**: All stats are pushed to the browser dashboard via WebSocket every second

### Architecture

```
OpenCode → Queue → Semaphore → Completion Window Gate → NVIDIA API
                ↕                                      ↓ (on stream close)
           Dashboard ←── WebSocket ←── Stats + Completion Log
```

## Recommended Settings

```bash
# Conservative defaults (35 RPM, 3 concurrent)
python nvidia_proxy.py

# Lower throughput for safety
python nvidia_proxy.py --rpm 20 --concurrency 2

# Max safety (single file, low RPM)
python nvidia_proxy.py --rpm 15 --concurrency 1
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

:: Run proxy (default 35 RPM, 3 concurrent)
python nvidia_proxy.py

:: Or with custom settings
python nvidia_proxy.py --rpm 20 --port 8000 --host 127.0.0.1
```

### 3. Verify Setup

- Dashboard: http://127.0.0.1:8000/
- Test API: http://127.0.0.1:8000/v1/models

---

## License

MIT