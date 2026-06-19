# NVIDIA API Rate Limiting Proxy

A lightweight reverse proxy that intercepts NVIDIA API calls, applies configurable rate limiting, and provides a real-time dashboard.

## Features

- **Rate Limiting**: Configurable requests per minute (RPM) via command-line arguments
- **Concurrency Limiting**: Configurable maximum concurrent upstream requests to stay within NVIDIA limits
- **Sequential Processing Mode**: Optional strict sequential processing (1 request at a time) for maximum safety
- **Context Pruning & Summarization**: Automatically prunes chat completion messages to stay under token-per-minute (TPM) limits
- **Real-time Dashboard**: Modern web UI showing API call details, queue status, and current RPM
- **Request Queuing**: Requests wait in queue when rate limit is reached
- **Live Statistics**: Track total, successful, rate-limited, failed, and context-pruned requests
- **WebSocket Updates**: Dashboard updates in real-time every second

## Installation

```bash
pip install fastapi uvicorn httpx truststore certifi python-multipart websockets
```

> `truststore` lets the proxy verify TLS using the OS (Windows) trust store. This
> is required on machines where antivirus/corporate software (e.g. **AVG Web/Mail
> Shield**) intercepts HTTPS and re-signs certs with a private root CA that
> OpenSSL/certifi reject.

## Usage

### Basic Usage (Default: 40 RPM)

```bash
python nvidia_proxy.py
```

### Custom Rate Limit

```bash
python nvidia_proxy.py --rpm 20
```

### Full Options

```bash
python nvidia_proxy.py --rpm 30 --port 8000 --host 127.0.0.1
```

## Context Pruning (for Long Conversations)

When using long-running conversations or agentic workflows, token count can grow rapidly. The proxy now automatically prunes chat context to stay within limits:

- **Sliding window**: Keeps the last N messages (configurable)
- **Smart summarization**: Summarizes older messages instead of dropping them
- **Token-aware**: Uses rough token estimation to stay under limits

```bash
# Enable context pruning (default)
python nvidia_proxy.py --context-pruning

# Disable context pruning
python nvidia_proxy.py --no-context-pruning

# Adjust context window
python nvidia_proxy.py --max-context-tokens 6000 --keep-last-messages 8
```

## Concurrency & Sequential Modes

By default, the proxy allows up to 4 concurrent upstream requests. You can adjust this:

```bash
# Limit to 2 concurrent requests (default: 4)
python nvidia_proxy.py --concurrency 2

# Force strict sequential processing (one at a time)
python nvidia_proxy.py --sequential
```

### Command-Line Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--rpm` | `-r` | 40 | Maximum requests per minute |
| `--port` | `-p` | 8000 | Port to run the proxy on |
| `--host` | | 127.0.0.1 | Host to bind to |
| `--timeout` | `-t` | 500 | Upstream timeout in seconds |
| `--concurrency` | `-c` | 4 | Maximum concurrent upstream requests |
| `--sequential` | | | Force sequential processing (max concurrency = 1) |
| `--max-context-tokens` | | 8000 | Maximum tokens to allow in chat context |
| `--keep-last-messages` | | 10 | Number of recent messages to always keep in chat context |
| `--context-pruning` | | (enabled) | Enable context pruning for chat requests |
| `--no-context-pruning` | | | Disable context pruning for chat requests |

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
- Concurrency status (e.g., `2 / 4` concurrent requests)
- Total, successful, rate-limited, failed, and context-pruned requests
- Request queue with waiting requests
- Recent API calls with status, wait time, and timestamps

## How It Works

1. **Intercept**: OpenCode sends requests to the local proxy instead of directly to NVIDIA
2. **Queue**: Requests are added to a queue
3. **Rate Limit**: Token bucket algorithm controls request flow
4. **Context Prune**: Chat completion requests have large contexts pruned/summarized to respect TPM limits
5. **Forward**: Approved requests are forwarded to NVIDIA API
6. **Response**: NVIDIA's response is returned to OpenCode
7. **Track**: All requests are logged and displayed on the dashboard

## Recommended Settings for Long-Running Workflows

To avoid 429 errors during long conversations or agentic workflows:

```bash
# Conservative settings for long conversations
python nvidia_proxy.py --rpm 20 --concurrency 2 --max-context-tokens 6000 --keep-last-messages 8
```

For maximum safety (strictly sequential):
```bash
python nvidia_proxy.py --sequential --rpm 15 --max-context-tokens 6000
```

## Architecture

```
OpenCode → Local Proxy (Rate Limit + Concurrency + Context Pruning) → NVIDIA API
              ↓
         Dashboard (Real-time UI)
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

:: Run proxy (default 40 RPM)
python nvidia_proxy.py

:: Or with custom settings
python nvidia_proxy.py --rpm 20 --port 8000 --host 127.0.0.1

:: For long conversations with context pruning
python nvidia_proxy.py --rpm 20 --concurrency 2 --max-context-tokens 6000 --keep-last-messages 8
```

### 3. Verify Setup

- Dashboard: http://127.0.0.1:8000/
- Test API: http://127.0.0.1:8000/v1/models

---

## License

MIT