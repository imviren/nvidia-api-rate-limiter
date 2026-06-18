@echo off
setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "LOG_FILE=%SCRIPT_DIR%initial_setup.log"
set "PROXY_SCRIPT=nvidia_proxy.py"
set "OPENCODE_CONFIG=%USERPROFILE%\.config\opencode\opencode.jsonc"
set "PROXY_PORT=8000"
set "PROXY_HOST=127.0.0.1"

net session >nul 2>&1
if %errorLevel% neq 0 (
    echo Requesting administrator privileges...
    powershell -Command "Start-Process cmd -ArgumentList '/c cd /d %~dp0 && %~nx0' -Verb RunAs"
    exit /b
)

(
echo ==============================================
echo   NVIDIA Proxy Initial Setup
echo ==============================================
echo.

echo [1/5] Checking Python dependencies...
python -c "import fastapi, httpx, uvicorn, truststore, websockets" 2>nul
if errorlevel neq 0 (
    echo [INFO] Installing dependencies...
    pip install fastapi uvicorn httpx truststore websockets --quiet
    if errorlevel neq 0 (
        echo [ERROR] Failed to install dependencies.
        exit /b 1
    )
)
echo [OK] Dependencies verified.
echo.

echo [2/5] Testing NVIDIA API connectivity...
python -c "import httpx, truststore, ssl; ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT); client = httpx.Client(verify=ctx, timeout=10); client.get('https://integrate.api.nvidia.com/'); print('OK')" > temp_cert_check.txt 2>&1
set /p CERT_CHECK=<temp_cert_check.txt
del temp_cert_check.txt >nul 2>&1

if "!CERT_CHECK!"=="OK" (
    echo [OK] Certificate verification passed.
) else (
    echo [WARNING] Certificate issue detected: !CERT_CHECK!
    echo.
    echo This usually means an antivirus intercepts HTTPS.
    echo Attempting to import AVG root certificate...
    echo.

    set "AVG_CERT_FOUND=0"
    for /r "C:\Program Files\AVG" %%f in (*.crt *.cer) do (
        if "!AVG_CERT_FOUND!"=="0" (
            echo Found: %%f
            certutil -addstore Root "%%f" >nul 2>&1
            if not errorlevel neq 0 (
                echo [OK] Imported %%f to Root store.
                set "AVG_CERT_FOUND=1"
            )
        )
    )

    if exist "C:\ProgramData\AVG\*" (
        for /r "C:\ProgramData\AVG" %%f in (*.crt *.cer) do (
            if "!AVG_CERT_FOUND!"=="0" (
                echo Found: %%f
                certutil -addstore Root "%%f" >nul 2>&1
                if not errorlevel neq 0 (
                    echo [OK] Imported %%f to Root store.
                    set "AVG_CERT_FOUND=1"
                )
            )
        )
    )

    if "!AVG_CERT_FOUND!"=="0" (
        echo.
        echo [WARNING] Could not auto-detect AVG certificate.
        echo Please manually export the AVG root CA and save it as:
        echo   C:\Users\YourUser\avg_root_ca.crt
        echo Then run: certutil -addstore Root C:\Users\YourUser\avg_root_ca.crt
    )
)
echo.

echo [3/5] Configuring OpenCode to use proxy...
if exist "%OPENCODE_CONFIG%" (
    echo [INFO] Found existing opencode.jsonc
    copy /Y "%OPENCODE_CONFIG%" "%OPENCODE_CONFIG%.backup" >nul 2>&1
    echo [OK] Backed up to opencode.jsonc.backup

    findstr /C:"\"nvidia\":" "%OPENCODE_CONFIG%" >nul 2>&1
    if %errorLevel% equ 0 (
        echo [INFO] NVIDIA provider exists, updating baseURL...
        powershell -Command "(Get-Content '%OPENCODE_CONFIG%') -replace '\"baseURL\":\s*\"[^\"]*\"', '\"baseURL\": \"http://%PROXY_HOST%:%PROXY_PORT%/v1\"' | Set-Content '%OPENCODE_CONFIG%'"
    ) else (
        echo [INFO] Adding NVIDIA provider to OpenCode config...
        powershell -Command "$c = Get-Content '%OPENCODE_CONFIG%' -Raw; $nvidia = @{\"nvidia\" = @{\"options\" = @{\"baseURL\" = \"http://%PROXY_HOST%:%PROXY_PORT%/v1\"}}}; if ($c -match '\"provider\":\s*\{{') { $c = $c -replace '(\"provider\":\s*\{{)', ('$1' + [Environment]::NewLine + '    \"nvidia\": { \"options\": { \"baseURL\": \"http://%PROXY_HOST%:%PROXY_PORT%/v1\" } },') } else { $c = '{\"provider\": ' + ($nvidia | ConvertTo-Json -Depth 3) + '}' }; Set-Content '%OPENCODE_CONFIG%' $c"
    )
) else (
    echo [INFO] Creating new opencode.jsonc with proxy config...
    (
        echo {
        echo   "$schema": "https://opencode.ai/config.json",
        echo   "provider": {
        echo     "nvidia": {
        echo       "options": {
        echo         "baseURL": "http://%PROXY_HOST%:%PROXY_PORT%/v1"
        echo       }
        echo     }
        echo   }
        echo }
    ) > "%OPENCODE_CONFIG%"
)
echo [OK] OpenCode configured to use proxy at http://%PROXY_HOST%:%PROXY_PORT%/v1
echo.

echo [4/5] Starting NVIDIA proxy...
echo.

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%PROXY_PORT% ^| findstr LISTENING') do (
    echo [INFO] Stopping existing process on port %PROXY_PORT%...
    taskkill /F /PID %%a >nul 2>&1
)

start "NVIDIA_Proxy" cmd /c "cd /d "%SCRIPT_DIR%" && python "%PROXY_SCRIPT%" --port %PROXY_PORT% --host %PROXY_HOST% --timeout 500"

echo Waiting for proxy to start...
for /l %%i in (1,1,10) do (
    curl -s -o NUL -w "" http://%PROXY_HOST%:%PROXY_PORT%/ 2>nul
    if not errorlevel neq 0 goto :proxy_started
    timeout /t 1 /nobreak >nul
)
:proxy_started

echo [OK] Proxy started.
echo.

echo [5/5] Setup Complete!
echo.
echo ==============================================
echo   Dashboard:    http://%PROXY_HOST%:%PROXY_PORT%/
echo   Proxy URL:    http://%PROXY_HOST%:%PROXY_PORT%/v1
echo ==============================================
echo.
echo [IMPORTANT] Please restart OpenCode GUI for changes to take effect.
echo.
echo Log saved to: %LOG_FILE%
echo Press any key to exit...
) > "%LOG_FILE%" 2>&1

pause >nul