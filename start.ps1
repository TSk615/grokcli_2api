# Start grokcli-2api on Windows and open the admin web UI
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Copy-Item ".env.example" ".env"
        Write-Host "Created .env from .env.example — edit secrets as needed."
    }
}
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) { return }
        $i = $line.IndexOf("=")
        if ($i -lt 1) { return }
        $name = $line.Substring(0, $i).Trim()
        $val = $line.Substring($i + 1).Trim()
        if ($val.StartsWith('"') -and $val.EndsWith('"') -and $val.Length -ge 2) {
            $val = $val.Substring(1, $val.Length - 2)
        } elseif ($val.StartsWith("'") -and $val.EndsWith("'") -and $val.Length -ge 2) {
            $val = $val.Substring(1, $val.Length - 2)
        }
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            Set-Item -Path "Env:$name" -Value $val
        }
    }
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "python not found in PATH. Install Python 3.10+ first."
}

python -c "import fastapi, uvicorn, httpx" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing dependencies..."
    python -m pip install -r requirements.txt
}

python -c "import curl_cffi, requests" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing remaining dependencies..."
    python -m pip install -r requirements.txt
}


if (-not $env:GROK2API_OPEN_BROWSER) { $env:GROK2API_OPEN_BROWSER = "1" }
if (-not $env:GROK2API_HOST) { $env:GROK2API_HOST = "127.0.0.1" }
if (-not $env:GROK2API_PORT) { $env:GROK2API_PORT = "3000" }
if (-not $env:GROK2API_REASONING_COMPAT) { $env:GROK2API_REASONING_COMPAT = "off" }

$port = $env:GROK2API_PORT
Write-Host "Starting grokcli-2api..."
Write-Host "  Admin: http://127.0.0.1:$port/admin"
Write-Host "  (browser opens automatically unless GROK2API_OPEN_BROWSER=0)"

python app.py
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] service exited with code $LASTEXITCODE"
    Write-Host "Common fixes:"
    Write-Host "  1) python -m pip install -r requirements.txt"
    exit $LASTEXITCODE
}
