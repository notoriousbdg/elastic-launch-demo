#!/bin/bash
# Start the NOVA-7 app
PIDS=$(ps aux | grep "uvicorn app.main:app" | grep -v grep | awk '{print $2}')
if [ -n "$PIDS" ]; then
    echo "Already running (PIDs: $(echo $PIDS | tr '\n' ' '))"
    exit 0
fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ ! -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    echo "Creating .venv..."
    python3 -m venv "$SCRIPT_DIR/.venv"
    source "$SCRIPT_DIR/.venv/bin/activate"
    echo "Installing dependencies..."
    pip install -q -r "$SCRIPT_DIR/requirements.txt"
else
    source "$SCRIPT_DIR/.venv/bin/activate"
fi
PYTHON="$SCRIPT_DIR/.venv/bin/python3"
nohup "$PYTHON" -m uvicorn app.main:app --host 0.0.0.0 --port 8080 > /tmp/nova7.log 2>&1 &
sleep 2
PIDS=$(ps aux | grep "uvicorn app.main:app" | grep -v grep | awk '{print $2}')
if [ -n "$PIDS" ]; then
    echo "Started (PIDs: $(echo $PIDS | tr '\n' ' '))"
else
    echo "Failed to start — check /tmp/nova7.log"
    exit 1
fi
