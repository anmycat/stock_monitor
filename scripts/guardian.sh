#!/bin/bash

APP_DIR="/stock_monitor"
VENV_DIR="$APP_DIR/.venv"
PID_FILE="/stock_monitor/logs/guardian.pid"

start() {
    echo "Starting Guardian..."
    cd "$APP_DIR"
    source "$VENV_DIR/bin/activate"
    python /stock_monitor/guardian.py
    echo "Guardian started"
}

stop() {
    echo "Stopping Guardian..."
    if [ -f "$PID_FILE" ]; then
        kill $(cat "$PID_FILE")
        rm -f "$PID_FILE"
        echo "Guardian stopped"
    else
        pkill -f "guardian.py"
        echo "Guardian killed"
    fi
}

restart() {
    stop
    sleep 2
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p $PID > /dev/null 2>&1; then
            echo "Guardian is running (PID: $PID)"
        else
            echo "Guardian is not running (stale PID file)"
        fi
    else
        echo "Guardian is not running"
    fi
}

health() {
    PORT=${HEALTH_PORT:-8080}
    curl -s http://localhost:$PORT/health | python -m json.tool
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    health) health ;;
    *) echo "Usage: $0 {start|stop|restart|status|health}" ;;
esac
