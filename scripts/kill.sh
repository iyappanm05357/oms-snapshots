#!/bin/bash

# Script to stop the Aeron Archive

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$ROOT_DIR/.aeron_archive.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo "Stopping Aeron Archive (PID: $PID)..."
    kill $PID 2>/dev/null || true
    rm -f "$PID_FILE"
    
    # Wait for process to exit
    sleep 1
    
    # Force kill if still running
    if ps -p $PID > /dev/null 2>&1; then
        echo "Force killing..."
        kill -9 $PID 2>/dev/null || true
    fi
    
    echo "Archive stopped."
else
    echo "No archive PID file found. Archive may not be running."
    
    # Try to find and kill any running archive processes
    PIDS=$(ps aux | grep "[i]o.aeron.archive.ArchivingMediaDriver" | awk '{print $2}')
    if [ ! -z "$PIDS" ]; then
        echo "Found archive processes: $PIDS"
        echo "$PIDS" | xargs kill 2>/dev/null || true
        sleep 1
        echo "$PIDS" | xargs kill -9 2>/dev/null || true
        echo "Killed archive processes."
    fi
fi

# Clean up directories
echo "Cleaning up directories..."
rm -rf /tmp/rusteron_aeron /tmp/rusteron_archive

echo "Done."