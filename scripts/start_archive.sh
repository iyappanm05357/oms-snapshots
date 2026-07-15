#!/bin/bash
set -e

# Script to start Aeron Archive for external replay example

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Directories for Aeron and Archive
export AERON_DIR="/tmp/rusteron_aeron"
export ARCHIVE_DIR="/tmp/rusteron_archive"

# Clean up any existing data
echo "Cleaning up existing directories..."
rm -rf "$AERON_DIR" "$ARCHIVE_DIR"
mkdir -p "$AERON_DIR" "$ARCHIVE_DIR"

# Download Aeron JAR if not present
JAR_DIR="$ROOT_DIR/lib"
JAR_FILE="$JAR_DIR/aeron-all-1.46.7.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Downloading Aeron JAR..."
    mkdir -p "$JAR_DIR"
    curl -L -o "$JAR_FILE" \
        "https://repo1.maven.org/maven2/io/aeron/aeron-all/1.46.7/aeron-all-1.46.7.jar"
fi

echo "Starting Aeron Archive..."
echo "  Aeron dir: $AERON_DIR"
echo "  Archive dir: $ARCHIVE_DIR"
echo ""

# Start archive and save PID
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
     --add-opens java.base/java.util.zip=ALL-UNNAMED \
     -Daeron.dir="$AERON_DIR" \
     -Daeron.archive.dir="$ARCHIVE_DIR" \
     -Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010 \
     -Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:8020 \
     -Daeron.archive.recording.events.channel=aeron:udp?control-mode=dynamic\|control=localhost:8030 \
     -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:8012 \
     -cp "$JAR_FILE" \
     io.aeron.archive.ArchivingMediaDriver &

PID=$!
echo $PID > "$ROOT_DIR/.aeron_archive.pid"

echo "Archive started with PID: $PID"
echo ""
echo "To stop the archive, run: ./scripts/kill_archive.sh"
echo ""

# Wait a bit for startup
sleep 2

# Keep script running
echo "Archive is running. Press Ctrl+C to stop."
wait $PID