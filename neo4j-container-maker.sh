#!/bin/bash
# ====================================================================
# Neo4j Docker Management Script for Thesis Project (Validated)
# ====================================================================

# --- CONFIGURATION ---
CONTAINER_NAME="neo4j_thesis_server"
NEO4J_USER="neo4j"
NEO4J_IMAGE="neo4j:latest" 
HOST_DATA_DIR="${PWD}/data"
NEO4J_BOLT_PORT="7687"

echo "--- NEO4J DOCKER MANAGEMENT SCRIPT ---"
echo "This script will use 'sudo' for Docker and system commands."

# --- 1. SECURE PASSWORD INPUT & VALIDATION ---
while true; do
    echo -n "Enter the desired password for the '$NEO4J_USER' user (min 8 chars): "
    read -s NEO4J_PASSWORD
    echo 

    if [ ${#NEO4J_PASSWORD} -lt 8 ]; then
        echo "ERROR: Password must be at least 8 characters long. Please try again."
    else
        break # Password is valid, exit the loop
    fi
done

export NEO4J_USER
export NEO4J_PASSWORD
export BOLT_PORT="$NEO4J_BOLT_PORT"
echo "Credentials for this session set as environment variables."

# --- 2. PORT CONFLICT RESOLUTION (with sudo) ---
HOST_HTTP_PORT=""
echo "Checking port availability (requires sudo)..."

if ! sudo lsof -i tcp:7474 -sTCP:LISTEN -n -P | grep "LISTEN" > /dev/null; then
    HOST_HTTP_PORT="7474"
    echo "Browser port 7474 is available."
else
    echo "Browser port 7474 is IN USE."
    if ! sudo lsof -i tcp:8080 -sTCP:LISTEN -n -P | grep "LISTEN" > /dev/null; then
        HOST_HTTP_PORT="8080"
        echo "Switching to alternative port 8080."
    else
        echo "FATAL ERROR: Both ports 7474 and 8080 are in use."
        exit 1
    fi
fi

# --- 3. CLEANUP: Remove ALL existing Neo4j containers ---
echo ""
OLD_NEO4J_CONTAINERS=$(sudo docker ps -a -q --filter ancestor="$NEO4J_IMAGE")

if [ -n "$OLD_NEO4J_CONTAINERS" ]; then
    echo "Cleaning up existing Neo4j container(s)..."
    sudo docker stop $OLD_NEO4J_CONTAINERS > /dev/null 2>&1
    sudo docker rm -f $OLD_NEO4J_CONTAINERS > /dev/null 2>&1
    echo "Cleanup complete."
else
    echo "No old containers found."
fi

# --- 4. RUN NEW CONTAINER ---
sudo mkdir -p "$HOST_DATA_DIR"
echo "Launching new Neo4j container: '$CONTAINER_NAME' with GDS Plugin..."

sudo docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$HOST_HTTP_PORT":7474 \
    -p "$NEO4J_BOLT_PORT":7687 \
    -v "$HOST_DATA_DIR":/var/lib/neo4j/data \
    -e NEO4J_AUTH="$NEO4J_USER/$NEO4J_PASSWORD" \
    -e NEO4J_PLUGINS='["graph-data-science"]' \
    "$NEO4J_IMAGE"

if [ $? -ne 0 ]; then
    echo "FATAL ERROR: Docker failed to start the Neo4j container. Aborting."
    exit 1
fi

echo "Waiting 10 seconds for Neo4j to initialize..."
sleep 10

# --- 5. PYTHON CONNECTION TEST ---
echo ""
echo "--- Running Python Bolt Connection Test ---"
sudo python -c "
import os, sys
from neo4j import GraphDatabase, exceptions
try:
    driver = GraphDatabase.driver(f\"bolt://localhost:{os.environ.get('BOLT_PORT')}\", auth=(os.environ.get('NEO4J_USER'), os.environ.get('NEO4J_PASSWORD')))
    driver.verify_connectivity()
    print('CONNECTION TEST SUCCESSFUL: Python authenticated via Bolt.')
    sys.exit(0)
except Exception as e:
    print(f'CONNECTION TEST FAILED: {e}')
    sys.exit(1)
finally:
    if 'driver' in locals():
        driver.close()
"

# --- 6. FINAL CONFIRMATION ---
echo ""
echo "=========================================================="
echo "Deployment Complete."
echo "   - Container Name: $CONTAINER_NAME"
echo "   - **Access Browser at:** http://localhost:$HOST_HTTP_PORT"
echo "=========================================================="