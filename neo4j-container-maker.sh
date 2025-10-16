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
echo "Launching new Neo4j container: '$CONTAINER_NAME' with GDS Plugin..."

# MODIFIED: Removed the volume mount to prevent permission errors on macOS.
sudo docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$HOST_HTTP_PORT":7474 \
    -p "$NEO4J_BOLT_PORT":7687 \
    -e NEO4J_AUTH="${NEO4J_USER}/${NEO4J_PASSWORD}" \
    -e NEO4J_PLUGINS='["graph-data-science"]' \
    --env NEO4J_dbms_memory_heap_initial__size=2G \
    --env NEO4J_dbms_memory_heap_max__size=4G \
    "$NEO4J_IMAGE"

if [ $? -ne 0 ]; then
    echo "FATAL ERROR: Docker failed to start the Neo4j container. Aborting."
    exit 1
fi

echo "Waiting 25 seconds for Neo4j to initialize..."
sleep 25

# --- 5. PYTHON DEPENDENCY INSTALLATION ---
echo ""
echo "--- Installing/Verifying Python Dependencies ---"
sudo -E python -m pip install \
    "fpdf>=1.7.2" \
    "matplotlib>=3.10.6" \
    "neo4j>=6.0.2" \
    "pandas>=2.3.2" \
    "networkx>=3.5" \
    "numpy>=2.3.2" \
    "pyarrow>=21.0.0" \
    "scikit-learn>=1.7.2" \
    "scipy>=1.16.2" \
    "urllib3>=2.5.0"

if [ $? -ne 0 ]; then
    echo "FATAL ERROR: Failed to install Python dependencies. Aborting."
    exit 1
fi
echo "Dependencies are up to date."

# --- 6. PYTHON CONNECTION TEST ---
echo ""
echo "--- Running Python Bolt Connection Test ---"
sudo -E python -c "
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

# --- 7. FINAL CONFIRMATION ---
echo ""
echo "=========================================================="
echo "Deployment Complete."
echo "   - Container Name: $CONTAINER_NAME"
echo "   - **Access Browser at:** http://localhost:$HOST_HTTP_PORT"
echo "=========================================================="