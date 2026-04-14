#!/usr/bin/env bash
# =============================================================================
# scripts/setup.sh
# One-shot local environment setup for Highway Toll Collection System
#
# Author : Mayukh Ghosh | Roll No : 23052334
# Batch  : Data Engineering 2025-2026, KIIT University
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

info "=== Highway Toll Collection System — Setup ==="

# ── Prerequisites check ────────────────────────────────────────────────────────
command -v python3 >/dev/null 2>&1 || error "Python 3.11+ is required."
command -v docker  >/dev/null 2>&1 || error "Docker is required."
command -v docker  >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || \
    error "Docker Compose (v2) is required."

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Python version: $PYTHON_VER"

# ── Virtual environment ────────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    info "Creating virtual environment..."
    python3 -m venv .venv
fi

info "Activating virtual environment..."
# shellcheck disable=SC1091
source .venv/bin/activate

info "Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
info "Dependencies installed."

# ── Docker services ────────────────────────────────────────────────────────────
info "Starting Docker services (Kafka, Redis, Airflow, Grafana)..."
docker compose -f docker/docker-compose.yml up -d --build

info "Waiting for Kafka to be ready (30s)..."
sleep 30

# ── Kafka topic ────────────────────────────────────────────────────────────────
info "Verifying Kafka topic 'toll-transactions'..."
docker exec toll-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --list | grep toll-transactions && \
    info "Topic exists." || \
    warn "Topic not yet visible — it will be auto-created on first produce."

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
info "=== Setup Complete ==="
echo ""
echo "  Airflow  → http://localhost:8080  (admin / admin)"
echo "  Grafana  → http://localhost:3000  (admin / admin)"
echo "  API      → http://localhost:8000/docs"
echo ""
echo "  Start producer : python kafka/producer.py"
echo "  Start consumer : python kafka/consumer_test.py"
echo "  Run Spark job  : spark-submit spark/streaming_job.py"
echo "  Run tests      : pytest tests/ -v"
echo ""
