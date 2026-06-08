#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python -m ruff check src/scripts tests
python -m pytest tests/unit tests/integration tests/e2e
