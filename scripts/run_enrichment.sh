#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="$(cd "$(dirname "$0")/.."; pwd)"
python scripts/enrich_companies.py "$@"
