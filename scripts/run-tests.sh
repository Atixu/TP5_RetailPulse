#!/usr/bin/env bash
# Tests unitaires : applications Python (image app) et transformations Spark (image spark).
set -euo pipefail
cd "$(dirname "$0")/.."
docker run --rm -u root -v "$PWD/tests:/tests:ro" retailpulse-app:1.0.0 sh -c \
  'pip install -q pytest==8.3.4 2>/dev/null && cd /tmp && python -m pytest -q -p no:cacheprovider /tests -o pythonpath=/app'
docker run --rm retailpulse-spark:1.0.0 python3 -m pytest -q -p no:cacheprovider tests
