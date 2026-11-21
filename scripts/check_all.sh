#!/bin/sh
# Build image, run fixtures, run dev and compare to the published answer.
set -e
cd "$(dirname "$0")/.."
docker build -q -t shopping-sessions:latest . >/dev/null
python scripts/fixtures_docker.py
sh scripts/run_dev.sh ../dev ../out 2>/dev/null
python scripts/compare.py ../out ../vendor/dev-answer/dev-answer | tail -3
