#!/bin/sh
# run_dev.sh DATA_DIR OUT_DIR  (Windows paths ok) -- scorer-like flags
D=$(cd "$1" && pwd -W 2>/dev/null || pwd); mkdir -p "$2"; rm -rf "$2"/*; O=$(cd "$2" && pwd -W 2>/dev/null || pwd)
S=$(mktemp -d); SW=$(cd "$S" && pwd -W 2>/dev/null || pwd)
MSYS_NO_PATHCONV=1 docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cpus "${CPUS:-8}" --memory "${MEM:-7g}" --memory-swap "${MEM:-7g}" --user 0 \
  -v "$D":/data:ro -v "$O":/output -v "$SW":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
  "${IMAGE:-shopping-sessions:latest}"
