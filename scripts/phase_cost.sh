#!/bin/sh
# phase_cost.sh IMAGE DATASET : user+sys cycles/instructions for prefixes of the pipeline
IMG=${1:-ss-prof}; D=${2:-p20}
for st in 1 2 3 0; do
  MSYS_NO_PATHCONV=1 docker run --rm --privileged -v ssdata:/v -e STOP_AFTER=$st -e NCPU=${NCPU:-8} -e DATA_DIR=/v/$D -e OUTPUT_DIR=/tmp/o $IMG \
    bash -c "mkdir -p /tmp/o; perf stat -x, -e cycles:u,cycles:k,instructions:u /sessions 2>&1 >/dev/null | grep -E 'cycles|instructions' | cut -d, -f1,3 | tr '\n' ' '" | sed "s/^/stop=$st: /"; echo
done
