#!/bin/sh
# run_vol.sh SUBDIR : run the image on volume ssdata/SUBDIR, output to ssdata/out-SUBDIR, timed
set -e
MSYS_NO_PATHCONV=1 docker run --rm -v ssdata:/v ss-gen sh -c "rm -rf /v/out-$1 /v/scratch && mkdir -p /v/out-$1 /v/scratch && chmod 777 /v/out-$1 /v/scratch"
start=$(date +%s.%N)
MSYS_NO_PATHCONV=1 docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cpus "${CPUS:-8}" --user 1000:1000 \
  --mount type=volume,src=ssdata,dst=/data,volume-subpath=$1,readonly \
  --mount type=volume,src=ssdata,dst=/output,volume-subpath=out-$1 \
  --mount type=volume,src=ssdata,dst=/scratch,volume-subpath=scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
  "${IMAGE:-shopping-sessions:latest}"
end=$(date +%s.%N)
echo "wall: $(python -c "print(round($end-$start,2))")"
