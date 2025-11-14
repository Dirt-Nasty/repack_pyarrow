#!/usr/bin/env bash
set -euo pipefail

: "${SRC_BUCKET:?SRC_BUCKET is required}"
: "${SRC_PREFIX:?SRC_PREFIX is required}"

DEST_PREFIX="${DEST_PREFIX:-${SRC_PREFIX%/}_repack}"
WORKERS="${WORKERS:-32}"
BATCH_SIZE="${BATCH_SIZE:-65536}"

exec python -m repack_s3 \
  --src "s3://${SRC_BUCKET}/${SRC_PREFIX}" \
  --dst "s3://${SRC_BUCKET}/${DEST_PREFIX}" \
  --workers "${WORKERS}" \
  --batch-size "${BATCH_SIZE}"


