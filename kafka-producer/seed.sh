#!/bin/sh
set -e

# Seed only if the volume is empty (or marker missing)
if [ ! -f /data/.seeded ]; then
  echo "[seed] Copying dataset into volume at /data ..."
  mkdir -p /data
  # copy without overwriting existing files
  cp -an /seed/dataset/. /data/
  date > /data/.seeded
else
  echo "[seed] Dataset already present; skipping copy."
fi

# Run the normal command
exec "$@"
