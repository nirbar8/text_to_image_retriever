#!/usr/bin/env bash
set -euo pipefail

DOTA_ROOT="${DOTA_ROOT:-data/dota}"
DOTA_URL="${DOTA_URL:-https://github.com/ultralytics/assets/releases/download/v0.0.0/DOTAv1.zip}"

mkdir -p "${DOTA_ROOT}"
cd "${DOTA_ROOT}"

echo "Downloading: ${DOTA_URL}"
curl -fL --retry 3 --retry-delay 1 -o DOTAv1.zip "${DOTA_URL}"

if ! unzip -tq DOTAv1.zip >/dev/null 2>&1; then
  echo "Downloaded DOTAv1.zip is not a valid zip." >&2
  exit 1
fi

unzip -q DOTAv1.zip
echo "Done. Extracted under ${DOTA_ROOT}"
