#!/usr/bin/env bash
set -euo pipefail

DOTA_BASE_URL="${DOTA_BASE_URL:-https://captain-whu.github.io/DOTA/DOTA-v1.0}"
DOTA_ROOT="${DOTA_ROOT:-data/dota}"

mkdir -p "${DOTA_ROOT}"
cd "${DOTA_ROOT}"

curl -L -o train.zip "${DOTA_BASE_URL}/train.zip"
curl -L -o val.zip "${DOTA_BASE_URL}/val.zip"
curl -L -o train_labels.zip "${DOTA_BASE_URL}/train_labels.zip"
curl -L -o val_labels.zip "${DOTA_BASE_URL}/val_labels.zip"

unzip -q train.zip
unzip -q val.zip
unzip -q train_labels.zip
unzip -q val_labels.zip

echo "Done. DOTA images and labels downloaded under ${DOTA_ROOT}."
