#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/coco
cd data/coco

# Train images (118K)
curl -L -o train2017.zip http://images.cocodataset.org/zips/train2017.zip
unzip -q train2017.zip

# Annotations
curl -L -o annotations_trainval2017.zip http://images.cocodataset.org/annotations/annotations_trainval2017.zip
unzip -q annotations_trainval2017.zip

echo "Done. Images in data/coco/train2017, annotations in data/coco/annotations"
