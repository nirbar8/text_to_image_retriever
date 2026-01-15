from __future__ import annotations

from enum import Enum


class TylerMode(str, Enum):
    ORTHOPHOTO = "orthophoto"
    SATELLITE = "satellite"
    COCO = "coco"
    DOTA = "dota"
    STRIP = "strip"
