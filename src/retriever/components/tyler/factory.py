from __future__ import annotations

from retriever.components.tyler.coco import CocoTyler, CocoTylerConfig
from retriever.components.tyler.orthophoto import OrthophotoTyler, OrthophotoTylerConfig
from retriever.components.tyler.satellite import SatelliteBoundsTyler, SatelliteTylerConfig
from retriever.components.tyler.settings import TylerMode, TylerSettings


class TylerFactory:
    def __init__(self, settings: TylerSettings) -> None:
        self._settings = settings

    def build(self) -> CocoTyler | OrthophotoTyler | SatelliteBoundsTyler:
        mode = self._settings.mode
        if mode is TylerMode.ORTHOPHOTO:
            cfg = self._settings.orthophoto
            return OrthophotoTyler(
                OrthophotoTylerConfig(
                    raster_path=cfg.raster_path,
                    tile_size_px=cfg.tile_size_px,
                    stride_px=cfg.stride_px,
                )
            )
        if mode is TylerMode.SATELLITE:
            cfg = self._settings.satellite
            return SatelliteBoundsTyler(
                SatelliteTylerConfig(
                    bounds=(cfg.bounds_minx, cfg.bounds_miny, cfg.bounds_maxx, cfg.bounds_maxy),
                    tile_size_deg=cfg.tile_size_deg,
                    tile_size_px=cfg.tile_size_px,
                    image_count=cfg.image_count,
                    image_size_deg=cfg.image_size_deg,
                    rotation_deg_max=cfg.rotation_deg_max,
                    seed=cfg.seed,
                )
            )
        if mode is TylerMode.COCO:
            cfg = self._settings.coco
            return CocoTyler(
                CocoTylerConfig(
                    instances_json=cfg.instances_json,
                    images_dir=cfg.images_dir,
                    max_items=cfg.max_items,
                    seed=cfg.seed,
                    lat_range=(cfg.lat_min, cfg.lat_max),
                    lon_range=(cfg.lon_min, cfg.lon_max),
                )
            )
        raise ValueError(f"Unsupported tyler mode: {mode}")
