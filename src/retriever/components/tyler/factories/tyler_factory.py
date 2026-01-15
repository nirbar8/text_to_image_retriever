from __future__ import annotations

from typing import List, Type

from retriever.components.tyler.settings.tyler_settings import TylerSettings
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.interfaces import Tyler


class TylerFactory:
    """Registry-based factory for creating tyler instances."""

    def __init__(self, settings: TylerSettings, available_tylers: List[Type[BaseTyler]]) -> None:
        self._settings = settings
        self._available_tylers = available_tylers

    def build(self) -> Tyler:
        """Build a tyler instance based on the configured mode."""
        mode_value = self._settings.mode.value
        
        tyler_class = next(
            (tyler_cls for tyler_cls in self._available_tylers if tyler_cls.tyler_mode == mode_value),
            None
        )
        
        if tyler_class is None:
            raise ValueError(f"Unsupported tyler mode: {mode_value}")
        
        settings = tyler_class.get_settings_from(self._settings)
        return tyler_class.from_settings(settings)
