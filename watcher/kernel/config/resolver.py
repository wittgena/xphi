# watcher.kernel.config.resolver
import os
import logging
from typing import Any
from watcher.plane.emitter import get_emitter

log = get_emitter("config.resolver")

class ConfigResolver:
    def __init__(self):
        self._local_overrides: dict[str, Any] = {}
        self._defaults: dict[str, Any] = {
            "modify_params": True,
            "drop_params": True,
        }

    def get(self, name: str, default: Any = None) -> Any:
        try:
            return getattr(self, name)
        except AttributeError:
            return default

    def set_override(self, key: str, value: Any):
        self._local_overrides[key] = value

    def __getattr__(self, name: str) -> Any:
        if name in self._local_overrides:
            return self._local_overrides[name]

        env_key = name.upper()
        if env_key in os.environ:
            val = os.environ[env_key]
            if val.lower() in ("true", "1", "yes", "t"):
                return True
            if val.lower() in ("false", "0", "no", "f"):
                return False
            return val

        if name in self._defaults:
            return self._defaults[name]

        raise AttributeError(f"'{type(self).__name__}' object (brane config) has no attribute '{name}'")
    
    def __setattr__(self, name: str, value: Any):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._local_overrides[name] = value

config = ConfigResolver()