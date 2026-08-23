# kernel.bind.redirector
## @lineage: kernel.phase.bind.redirector
## @lineage: phase.bind.redirector
import sys
import types
import importlib.util
from pathlib import Path
from typing import Optional, Union
from xphi.kernel.bind.resolver import find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bind.redirector")

SELF_ROOT = find_current_self()

class ModuleRedirector:
    def __init__(self, target_package: str, local_dir: Union[str, Path], clear_cache: bool = True):
        self.target_package = target_package
        self.local_dir = Path(local_dir).resolve()
        self.clear_cache = clear_cache
        self._is_installed = False

    def find_spec(self, fullname, path, target=None):
        ## Check if it matches the target package or its sub-packages
        if fullname == self.target_package or fullname.startswith(f"{self.target_package}."):
            rel_path = fullname[len(self.target_package):].lstrip(".").replace(".", "/")
            target_path = self.local_dir / rel_path

            ## Check for package structure (presence of __init__.py)
            if target_path.is_dir():
                init_file = target_path / "__init__.py"
                if init_file.exists():
                    return importlib.util.spec_from_file_location(
                        fullname,
                        str(init_file),
                        submodule_search_locations=[str(target_path)]
                    )
            
            ## Check for single file structure (presence of .py)
            py_file = target_path.with_suffix(".py")
            if py_file.exists():
                return importlib.util.spec_from_file_location(fullname, str(py_file))

        return None

    def install(self):
        """Register the custom finder at the highest priority in sys.meta_path."""
        if self._is_installed:
            return

        if self.clear_cache:
            self._clear_sys_modules()

        sys.meta_path.insert(0, self)
        self._is_installed = True
        log.info(f"[Redirector] '{self.target_package}' -> '{self.local_dir}' mapping installed.")

    def uninstall(self):
        """Remove the registered custom finder."""
        if self in sys.meta_path:
            sys.meta_path.remove(self)
        
        if self.clear_cache:
            self._clear_sys_modules()
            
        self._is_installed = False
        log.info(f"[Redirector] '{self.target_package}' mapping uninstalled.")

    def _clear_sys_modules(self):
        """Delete previously loaded cached modules to force a reload."""
        keys_to_del = [
            key for key in sys.modules.keys() 
            if key == self.target_package or key.startswith(f"{self.target_package}.")
        ]
        for key in keys_to_del:
            del sys.modules[key]

    ## Context manager support (enables 'with' statement)
    def __enter__(self):
        self.install()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.uninstall()

class PhaseAirlock:
    """
    @desc: An airlock mechanism controlling namespace fragmentation at phase boundaries.
    Forces memory synchronization of the past legacy path (Legacy) to the current canonical phase (Canonical).
    """

    @classmethod
    def establish_resonance(cls, legacy_path: str, canonical_path: str, submodules: list[str] = None):
        """
        Manipulates sys.modules to match the memory IDs of two namespaces (preventing fragmentation).
        If the canonical physical module does not exist, it synthesizes a dummy module in memory.
        
        Args:
            legacy_path: The legacy path external packages attempt to find (e.g., "vuln_lib").
            canonical_path: The true physical path in the current system (or a blackhole target).
            submodules: List of sub-module names to bind together.
        """
        # 1. Load or Synthesize the Canonical Module
        try:
            canonical_module = importlib.import_module(canonical_path)
        except ImportError:
            # Fallback: Create a synthetic module dynamically if physical module is missing
            canonical_module = types.ModuleType(canonical_path)
            canonical_module.__doc__ = f"Synthetic blackhole created by PhaseAirlock for '{legacy_path}'"
            sys.modules[canonical_path] = canonical_module
            log.warning(f"[!] PhaseAirlock: Physical module '{canonical_path}' not found. Synthesized a dummy module in memory.")

        # 2. Override the legacy trajectory with the canonical/synthetic phase
        sys.modules[legacy_path] = canonical_module
        log.info(f"[*] Resonance Established: {legacy_path} ➔ {canonical_path}")
        
        # 3. Synchronize specified sub-modules (critical for bypassing Pydantic validation etc.)
        if submodules:
            for sub in submodules:
                target_sub_path = f"{canonical_path}.{sub}"
                legacy_sub_path = f"{legacy_path}.{sub}"
                
                try:
                    target_sub_module = importlib.import_module(target_sub_path)
                except ImportError:
                    ## Cascade synthesis to sub-modules
                    target_sub_module = types.ModuleType(target_sub_path)
                    sys.modules[target_sub_path] = target_sub_module
                    log.warning(f"    ↳ Synthesized Submodule: {target_sub_path}")
                    
                sys.modules[legacy_sub_path] = target_sub_module
                log.info(f"    ↳ Linked Submodule: {legacy_sub_path} ➔ {target_sub_path}")