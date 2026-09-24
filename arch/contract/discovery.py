# xphi.arch.contract.discovery
import sys
import importlib
import importlib.util
import os
import traceback
from pathlib import Path

from xphi.kernel.space.bind.resolver import load_bound, find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("contract.discovery")
_TRACEBACK_PRINTED = False

_SCANNED_DIRS = set()
_SCANNED_MODULES = set()

def _load_module_by_fqn(py_file: Path, base_dir: Path, base_fqn: str = "") -> None:
    try:
        rel_path = py_file.relative_to(base_dir)
        parts = list(rel_path.parts)
        parts[-1] = parts[-1][:-3]
        
        fqn = f"{base_fqn}." + ".".join(parts) if base_fqn else ".".join(parts)
        if fqn and fqn not in sys.modules and fqn not in _SCANNED_MODULES:
            importlib.import_module(fqn)
            _SCANNED_MODULES.add(fqn)
            
    except Exception as e:
        log.warning(f"[Discover] Failed to load {py_file}: {e}")
        global _TRACEBACK_PRINTED
        if not _TRACEBACK_PRINTED and os.getenv("DEBUG_DISCOVERY") == "1":
            traceback.print_exc()
            _TRACEBACK_PRINTED = True


def _walk_and_discover(root: Path, base_fqn: str = "", apply_quarantine: bool = True) -> None:
    """디렉토리를 순회하며 모듈 발견 (통합 격리 및 중복 방지 로직 포함)"""
    if not root.exists():
        return

    exclude_patterns = {"dev/ex", "space/time"}
    if apply_quarantine:
        exclude_patterns.update({"venv", ".venv", "env", "node_modules"})
        try:
            self_root = find_current_self(root)
            bound_data = load_bound(self_root)
            if "quarantine" in bound_data:
                exclude_patterns.update(bound_data["quarantine"])
        except Exception:
            pass
        log.info(f"[Discover] 🛡️ Workspace Quarantine for {root.name}: {exclude_patterns}")
    else:
        log.info(f"[Discover] 🛡️ Core Quarantine for {root.name}: {exclude_patterns}")

    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)

    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath).resolve()
        if current_dir in _SCANNED_DIRS:
            dirnames[:] = []  
            continue
            
        _SCANNED_DIRS.add(current_dir)
        try:
            rel_dir = current_dir.relative_to(root).as_posix()
            if rel_dir == ".": 
                rel_dir = ""
        except ValueError:
            rel_dir = current_dir.name

        valid_dirs = []
        for d in dirnames:
            if d.startswith((".", "__")):
                continue
            
            child_rel = f"{rel_dir}/{d}" if rel_dir else d
            if any(ex in child_rel for ex in exclude_patterns):
                continue
                
            valid_dirs.append(d)
            
        dirnames[:] = valid_dirs
        for f in filenames:
            if not f.endswith(".py") or (f.startswith("_") and f != "__init__.py"):
                continue

            py_file = current_dir / f
            _load_module_by_fqn(py_file, root, base_fqn)


def discover_core_package(pkg_name: str) -> None:
    try:
        spec = importlib.util.find_spec(pkg_name)
        if not spec or not spec.submodule_search_locations:
            return
        
        pkg_root = Path(spec.submodule_search_locations[0]).resolve()
        log.info(f"[Discover] Core package '{pkg_name}' pinpointed at: {pkg_root}")
        _walk_and_discover(pkg_root, base_fqn=pkg_name, apply_quarantine=False)
    except ImportError:
        log.warning(f"[Discover] Core package '{pkg_name}' could not be imported.")


def discover_modules(workspace_root: Path) -> None:
    log.info("\n[Discover] Initiating system-wide module discovery...")
    
    discover_core_package("fiber")
    discover_core_package("xphi")
    
    log.info(f"[Discover] Scanning workspace: {workspace_root}")
    _walk_and_discover(workspace_root, base_fqn="", apply_quarantine=True)