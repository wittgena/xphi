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
        
        # .py 확장자 제거
        parts[-1] = parts[-1][:-3]
        
        # Base 패키지명이 있으면 접두어로 붙임 (예: fiber + phase.reactor -> fiber.phase.reactor)
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
    """디렉토리를 순회하며 모듈 발견 (격리 및 중복 방지 로직 포함)"""
    if not root.exists():
        return

    quarantine_patterns = set()
    if apply_quarantine:
        quarantine_patterns = {"venv", ".venv", "env", "node_modules", "space/time"}
        try:
            self_root = find_current_self(root)
            bound_data = load_bound(self_root)
            if "quarantine" in bound_data:
                quarantine_patterns.update(bound_data["quarantine"])
        except Exception:
            pass
        log.info(f"[Discover] 🛡️ Active Quarantine for {root.name}: {quarantine_patterns}")

    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)

    ignored_count = 0

    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath).resolve()
        
        # 1. 단일성 보장: 이미 스캔한 디렉토리면 하위 트리를 무시하고 스킵
        if current_dir in _SCANNED_DIRS:
            dirnames[:] = []  
            continue
            
        _SCANNED_DIRS.add(current_dir)
        
        # 2. 숨김 폴더 제외 (.git, .idea 등)
        dirnames[:] = [d for d in dirnames if not d.startswith((".", "__"))]

        # 3. 격리 대상 폴더 필터링 (I/O 성능 최적화)
        if apply_quarantine:
            try:
                rel_dir = current_dir.relative_to(root).as_posix()
            except ValueError:
                rel_dir = current_dir.name
            
            # venv 등의 폴더를 만나면 `dirnames`를 비워버려 하위 파일 탐색을 원천 차단
            if any(q in rel_dir for q in quarantine_patterns):
                ignored_count += len(filenames)
                dirnames[:] = [] 
                continue

        # 4. 모듈 로드
        for f in filenames:
            if not f.endswith(".py") or (f.startswith("_") and f != "__init__.py"):
                continue

            py_file = current_dir / f
            _load_module_by_fqn(py_file, root, base_fqn)

    if ignored_count > 0 and apply_quarantine:
        log.info(f"[Discover] Ignored files matching quarantine patterns in {root.name}.")


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