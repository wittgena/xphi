# xphi.arch.contract.discovery
import sys
import importlib
import os
import traceback
from pathlib import Path

from xphi.kernel.space.bind.resolver import load_bound, find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("contract.discovery")
_TRACEBACK_PRINTED = False

def discover_modules(root: Path) -> None:
    if not root.exists():
        log.warning(f"[Discover] Root path {root} does not exist.")
        return
    
    ## 기본 격리 패턴 + 설정 파일(bound.json) 패턴 병합
    quarantine_patterns = {"venv", "env", "node_modules", "space/time"}
    
    try:
        self_root = find_current_self(root)
        bound_data = load_bound(self_root)
        if "quarantine" in bound_data:
            quarantine_patterns.update(bound_data["quarantine"])
    except Exception as e:
        log.warning(f"[Discover] Bound loading failed, applying default quarantine: {e}")

    log.info(f"[Discover] Start module discovery from: {root}")
    log.info(f"[Discover] 🛡️ Active Quarantine Patterns: {quarantine_patterns}")

    ## Python Path 등록
    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)

    ignored_count = 0

    ## 파일 순회 및 패턴 필터링
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith((".", "__"))]
        
        for f in filenames:
            if not f.endswith(".py") or (f.startswith("_") and f != "__init__.py"):
                continue

            py_file = Path(dirpath) / f
            try:
                # POSIX 스타일( / )로 정규화된 상대 경로 생성
                rel_path = py_file.relative_to(root).as_posix()
            except ValueError:
                rel_path = py_file.name

            # 격리 대상 경로 패턴이 포함되어 있다면 스킵
            if any(q in rel_path for q in quarantine_patterns):
                ignored_count += 1
                continue

            # 4. 모듈 로드
            try:
                module_path = ".".join(py_file.relative_to(root).with_suffix("").parts)
                if module_path and module_path not in sys.modules:
                    importlib.import_module(module_path)
            except Exception as e:
                log.warning(f"[Discover] Failed to load {rel_path}: {e}")
                
                global _TRACEBACK_PRINTED
                if not _TRACEBACK_PRINTED and os.getenv("DEBUG_DISCOVERY") == "1":
                    traceback.print_exc()
                    _TRACEBACK_PRINTED = True

    if ignored_count > 0:
        log.info(f"[Discover] Ignored {ignored_count} files matching quarantine patterns.")