# xphi.arch.contract.discovery
import sys
import importlib
import ast
import os
import traceback
from pathlib import Path
from typing import Optional, Set, List, Iterator

from xphi.kernel.space.bind.resolver import load_bound, find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("contract.discovery")

_TRACEBACK_PRINTED = False
SAFE_TOP_LEVEL_CALLS = {
    "get_logger", "get_emitter", "resolve_path", "find_current_self", "Path", "os.getenv", 
    "load_dotenv", "dict", "set", "list", "tuple", "SynapticRouter"
}


def _get_airlocked_namespaces() -> Set[str]:
    airlocked = os.environ.get("PHASE_AIRLOCKED_NAMESPACES", "")
    return set(airlocked.split(",")) if airlocked else set()


def _walk_safe_python_files(root: Path, quarantine_dirs: Set[str]) -> Iterator[Path]:
    """@internal: 격리된 디렉토리(venv 등)를 제외하고 안전하게 Python 파일을 순회"""
    for dirpath, dirnames, filenames in os.walk(root):
        # 격리할 디렉토리 및 숨김 디렉토리 제외
        dirnames[:] = [
            d for d in dirnames 
            if d not in quarantine_dirs and not d.startswith((".", "__"))
        ]
        
        for f in filenames:
            if f.endswith(".py"):
                yield Path(dirpath) / f


def _has_top_level_side_effects(py_file: Path) -> bool:
    """@internal: Analyzes AST to detect unwanted side-effects during module import"""
    try:
        with open(py_file, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=py_file.name)
            
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            
            if isinstance(node, ast.Assign):
                if isinstance(node.value, ast.Call):
                    func_name = ""
                    if isinstance(node.value.func, ast.Name):
                        func_name = node.value.func.id
                    elif isinstance(node.value.func, ast.Attribute):
                        val = node.value.func.value
                        func_name = f"{val.id}.{node.value.func.attr}" if isinstance(val, ast.Name) else node.value.func.attr
                    
                    if func_name in SAFE_TOP_LEVEL_CALLS:
                        continue
                    else:
                        return True 
                continue

            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                continue
            
            if isinstance(node, ast.If):
                test = node.test
                if isinstance(test, ast.Compare):
                    left = test.left
                    if isinstance(left, ast.Name) and left.id == "__name__":
                        continue

            return True
    except Exception as e:
        log.error(f"[Discover] AST Side-effect Check failed for {py_file.name}: {e}")
        return True 

    return False


def _contains_forbidden_imports(py_file: Path, forbidden_libs: Set[str], airlocked_libs: Set[str]) -> bool:
    """
    @internal: Statically verifies if a module references restricted external packages.
    Airlocked(Hijacked) namespaces are explicitly exempted.
    """
    if not forbidden_libs:
        return False
        
    try:
        with open(py_file, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=py_file.name)
            
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_module = alias.name.split('.')[0]
                    # 🚨 방어책 2: Airlock에 의해 가로채진 모듈은 금지 목록에서 면제
                    if root_module in forbidden_libs and root_module not in airlocked_libs:
                        return True
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root_module = node.module.split('.')[0]
                    if root_module in forbidden_libs and root_module not in airlocked_libs:
                        return True
    except Exception as e:
        log.error(f"[Discover] AST Parsing failed for {py_file.name}: {e}")
        return False

    return False


def discover_modules(
    root: Path, 
    forbidden_libs: Optional[Set[str]] = None,
    exclude_files: Optional[Set[str]] = None,
    force_reload: bool = False
) -> None:
    if not root.exists():
        log.warning(f"[Discover] Root path {root} does not exist.")
        return
    
    ## 1. Pre-load bound.json
    core_paths = []
    quarantine_dirs = {"venv", "env", "node_modules"} # 기본 격리 구역
    
    try:
        self_root = find_current_self(root)
        bound_data = load_bound(self_root)
        around_data = bound_data.get("around", {})
        
        # 🚨 방어책 3: bound.json에서 명시적 Quarantine(격리) 목록 로드
        if "quarantine" in bound_data:
            quarantine_dirs.update(bound_data["quarantine"])
        
        for repo_name, repo_info in around_data.items():
            if repo_info.get("allow_side_effects", False):
                repo_abs_path = repo_info.get("path")
                if repo_abs_path:
                    core_paths.append((repo_name, repo_abs_path))
    except Exception as e:
        log.warning(f"[Discover] Bound loading failed, applying strict rules: {e}")

    log.info(f"[Discover] Start module discovery from: {root}")
    if quarantine_dirs:
        log.info(f"[Discover] 🛡️ Active Quarantine Zones: {quarantine_dirs}")
    if core_paths:
        log.info(f"[Discover] Active Core Repos (Bypassing side-effects): {[name for name, _ in core_paths]}")

    forbidden_set = forbidden_libs or {"dspy"}
    exclude_set = exclude_files or {"registry.py", "scanner.py", "discover.py"}
    airlocked_set = _get_airlocked_namespaces()

    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)

    ignored_files = []

    # 기존 rglob("*.py") 대신 안전한 디렉토리 가지치기가 적용된 순회 함수 사용
    for py_file in _walk_safe_python_files(root, quarantine_dirs):
        try:
            rel_path_str = str(py_file.relative_to(root.parent))
        except ValueError:
            rel_path_str = str(py_file.name)

        # 숨김 파일(단 __init__.py는 허용)이거나 제외 대상인 경우 스킵
        if (py_file.name.startswith("_") and py_file.name != "__init__.py") or py_file.name in exclude_set:
            continue

        if _contains_forbidden_imports(py_file, forbidden_set, airlocked_set):
            ignored_files.append((rel_path_str, "Heavy/Forbidden Dependency"))
            continue

        abs_py_file = str(py_file.resolve())
        is_core_file = any(abs_py_file.startswith(core_path) for _, core_path in core_paths)

        if not is_core_file:
            if _has_top_level_side_effects(py_file):
                ignored_files.append((rel_path_str, "Top-level Side-effects"))
                continue

        try:
            relative = py_file.relative_to(root)
            module_path = ".".join(relative.with_suffix("").parts)
            if module_path:
                if module_path in sys.modules:
                    if force_reload:
                        importlib.reload(sys.modules[module_path])
                else:
                    importlib.import_module(module_path)
        except Exception as e:
            log.warning(f"[Discover] Failed to load {rel_path_str}: {e}")
            global _TRACEBACK_PRINTED
            if not _TRACEBACK_PRINTED and os.getenv("DEBUG_DISCOVERY") == "1" and isinstance(e, (ImportError, AttributeError)):
                traceback.print_exc()
                _TRACEBACK_PRINTED = True  # 플래그 업데이트 추가
                
    if ignored_files:
        log.info(f"[Discover] Ignored {len(ignored_files)} modules during discovery phase.")
        if os.getenv("DEBUG_DISCOVERY") == "1":
            for path, reason in ignored_files:
                log.info(f"  - Skipped: {path} (Reason: {reason})")