# arch.contract.discovery
import sys
import importlib
import ast
import os
import traceback
from pathlib import Path
from typing import Optional, Set
from xphi.kernel.space.bind.resolver import load_bound, find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("contract.discovery")

_TRACEBACK_PRINTED = False

SAFE_TOP_LEVEL_CALLS = {
    "get_logger", "get_emitter", "resolve_path", "find_current_self", "Path", "os.getenv", 
    "load_dotenv", "dict", "set", "list", "tuple", "SynapticRouter"
}

def _has_top_level_side_effects(py_file: Path) -> bool:
    """
    @internal: Analyzes AST to detect unwanted side-effects during module import.
    """
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
                        func_name = f"{node.value.func.value.id}.{node.value.func.attr}" if isinstance(node.value.func.value, ast.Name) else node.value.func.attr
                    
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

def _contains_forbidden_imports(py_file: Path, forbidden_libs: Set[str]) -> bool:
    """
    @internal: Statically verifies if a module references restricted external packages.
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
                    if root_module in forbidden_libs:
                        return True
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root_module = node.module.split('.')[0]
                    if root_module in forbidden_libs:
                        return True
    except Exception as e:
        log.erro(f"[Discover] AST Parsing failed for {py_file.name}: {e}")
        return False

    return False

def discover_modules(
    root: Path, 
    forbidden_libs: Optional[Set[str]] = None,
    exclude_files: Optional[Set[str]] = None,
    force_reload: bool = False
) -> None:
    if not root.exists():
        print(f"[Discover] Root path {root} does not exist.")
        return
    
    ## 1. Pre-load bound.json to cache absolute paths of privileged repositories
    core_paths = []
    try:
        self_root = find_current_self(root)
        bound_data = load_bound(self_root)
        around_data = bound_data.get("around", {})
        
        for repo_name, repo_info in around_data.items():
            if repo_info.get("allow_side_effects", False):
                repo_abs_path = repo_info.get("path")
                if repo_abs_path:
                    core_paths.append((repo_name, repo_abs_path))
    except Exception as e:
        print(f"[Discover] Bound loading failed, applying strict rules: {e}")

    print(f"[Discover] Start module discovery from: {root}")
    if core_paths:
        print(f"[Discover] Active Core Repos (Bypassing side-effects): {[name for name, _ in core_paths]}")

    forbidden_set = forbidden_libs or {"dspy"}
    exclude_set = exclude_files or {"registry.py", "scanner.py", "discover.py"}

    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)

    ignored_files = [] # Gather ignored files for summary logging

    for py_file in root.rglob("*.py"):
        ## Relative path for logging purposes
        try:
            rel_path_str = str(py_file.relative_to(root.parent))
        except ValueError:
            rel_path_str = str(py_file.name)

        if (py_file.name.startswith("_") and py_file.name != "__init__.py") or \
           py_file.name in exclude_set:
            continue

        if _contains_forbidden_imports(py_file, forbidden_set):
            ignored_files.append((rel_path_str, "Heavy Dependency"))
            continue

        ## Verify absolute path to determine privileged repository status
        abs_py_file = str(py_file.resolve())
        is_core_file = False
        repo_label = "Periphery"
        
        for repo_name, core_path in core_paths:
            # Grant exemption if file resides within a privileged repository
            if abs_py_file.startswith(core_path):
                is_core_file = True
                repo_label = repo_name
                break

        ## @rule.C: Block unprivileged top-level side-effects
        if not is_core_file:
            if _has_top_level_side_effects(py_file):
                ignored_files.append((rel_path_str, "Top-level Side-effects"))
                continue

        ## @rule.D: Dynamic module loading
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
            log.warn(f"[Discover] Failed to load {rel_path_str}: {e}")
            global _TRACEBACK_PRINTED
            if not _TRACEBACK_PRINTED and os.getenv("DEBUG_DISCOVERY") == "1" and isinstance(e, (ImportError, AttributeError)):
                traceback.print_exc()
                
    ## Output summarized logs for ignored files
    if ignored_files:
        log.info(f"[Discover] Ignored {len(ignored_files)} modules during discovery phase.")
        if os.getenv("DEBUG_DISCOVERY") == "1":
            for path, reason in ignored_files:
                log.info(f"  - Skipped: {path} (Reason: {reason})")