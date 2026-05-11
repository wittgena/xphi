# phase.runtime.contract.doctor
## @lineage: meta.flow.contract.doctor
import ast
import argparse
import sys
import pkgutil
from importlib import metadata
from pathlib import Path
from typing import List, Dict, Set
from collections import defaultdict
from topos.bound.resolver import find_current_self
from anchor.around import discover_repos
from topos.bound.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
log = get_emitter('contract.doctor')

import sys
import pkgutil

def get_stdlib_modules() -> set:
    if hasattr(sys, "stdlib_module_names"):
        return set(sys.stdlib_module_names)
    
    stdlib_path = Path(sys.executable).parent.parent / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}"
    return {m.name for m in pkgutil.iter_modules() if str(m.module_finder.path).startswith(str(stdlib_path))}

STDLIB_MODULES = get_stdlib_modules()
STDLIB_MODULES.add("__future__")

class ImportDependencyVisitor(ast.NodeVisitor):
    def __init__(self):
        self.imported_packages: Set[str] = set()

    def _extract_root_package(self, module_name: str) -> str:
        return module_name.split('.')[0]

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            pkg = self._extract_root_package(alias.name)
            if pkg not in STDLIB_MODULES:
                self.imported_packages.add(pkg)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        if node.module:
            pkg = self._extract_root_package(node.module)
            if pkg not in STDLIB_MODULES:
                self.imported_packages.add(pkg)
        self.generic_visit(node)

def check_health(usage_map: Dict[str, List[str]], internal_names: Set[str]):
    log.info("\n" + "###")
    log.info("🩺 DEPENDENCY HEALTH REPORT")
    log.info("###")
    
    for pkg, files in sorted(usage_map.items()):
        if pkg in STDLIB_MODULES or pkg in internal_names:
            continue
            
        try:
            search_pkg = "PyYAML" if pkg == "yaml" else pkg
            ver = metadata.version(search_pkg)
            log.info(f"[O] {pkg.ljust(20)} | v{ver:<10} | Used in {len(files)} files")
        except metadata.PackageNotFoundError:
            log.error(f"[X] {pkg.ljust(20)} | NOT INSTALLED | Found in {files[0]}...")

def main():
    parser = argparse.ArgumentParser(description="Predictive Dependency Diagnosis Tool")
    parser.add_argument("--repo", type=str, nargs="+", help="Target paths to scan")
    args = parser.parse_args()

    target_paths = [Path(p).resolve() for p in args.repo] if args.repo else discover_repos(SELF_ROOT)
    
    internal_names = set()
    for p in target_paths:
        if p.is_dir():
            internal_names.add(p.name) # 루트 폴더명 (theoria)
            internal_names.update([d.name for d in p.iterdir() if d.is_dir()]) # 하위 폴더명 (arch, meta 등)

    if not target_paths:
        return

    from collections import defaultdict
    package_to_files = defaultdict(list)
    for path in target_paths:
        files = [path] if path.is_file() else path.rglob("*.py")
        for py_file in files:
            if py_file.name.startswith("_") and py_file.name != "__init__.py": continue
            try:
                tree = ast.parse(py_file.read_text(encoding="utf-8"))
                v = ImportDependencyVisitor()
                v.visit(tree)
                rel = str(py_file.relative_to(SELF_ROOT)) if SELF_ROOT in py_file.parents else py_file.name
                for pkg in v.imported_packages:
                    package_to_files[pkg].append(rel)
            except: continue

    check_health(package_to_files, internal_names)

if __name__ == "__main__":
    main()