# meta.scan.spec
## @lineage: loop.debug.fragment.detect
"""
@phase: Topos Reflection & Fragment Isolation
@flow: Manifold Discovery -> AST Traversal -> Delta Resolution -> Materialize/Marker Injection
@desc: identify decoupled nodes (fragments) and compile them into an executable payload registry
"""
import os
import sys
import ast
import json
import argparse
from collections import defaultdict
from typing import List, Dict, Any
from pathlib import Path
from phase.bound.resolver import find_current_self, resolve_path
from arch.contract.registry.unified import contract
from phase.plane.emitter import get_emitter
from phase.runtime.cli.executor import execute_cli_task, CliTaskAdapter, dispatch_cli, parse_local

log = get_emitter("scan.spec")

try:
    SELF_ROOT = find_current_self()
    SPEC_ROOT = resolve_path('workspace') / "spec"
except Exception as e:
    log.error(f"기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)


class CodeAnalyzer(ast.NodeVisitor):
    """@desc: traverses the syntax tree to separate established structures"""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.defined_nodes = []
        self.used_names = set()

    def _is_dunder(self, name: str) -> bool:
        return name.startswith("__") and name.endswith("__")

    def _is_registry_bound(self, node) -> bool:
        """@point: Framework bindings (e.g., @contract, @step) act as implicit invocations in the runtime phase"""
        for decorator in node.decorator_list:
            try:
                dec_str = ast.unparse(decorator)
                if any(kw in dec_str for kw in ("contract", "register", "step")):
                    return True
            except AttributeError:
                func = decorator.func if isinstance(decorator, ast.Call) else decorator
                if isinstance(func, ast.Attribute) and getattr(func.value, 'id', '') in ("contract", "step"):
                    return True
                if isinstance(func, ast.Name) and any(kw in func.id for kw in ("contract", "register", "step")):
                    return True
        return False

    def _is_framework_hook(self, name: str) -> bool:
        """@point: Shields dynamic lifecycle hooks (TUI, Validation, Sockets) from false positive detection"""
        if name.startswith(("on_", "action_")) or name == "compose":
            return True
        if name.startswith(("_val_", "_ser_", "_validate", "_coerce")):
            return True
        if name.startswith(("visit_", "test_")) or name in ("setUp", "tearDown"):
            return True
        if name in ("alive", "health", "initialized", "closed"):
            return True
            
        return False

    def visit_FunctionDef(self, node):
        if not self._is_dunder(node.name) and not self._is_registry_bound(node) and not self._is_framework_hook(node.name):
            self.defined_nodes.append({
                'type': 'Function',
                'name': node.name,
                'file': self.filepath,
                'lineno': node.lineno,
                'col_offset': node.col_offset
            })
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        if not self._is_dunder(node.name) and not self._is_registry_bound(node) and not self._is_framework_hook(node.name):
            self.defined_nodes.append({
                'type': 'AsyncFunction',
                'name': node.name,
                'file': self.filepath,
                'lineno': node.lineno,
                'col_offset': node.col_offset
            })
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        if not self._is_registry_bound(node):
            self.defined_nodes.append({
                'type': 'Class',
                'name': node.name,
                'file': self.filepath,
                'lineno': node.lineno,
                'col_offset': node.col_offset
            })
        self.generic_visit(node)

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self.used_names.add(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        self.used_names.add(node.attr)
        self.generic_visit(node)


class FragmentDetector:
    """@desc: detect, catalog, and isolate decoupled Fragments"""
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir

    def _find_files(self, target_path: str) -> List[Path]:
        if "." in target_path and "/" not in target_path:
            normalized_path = target_path.replace(".", "/")
        else:
            normalized_path = target_path

        base_path = self.root_dir / normalized_path if not os.path.isabs(normalized_path) else Path(normalized_path)
        if not base_path.exists():
            log.error(f"[error] 경로를 찾을 수 없습니다: {base_path}")
            return []
            
        if base_path.is_file():
            return [base_path] if base_path.suffix == ".py" else []
            
        return list(base_path.rglob("*.py"))

    def _apply_tags(self, fragments_by_file: Dict[str, List[Dict[str, Any]]]):
        log.info("## @tagging: 태그 삽입 작업 시작...")
        for filepath, nodes in fragments_by_file.items():
            nodes.sort(key=lambda x: x['lineno'], reverse=True)
            path_obj = Path(filepath)
            lines = path_obj.read_text(encoding="utf-8").splitlines(keepends=True)
            tagged_count = 0
            
            for node in nodes:
                idx = node['lineno'] - 1
                if idx > 0 and "## @fragment.detect" in lines[idx - 1]:
                    continue
                
                indent = " " * node['col_offset']
                lines.insert(idx, f"{indent}## @fragment.detect\n")
                tagged_count += 1
                
            if tagged_count > 0:
                path_obj.write_text("".join(lines), encoding="utf-8")
                display_path = path_obj.resolve()
                log.info(f"  - [tagged] {display_path}: {tagged_count}개 삽입 완료")

    def run(self, target_path: str, apply_tag: bool):
        log.info(f"[detect] start (path={target_path}, tag={apply_tag})")
        target_name = target_path.replace("/", "_")

        ## @step.1: Bound extraction -> Discover target manifolds
        py_files = self._find_files(target_path)
        if not py_files:
            return {"status": "fail", "reason": "No python files found"}

        log.info(f"## @scan: {len(py_files)}개의 파일 스캔")
        all_defined = []
        all_used = set()

        ## @step.2: Syntax tree parsing & topological mapping
        for filepath in py_files:
            try:
                source = filepath.read_text(encoding="utf-8")
                tree = ast.parse(source, filename=str(filepath))
                analyzer = CodeAnalyzer(str(filepath))
                analyzer.visit(tree)
                all_defined.extend(analyzer.defined_nodes)
                all_used.update(analyzer.used_names)
            except SyntaxError:
                log.warning(f"[skip] 문법 오류 건너뜀: {filepath.name}")
            except Exception as e:
                log.error(f"[error] 파싱 오류 ({filepath.name}): {e}")

        ## @step.3: Delta resolution -> Isolate decoupled nodes
        fragments = [node for node in all_defined if node['name'] not in all_used]
        
        out_file = SPEC_ROOT / "fragments" / f"{target_name}.json"
        try:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump({
                    "manifest": "dormant_payload_registry",
                    "total_count": len(fragments),
                    "fragments": fragments
                }, f, indent=2, ensure_ascii=False)
            log.info(f"## @material: 고아 카탈로그 덤프 완료 -> {out_file.name}")
        except Exception as e:
            log.error(f"[error] fragments.json 저장 실패: {e}")

        if not fragments:
            log.info("## @result: Fragment 미발견")
            return {"status": "success", "fragments": 0, "registry_path": str(out_file)}

        log.warning(f"## @result: {len(fragments)}개의 Dormant Payload 발견:")
        fragments_by_file = defaultdict(list)
        for node in fragments:
            file_path_obj = Path(node['file'])
            display_path = file_path_obj.resolve()
            log.warning(f"  - [{node['type']}] {node['name']} (File: {display_path}, Line: {node['lineno']})")
            
            if apply_tag:
                fragments_by_file[node['file']].append(node)

        if apply_tag:
            self._apply_tags(fragments_by_file)
            log.info("## @tagging: 태깅 작업이 완료")
            
        return {
            "status": "success", 
            "fragments": len(fragments), 
            "tagged": apply_tag,
            "registry_path": str(out_file)
        }


def entry_task(args):
    parser = argparse.ArgumentParser(description="Fragment Code Detector & Payload Registry Generator")
    parser.add_argument("--path", required=True, type=str, help="탐색할 기준 경로")
    parser.add_argument("--tag", action="store_true", help="태그 추가 여부")
    parsed_args = parser.parse_args(args)
    detector = FragmentDetector(SELF_ROOT)
    run_kwargs = {
        "target_path": parsed_args.path,
        "apply_tag": parsed_args.tag,
    }
    return CliTaskAdapter(detector.run, **run_kwargs)

@contract.cli(name="scan.fragment", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("scan.fragment", entry_task, __file__)

if __name__ == "__main__":
    main()