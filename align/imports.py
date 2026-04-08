# align.imports
import os
import difflib
import argparse
import libcst as cst
from typing import List, Dict, Any, Tuple, Callable
from pathlib import Path
from plane.emitter import get_emitter, flow_scope
from flow.surface.aligner import FieldAligner, AlignRecord

# CLI 진입점용 글로벌 발화기
emitter = get_emitter("flow.align", phase="SYSTEM")

## Utilities
def node_to_str(node: cst.CSTNode) -> str:
    return cst.Module([]).code_for_node(node)

def generate_diff(original: str, modified: str, filename: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            original.splitlines(),
            modified.splitlines(),
            fromfile=f"{filename} (original)",
            tofile=f"{filename} (modified)",
            lineterm=""
        )
    )

## Transformer (기존과 동일)
class ImportTransformer(cst.CSTTransformer):
    def __init__(self, old: str, new: str):
        self.old = old
        self.new = new

    def _should_replace(self, module_name: str) -> bool:
        return module_name == self.old or module_name.startswith(self.old + ".")

    def _replace(self, module_name: str) -> str:
        return self.new + module_name[len(self.old):]

    def leave_ImportFrom(self, original_node, updated_node):
        if original_node.module is None:
            return updated_node

        module_name = node_to_str(original_node.module)
        if self._should_replace(module_name):
            new_module = self._replace(module_name)
            return updated_node.with_changes(
                module=cst.parse_expression(new_module)
            )
        return updated_node

    def leave_Import(self, original_node, updated_node):
        new_names = []
        for alias in original_node.names:
            name_str = node_to_str(alias.name)
            if self._should_replace(name_str):
                new_name = self._replace(name_str)
                new_alias = alias.with_changes(name=cst.parse_expression(new_name))
                new_names.append(new_alias)
            else:
                new_names.append(alias)
        return updated_node.with_changes(names=new_names)


## Aligner Implementation
class ImportAligner(FieldAligner):
    def __init__(self, root_dir: str, old_prefix: str, new_prefix: str):
        super().__init__(root_dir)
        self.old_prefix = old_prefix
        self.new_prefix = new_prefix
        # [Topological Sensor] Aligner 전용 발화기 장착
        self.emitter = get_emitter("aligner.core", boundary=root_dir)

    def scan(self, **kwargs) -> Tuple[List[AlignRecord], int, int]:
        mismatches = []
        matched_count = 0
        mismatched_count = 0

        # [Topology] SCAN 위상 스코프 진입
        with flow_scope(phase="SCAN"):
            for root, _, files in os.walk(self.root_dir):
                for file in files:
                    if not file.endswith(".py"):
                        continue
                    
                    path = Path(root) / file
                    try:
                        original = path.read_text(encoding="utf-8", errors="ignore")
                        tree = cst.parse_module(original)
                        modified_tree = tree.visit(
                            ImportTransformer(self.old_prefix, self.new_prefix)
                        )
                        modified = modified_tree.code

                        if original != modified:
                            diff = generate_diff(original, modified, str(path))
                            mismatches.append({
                                "path": str(path),
                                "original": original,
                                "modified": modified,
                                "diff": diff,
                                "status": "pending"
                            })
                            mismatched_count += 1
                            # 발견 사실을 발화 (파일이 많으면 알아서 폴딩됨)
                            self.emitter.info(f"Mismatch detected in {file}")
                        else:
                            matched_count += 1
                    except Exception as e:
                        self.emitter.error(f"Failed scanning {path}: {e}")
                        continue

        return mismatches, matched_count, mismatched_count

    def align(self, mismatches: List[AlignRecord], **kwargs) -> List[AlignRecord]:
        apply_changes = kwargs.get("apply", False)
        results = []

        # [Topology] ALIGN 위상 스코프 진입 (중첩 맥락)
        with flow_scope(phase="ALIGN", mode="apply" if apply_changes else "dry_run"):
            for record in mismatches:
                path_str = record["path"]
                modified_code = record["modified"]
                
                if apply_changes:
                    try:
                        Path(path_str).write_text(modified_code, encoding="utf-8")
                        record["status"] = "applied"
                        # 비가역적 상태 변이이므로 가장 강력한 위상 기호(crit) 사용
                        self.emitter.crit(f"Updated: {path_str}") 
                    except Exception as e:
                        record["status"] = f"failed: {e}"
                        self.emitter.error(f"Failed to write {path_str}: {e}")
                else:
                    record["status"] = "dry_run"
                    # 기존 debug 대신, info 또는 signal을 사용하여 의도 표현
                    self.emitter.info(f"Dry-run, would update: {path_str}")
                
                results.append(record)

        return results


def main():
    parser = argparse.ArgumentParser(description="Safely refactor Python import paths using LibCST and BaseAligner.")
    parser.add_argument("--dir", required=True, help="Target directory")
    parser.add_argument("--old", required=True, help="Old module prefix")
    parser.add_argument("--new", required=True, help="New module prefix")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default is dry-run)")
    args = parser.parse_args()

    # CLI 실행도 전체 흐름(Flow)의 일부로 묶어 관리
    with flow_scope(flow_id="ALIGN_JOB", phase="CLI"):
        emitter.signal(f"Initiating Import Alignment: '{args.old}' -> '{args.new}'")
        aligner = ImportAligner(root_dir=args.dir, old_prefix=args.old, new_prefix=args.new)
        group_keys = { "by_directory": lambda r: str(Path(r["path"]).parent) }
        result = aligner.run(
            axis="by_directory",
            group_keys=group_keys,
            scan_kwargs={},
            fix_kwargs={"apply": args.apply}
        )

        # 시스템 요약 전 잔류 캐시 방출 (Flush)
        emitter.flush()
        print("\n" + "="*50)
        print("Alignment Summary:")
        print(f"Total Files: {result['summary']['total_files']}")
        print(f"Matched (No change needed): {result['summary']['matched']}")
        print(f"Mismatched (To be updated): {result['summary']['mismatched']}")
        print("="*50)

        for cluster in result["clusters"]:
            print(f"\nDirectory: {cluster['path']}")
            for item in cluster["items"]:
                print(f"  - File: {Path(item['path']).name} [Status: {item['status']}]")
                if not args.apply:
                    print(item.get("diff", ""))
                    print("-" * 40)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        emitter.warn("Alignment aborted by user.")
        emitter.flush()