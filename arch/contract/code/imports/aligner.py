# arch.contract.code.imports.aligner
import os
import sys
import difflib
import argparse
import libcst as cst
from typing import List, Dict, Any, Tuple, Callable
from pathlib import Path
from arch.surface.aligner import FieldAligner, AlignRecord
from topos.bound.plane.emitter import get_emitter, flow_scope
from arch.contract.code.imports.transformer import ImportTransformer

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

class ImportAligner(FieldAligner):
    def __init__(self, root_dir: str, old_prefix: str, new_prefix: str):
        super().__init__(root_dir)
        self.old_prefix = old_prefix
        self.new_prefix = new_prefix
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
                            ## 발견 사실을 발화 (파일이 많으면 알아서 폴딩됨)
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

        ## ALIGN 위상 스코프 진입 (중첩 맥락)
        with flow_scope(phase="ALIGN", mode="apply" if apply_changes else "dry_run"):
            for record in mismatches:
                path_str = record["path"]
                modified_code = record["modified"]
                
                if apply_changes:
                    try:
                        Path(path_str).write_text(modified_code, encoding="utf-8")
                        record["status"] = "applied"
                        ## 비가역적 상태 변이이므로 가장 강력한 위상 기호(crit) 사용
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
