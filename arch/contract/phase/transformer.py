# xphi.arch.contract.phase.transformer
import os
import sys
import difflib
import argparse
import libcst as cst
from typing import List, Tuple, Dict, Any, Callable
from pathlib import Path

from xphi.arch.contract.phase.aligner import PhaseAligner, AlignRecord
from xphi.watcher.plane.emitter import get_emitter, flow_scope

def node_to_str(node: cst.CSTNode) -> str:
    return cst.Module([]).code_for_node(node)

def generate_diff(original: str, modified: str, filename: str) -> str:
    """원본 코드와 수정된 코드 간의 Unified Diff를 생성합니다."""
    return "\n".join(
        difflib.unified_diff(
            original.splitlines(),
            modified.splitlines(),
            fromfile=f"{filename} (original)",
            tofile=f"{filename} (modified)",
            lineterm=""
        )
    )

# ==========================================
# CST Transformers (AST 조작)
# ==========================================

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


class RelativeImportTransformer(cst.CSTTransformer):
    def __init__(self, current_package: str):
        """:param current_package: 현재 파일이 속한 패키지의 절대 경로 (예: "title.a.b")"""
        self.current_package = current_package

    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.CSTNode:
        if not original_node.relative:
            return updated_node

        if not self.current_package:
            return updated_node

        dots = len(original_node.relative)
        parts = self.current_package.split(".")
        if dots > len(parts) + 1:
            return updated_node

        slice_idx = len(parts) - (dots - 1)
        base_pkg_parts = parts[:slice_idx]
        module_str = node_to_str(original_node.module) if original_node.module else ""
        if module_str:
            base_pkg_parts.append(module_str)

        absolute_module_name = ".".join(base_pkg_parts)
        return updated_node.with_changes(
            relative=[],
            module=cst.parse_expression(absolute_module_name) if absolute_module_name else None
        )


# ==========================================
# Phase Aligners (파일 스캔 및 오케스트레이션)
# ==========================================

class PrefixImportAligner(PhaseAligner):
    """절대 경로 간의 Prefix 치환(old -> new)을 담당하는 클래스"""
    def __init__(self, root_dir: str, old_prefix: str, new_prefix: str):
        super().__init__(root_dir, emitter_name="aligner.imports.prefix")
        self.old_prefix = old_prefix
        self.new_prefix = new_prefix

    def scan(self, **kwargs) -> Tuple[List[AlignRecord], int, int]:
        mismatches = []
        matched_count = 0
        mismatched_count = 0

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
                            self.emitter.info(f"Mismatch detected in {file}")
                        else:
                            matched_count += 1
                    except Exception as e:
                        self.emitter.error(f"Failed scanning {path}: {e}")
                        continue

        return mismatches, matched_count, mismatched_count


class RelativeImportAligner(PhaseAligner):
    """상대 경로(., ..)를 절대 경로로 풀어주는 변환을 담당하는 클래스"""
    def __init__(self, root_dir: str, project_root: str = None):
        super().__init__(root_dir, emitter_name="relative.imports.aligner")
        self.project_root = Path(project_root).resolve() if project_root else Path.cwd()

    def scan(self, **kwargs) -> Tuple[List[AlignRecord], int, int]:
        mismatches = []
        matched_count = 0
        mismatched_count = 0
        root_path = Path(self.root_dir).resolve()

        with flow_scope(phase="SCAN"):
            for current_root, _, files in os.walk(root_path):
                for file in files:
                    if not file.endswith(".py"):
                        continue
                        
                    file_path = Path(current_root) / file
                    try:
                        rel_path = file_path.relative_to(self.project_root)
                    except ValueError:
                        rel_path = file_path.relative_to(root_path)

                    parent_dir = rel_path.parent
                    current_package = ".".join(parent_dir.parts) if str(parent_dir) != "." else ""
                    
                    try:
                        original = file_path.read_text(encoding="utf-8", errors="ignore")
                        tree = cst.parse_module(original)
                        
                        modified_tree = tree.visit(RelativeImportTransformer(current_package))
                        modified = modified_tree.code

                        if original != modified:
                            diff = generate_diff(original, modified, str(file_path))
                            mismatches.append({
                                "path": str(file_path),
                                "original": original,
                                "modified": modified,
                                "diff": diff,
                                "status": "pending"
                            })
                            mismatched_count += 1
                            self.emitter.info(f"Relative import mapped to absolute in {file}")
                        else:
                            matched_count += 1
                            
                    except Exception as e:
                        self.emitter.error(f"Failed scanning {file_path}: {e}")
                        continue

        return mismatches, matched_count, mismatched_count