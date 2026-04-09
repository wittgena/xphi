# model.topos.model
# @py.start
import os
import sys
import re
import asyncio
import argparse
import fnmatch
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Tuple, Dict, Any
from pathlib import Path
from collections import defaultdict
from plane.emitter import get_logger
from anchor.resolver import find_current_self, resolve_path, get_invoker
from block.parser.py import PyDotMdParser 
from bridge.executor.cli import execute_cli_task, CliTaskAdapter
from flow.surface.compiler import SurfaceCompiler

log = get_logger("align.modeler")

DELIMITER = "---"

class ModelAligner(SurfaceCompiler[Path, Tuple[str, str], Dict[str, str]]):
    def __init__(self, target_dir: str):
        try:
            self.self_root = find_current_self()
            self.model_root = resolve_path('cache')
        except Exception as e:
            log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
            sys.exit(1)

        self.merge_root = self.self_root / target_dir
        path_name = f"model_{target_dir}" if target_dir not in ['.', '/', './'] else 'self'
        self.emit_root = self.model_root / path_name

        if not self.merge_root.exists():
            log.info(f"[error] 입력 경로 없음: {self.merge_root}")
            sys.exit(1)

        self.ignore_patterns = self._load_gitignore_patterns()
        log.info(f"[merge.from] {self.merge_root}")
        log.info(f"[emit.to] {self.emit_root}")

    def _load_gitignore_patterns(self) -> List[str]:
        patterns = [
            "*.jar", "*.class", "*.log", "*.pyc", "*.exe", "build/**", "gradle/**", "gradlew", "gradlew.bat",
            "__pycache__/**", ".venv/**", ".idea/**", ".git/**", ".DS_Store", "node_modules/**", ".tab"
        ]
        for gitignore_path in [self.merge_root / ".gitignore", Path.home() / ".gitignore"]:
            if gitignore_path.exists():
                with open(gitignore_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            patterns.append(line)
                break
        log.info(f"[IGNORE] 적용된 패턴 수: {len(patterns)}")
        return patterns

    def _is_binary_file(self, path: Path, sample_size: int = 1024) -> bool:
        if path.suffix == '.md': return False
        try:
            with path.open("rb") as f:
                chunk = f.read(sample_size)
                if b'\0' in chunk: return True
                text_ratio = sum(32 <= b <= 126 or b in (9, 10, 13) for b in chunk) / max(len(chunk), 1)
                return text_ratio < 0.8
        except Exception as e:
            log.error(f"[BINARY CHECK ERROR] {path}: {e}")
            return True # 오류 시 보수적으로 제외

    def _should_exclude(self, path: Path) -> bool:
        rel_path = str(path.relative_to(self.merge_root))
        rel_path_obj = path.relative_to(self.merge_root)

        for pattern in self.ignore_patterns:
            if pattern.endswith("/**") and rel_path_obj.match(pattern): return True
            elif pattern.endswith("/") and rel_path.startswith(pattern): return True
            elif fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(path.name, pattern): return True

        if any(p.startswith(".") for p in rel_path_obj.parts): return True
        if self._is_binary_file(path): return True
        return False

    def _extract_title(self, content: str, fallback: str, prefix="@phase") -> str:
        lines = content.splitlines()
        for line in lines:
            if re.match(r"^#\s+\S", line): return f"## {prefix}: {line.lstrip('#').strip()}"
        for line in lines:
            if re.match(r"^##\s+\S", line): return f"## {prefix}: {line.lstrip('#').strip()}"
        return f"## @file: {Path(fallback).stem.replace('_', ' ')}"

    def _get_group_key(self, md_path: Path, level: int = 1) -> str:
        rel = md_path.relative_to(self.merge_root)
        parts = rel.parts[:-1]
        return "root" if not parts else ".".join(parts[:level])

    # [추가됨] MdDocument AST 트리를 순회하며 마크다운 텍스트로 렌더링하는 재귀 함수
    def _render_md_node(self, node: Any) -> str:
        node_type = type(node).__name__
        if node_type == "MdDocument":
            return "".join(self._render_md_node(sec) for sec in getattr(node, 'sections', []))
        elif node_type == "MdSection":
            res = [f"{'#' * node.level} {node.title}\n\n"]
            for child in getattr(node, 'children', []):
                res.append(self._render_md_node(child))
            for sub in getattr(node, 'subsections', []):
                res.append(self._render_md_node(sub))
            return "".join(res)
        elif node_type == "Paragraph":
            return f"{node.text}\n\n"
        elif node_type == "CodeBlock":
            content = node.content if node.content.endswith('\n') else node.content + '\n'
            return f"```{node.lang}\n{content}```\n\n"
        return ""

    def scan(self) -> List[Path]:
        return list(self.merge_root.rglob("*"))

    def filter(self, topos: List[Path]) -> List[Path]:
        return [p for p in topos if p.is_file() and not self._should_exclude(p)]

    def project(self, skeleton: List[Path]) -> List[Tuple[str, str]]:
        representations = []
        for path in skeleton:
            try:
                key = self._get_group_key(path)
                
                if path.suffix == ".py":
                    try:
                        # [수정됨] 객체 지향 Parser 활용
                        parser = PyDotMdParser(path)
                        doc = parser.parse()
                        
                        root = getattr(doc, 'sections', [])[0]
                        
                        # Fallback 트리거를 위한 필수 요소 검증 (desc와 코드 블록 유무)
                        has_desc = any(sub.title == "@desc" for sub in getattr(root, 'subsections', []))
                        has_script = any(
                            sub.title == "py.script" and any(type(c).__name__ == "CodeBlock" and c.content.strip() for c in getattr(sub, 'children', []))
                            for sub in getattr(root, 'subsections', [])
                        )

                        if not has_desc or not has_script:
                            raise ValueError("dotmd 요소 부족 (fallback 발생)")

                        # AST를 마크다운 문자열로 렌더링
                        content = self._render_md_node(doc).strip()
                        
                    except Exception as e:
                        ## Fallback: Plain python code
                        log.debug(f"[FALLBACK] {path.name}: {e}")
                        content = f"```python\n{path.read_text(encoding='utf-8', errors='replace')}\n```"
                else:
                    ## 일반 텍스트
                    content = path.read_text(encoding="utf-8", errors="replace").strip()

                title_line = self._extract_title(content, path.name)
                final_block = f"{DELIMITER} {title_line}\n\n{content}\n"
                representations.append((key, final_block))
                
            except Exception as e:
                log.error(f"[PROJECT ERROR] {path}: {e}")
                
        return representations

    def assemble(self, representations: List[Tuple[str, str]]) -> Dict[str, str]:
        groups = defaultdict(list)
        for key, text in representations:
            groups[key].append(text)
        return {key: "\n".join(texts) for key, texts in groups.items()}

    def emit(self, projection: Dict[str, str]) -> None:
        self.emit_root.mkdir(parents=True, exist_ok=True)
        for key, content in projection.items():
            out_path = self.emit_root / f"{key}.md"
            try:
                out_path.write_text(content, encoding="utf-8")
                log.info(f"[WRITE] → {out_path}")
            except Exception as e:
                log.error(f"[WRITE ERROR] {out_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Compile project topos into a grouped markdown.")
    parser.add_argument("--dir", type=str, required=True, help="Target input path. E.g., flow/dev")
    args = parser.parse_args()
    
    invoker_path = os.path.abspath(__file__)
    module_name = __name__ if __name__ != "__main__" else "anchor.modeler"

    compiler = ModelAligner(target_dir=args.dir)
    task = CliTaskAdapter(compiler.compile)

    invoker, command = get_invoker(Path(__file__))
    payload={"_context": {"invoker": str(invoker), "command": command, "cli_args": sys.argv[1:]}}
    execute_cli_task(task_instance=task, command_name=module_name, payload=payload)

if __name__ == "__main__":
    log.info(f"[AUG] anchor model compiler :: sys.argv = {sys.argv}")
    main()