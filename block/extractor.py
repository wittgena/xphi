# block.extractor
import re
import sys
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Union, Dict
from plane.emitter import get_logger
from anchor.resolver import find_current_self, resolve_path
from model.block import MdDocument, MdSection, MdNode, CodeBlock, Paragraph
from block.parser.md import MdAstParser
from block.parser.py import PyAstParser
from block.parser.kt import KtAstParser 

log = get_logger("block.extractor")

class BlockExtractor:
    def __init__(self):
        self.counter = 0

    def extract(self, doc: MdDocument) -> List[dict]:
        blocks = []
        source_type = doc.path.suffix.lstrip(".") or "txt"

        def walk(section: MdSection, parent_path="", depth=0):
            section_path = (
                f"{parent_path}/{section.title}"
                if parent_path else section.title
            )

            for node in section.children:
                block_type = None
                content = None
                dsl_name = None

                if isinstance(node, Paragraph):
                    block_type = "paragraph"
                    content = node.text.strip()

                elif isinstance(node, CodeBlock):
                    block_type = node.lang
                    content = node.content.strip()

                    # DSL name extraction
                    if content.startswith("@"):
                        first_line = content.splitlines()[0]
                        dsl_name = first_line.split()[0].strip("@")

                if content: 
                    block = {
                        "block_id": f"{doc.path}::{self.counter}",
                        "file_path": str(doc.path),
                        "source_type": source_type,
                        "section": section.title,
                        "section_path": section_path,
                        "section_depth": depth,
                        "block_type": block_type,
                        "meta": section.meta_tag,
                        "order_index": self.counter,
                        "symbols": self._extract_symbols(section, block_type),
                        "content": content,
                    }

                    if dsl_name:
                        block["dsl_name"] = dsl_name

                    blocks.append(block)
                    self.counter += 1

            for sub in section.subsections:
                walk(sub, section_path, depth + 1)

        for root in doc.sections:
            walk(root, depth=0)

        return blocks

    def _extract_symbols(self, section: MdSection, block_type: str) -> List[str]:
        symbols = []
        if block_type:
            symbols.append(block_type)

        tokens = re.split(r"[^\w\.]+", section.title)
        symbols.extend([t for t in tokens if t])

        if section.meta_tag:
            meta_tokens = re.split(r"[^\w\.]+", str(section.meta_tag))
            symbols.extend([t for t in meta_tokens if t and t not in ("features", "refs")])

        return list(set(symbols))

def extract_block_from_file(path: Path, kt_contracts: dict = None) -> List[dict]:
    """파일의 AST를 파싱하고 Extractor를 통해 JSON 직렬화 가능한 dict 리스트"""
    if path.suffix == ".md":
        parser = MdAstParser(path)
    elif path.suffix == ".py":
        parser = PyAstParser(path)
    elif path.suffix == ".kt":
        file_contracts = kt_contracts.get(str(path.absolute()), []) if kt_contracts else []
        parser = KtAstParser(path, file_contracts)
    else:
        log.warning(f"[skip] 지원하지 않는 확장자: {path}")
        return []

    doc = parser.parse()
    extractor = BlockExtractor()
    return extractor.extract(doc)
