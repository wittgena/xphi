# contract.xor.agent.cache
import os
import sys
import re
import json
import hashlib
import argparse
import fnmatch
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple
from google import genai
from google.genai import types
from bound.resolver import find_current_self, resolve_path, get_invoker
from contract.registry import contract
from flow.surface.emitter import get_emitter
from flow.surface.projector import SurfaceProjector
from contract.executor.cli import execute_cli_task, CliTaskAdapter, dispatch_cli, parse_local
from contract.block.parser.py import PyDotMdParser

SELF_ROOT = find_current_self()
SUBST_ROOT = resolve_path('subst')
log = get_emitter("context.cache")

DELIMITER = "---"

class SubstCacheProjector(SurfaceProjector[Path, Optional[Dict[str, Any]], Tuple[str, str], Dict[str, str]]):
    """
    Projector implementation that aggregates repository code, 
    projects it into structured markdown, and freezes it into Gemini Context Cache.
    """
    def __init__(self, target_repo: str, model_name: str = "gemini-3-flash", ttl_hours: int = 2):
        try:
            self.self_root = SELF_ROOT
            self.subst_root = SUBST_ROOT
        except Exception as e:
            log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
            sys.exit(1)

        self.target_repo = target_repo
        self.merge_root = self.self_root / target_repo
        self.registry_path = self.subst_root / "project" / "cache_registry.json"
        self.gemini_md_path = self.self_root / ".gemini" / "GEMINI.md"

        self.model_name = model_name
        self.ttl_seconds = ttl_hours * 3600

        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log.error("[error] GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")
            sys.exit(1)
        self.client = genai.Client(api_key=api_key)

        if not self.merge_root.exists():
            log.info(f"[error] 입력 경로 없음: {self.merge_root}")
            sys.exit(1)

        self.ignore_patterns = self._load_gitignore_patterns()

    def _load_gitignore_patterns(self) -> List[str]:
        patterns = ["*.jar", "*.class", "*.log", "*.pyc", "__pycache__/**", ".venv/**", ".git/**", ".DS_Store"]
        gitignore_path = self.merge_root / ".gitignore"
        if gitignore_path.exists():
            with open(gitignore_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        patterns.append(line)
        return patterns

    def _should_exclude(self, path: Path) -> bool:
        rel_path = str(path.relative_to(self.merge_root))
        for pattern in self.ignore_patterns:
            if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(path.name, pattern):
                return True
        if any(p.startswith(".") for p in path.relative_to(self.merge_root).parts): 
            return True
        return False

    def select(self, topos: List[Path], context: Optional[Dict[str, Any]] = None) -> List[Path]:
        return [p for p in topos if p.is_file() and not self._should_exclude(p)]

    def project(self, subgraph: List[Path], context: Optional[Dict[str, Any]] = None) -> List[Tuple[str, str]]:
        representations = []
        for path in subgraph:
            try:
                rel_path = path.relative_to(self.merge_root)
                key = "subst" # 모든 파일을 하나의 기질로 묶기 위해 단일 key 사용
                
                if path.suffix == ".py":
                    try:
                        parser = PyDotMdParser(path)
                        doc = parser.parse()
                        # 임시로 raw text 추출 (실제 AST 렌더링 로직은 기존 Bundler 참조)
                        content = path.read_text(encoding="utf-8", errors="replace").strip()
                    except Exception as e:
                        content = f"```python\n{path.read_text(encoding='utf-8', errors='replace')}\n```"
                else:
                    content = path.read_text(encoding="utf-8", errors="replace").strip()

                final_block = f"\n{DELIMITER} MODULE_PATH: {rel_path} {DELIMITER}\n{content}\n"
                representations.append((key, final_block))
            except Exception as e:
                log.error(f"[PROJECT ERROR] {path}: {e}")
        return representations

    def assemble(self, representations: List[Tuple[str, str]], context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        groups = defaultdict(list)
        for key, text in representations:
            groups[key].append(text)
        
        subst_text = "SYSTEM SUBST START\n" + "\n".join(groups["subst"]) + "\n[SYSTEM SUBST END]"
        return {"subst": subst_text}

    def emit(self, surface: Dict[str, str], context: Optional[Dict[str, Any]] = None) -> None:
        """조립된 Surface를 파일이 아닌 Gemini Cache API로 '투영(Emit)'하고 상태를 갱신"""
        subst_text = surface.get("subst", "")
        if not subst_text:
            log.warning("생성된 Subst가 없습니다.")
            return

        content_hash = hashlib.sha256(subst_text.encode('utf-8')).hexdigest()
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        registry = json.loads(self.registry_path.read_text()) if self.registry_path.exists() else {}

        # 1. 멱등성 검증 (Hash 기반 TTL 연장)
        if self.target_repo in registry and registry[self.target_repo].get('hash') == content_hash:
            cache_id = registry[self.target_repo]['cache_id']
            try:
                self.client.caches.patch(
                    name=cache_id,
                    config=types.UpdateCachedContentConfig(ttl=f"{self.ttl_seconds}s")
                )
                log.info(f"[+] Cache 갱신됨 (No Hash Change): {cache_id}")
                return
            except Exception as e:
                log.info(f"[!] 기존 캐시 만료 혹은 갱신 실패. 새로 생성합니다. ({e})")

        # 2. 신규 Cache 생성 (결빙)
        log.info(f"[*] New Subst detected for {self.target_repo}. Freezing...")
        system_instruction = "당신은 meta.self 시스템의 자가 인지 기질입니다. 주입된 모듈 간의 위상 정합성을 유지하며 추론하십시오."
        
        cache = self.client.caches.create(
            model=self.model_name,
            config=types.CreateCachedContentConfig(
                contents=[types.Content(role="user", parts=[types.Part.from_text(text=subst_text)])],
                system_instruction=system_instruction,
                ttl=f"{self.ttl_seconds}s"
            )
        )

        # 3. Registry 및 GEMINI.md 동기화 (CLI 접점)
        registry[self.target_repo] = {
            "cache_id": cache.name,
            "hash": content_hash,
            "updated_at": datetime.now().isoformat()
        }
        self.registry_path.write_text(json.dumps(registry, indent=4))
        
        self.gemini_md_path.parent.mkdir(parents=True, exist_ok=True)
        self.gemini_md_path.write_text(f"# SYSTEM SUBST (Cache ID: {cache.name})\n\n{subst_text}", encoding='utf-8')

        log.info(f"[+] Success. Subst Locked: {cache.name}")
        log.info(f"[>] Synchronized with: {self.gemini_md_path}")

    def compile(self) -> None:
        """Pipeline Execution"""
        topos = list(self.merge_root.rglob("*"))
        selected = self.select(topos)
        projected = self.project(selected)
        assembled = self.assemble(projected)
        self.emit(assembled)

def entry_task(args):
    parser = argparse.ArgumentParser(description="Freeze project topos into Gemini Cache Subst")
    parser.add_argument("--repo", type=str, required=True, help="Target input path. E.g., flow/dev")
    parser.add_argument("--ttl", type=int, default=2, help="Cache Time-To-Live in hours.")
    parsed_args = parser.parse_args(args)
    
    projector = SubstCacheProjector(target_repo=parsed_args.repo, ttl_hours=parsed_args.ttl)
    return CliTaskAdapter(projector.compile)

@contract.cli(name="gemini.cache", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("gemini.cache", entry_task, __file__)

if __name__ == "__main__":
    main()