# topos.context.assembler
import json
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict
from meta.flow.xphi.xor import Xor, SearchResult
from bound.surface.emitter import get_logger
from bound.resolver import find_current_self, resolve_path
from topos.context.prompt.input import InputBundle, build_prompt 

log = get_logger("context.assembler")

class ContextAssembler: # 변경: PromptSampler -> ContextAssembler
    """
    @topos.bridge: 다양한 데이터 소스(Xor, JSON, YAML)를 수집하여 
                   LLM이 소비할 수 있는 InputBundle로 조립하는 교량.
    """
    def __init__(self):
        self.xor = Xor()
        self.self_root = find_current_self()
        
        # 데이터의 성격에 따라 저장소 경로 분리
        self.blocks_root = resolve_path("blocks")     # 비정형 파편화 문서 (Xor 대상)
        self.schema_root = resolve_path("schemas")    # 정형 구조체 (JSON)
        self.config_root = resolve_path("configs")    # 설정/프롬프트 (YAML)

    def _fetch_xor(self, query: str, top_k: int = 20) -> List[str]:
        """비정형 데이터: Lucene 기반 검색 및 파편화 블록 병합"""
        raw_results = self.xor.search(query)
        return [f"// @source: Xor\n// @relevance: {r.score}\n{r.text}" for r in raw_results[:top_k]]

    def _load_structured_json(self, file_name: str) -> Optional[str]: # 변경: exact -> structured
        """정형 데이터(JSON): 무결성 검증 후 구조화된 텍스트로 반환"""
        file_path = (self.schema_root / file_name).with_suffix('.json')
        if not file_path.exists():
            log.warning(f"JSON schema not found: {file_path}")
            return None
            
        try:
            # 포맷 검증 (Syntax 에러 방지)
            content = file_path.read_text(encoding="utf-8")
            parsed = json.loads(content)
            # LLM이 읽기 좋게 정규화하여 덤프
            normalized_content = json.dumps(parsed, indent=2, ensure_ascii=False)
            return f"// @source: registry/{file_name}.json\n// @format: JSON Schema\n```json\n{normalized_content}\n```"
        except json.JSONDecodeError as e:
            log.error(f"Invalid JSON format in {file_path}: {e}")
            return None

    def _load_structured_yaml(self, file_name: str) -> Optional[str]: # 변경: exact -> structured
        """정형 데이터(YAML): 무결성 검증 후 구조화된 텍스트로 반환"""
        file_path = (self.config_root / file_name).with_suffix('.yaml')
        if not file_path.exists():
            file_path = (self.config_root / file_name).with_suffix('.yml')
            if not file_path.exists():
                log.warning(f"YAML config not found: {file_name}")
                return None
                
        try:
            content = file_path.read_text(encoding="utf-8")
            # 포맷 검증
            parsed = yaml.safe_load(content)
            normalized_content = yaml.dump(parsed, allow_unicode=True, sort_keys=False)
            return f"// @source: configs/{file_name}.yaml\n// @format: YAML Config\n```yaml\n{normalized_content}\n```"
        except yaml.YAMLError as e:
            log.error(f"Invalid YAML format in {file_path}: {e}")
            return None

    def assemble(self, 
                 query: str, 
                 anchor: str, 
                 state: List[str], 
                 structured_json: Optional[List[str]] = None,  # 타입 힌트 및 파라미터명 개선
                 structured_yaml: Optional[List[str]] = None,  # 타입 힌트 및 파라미터명 개선
                 use_xor: bool = True) -> List[Dict]:
        """
        요청된 포맷(JSON, YAML, Xor)에 따라 컨텍스트를 동적으로 라우팅하고 조립합니다.
        """
        evidence_list = []

        # 1. JSON 구조체 투영 (가장 높은 우선순위)
        if structured_json:
            for j_file in structured_json:
                data = self._load_structured_json(j_file)
                if data: evidence_list.append(data)

        # 2. YAML 설정/프롬프트 투영
        if structured_yaml:
            for y_file in structured_yaml:
                data = self._load_structured_yaml(y_file)
                if data: evidence_list.append(data)

        # 3. 비정형 지식 검색 투영 (Xor)
        if use_xor and query:
            x_data = self._fetch_xor(query)
            if x_data: evidence_list.extend(x_data)

        # 4. InputBundle 구성 (캡슐화)
        bundle = InputBundle(
            anchor=anchor,
            query=query,
            state=state,
            evidence=evidence_list,
            max_tokens=8000
        )

        # 조립된 Bundle을 최종 LLM 프롬프트 형태로 렌더링
        return build_prompt(bundle)

if __name__ == "__main__":
    assembler = ContextAssembler()
    messages = assembler.assemble(
        query="context assemble의 위상적 재정의",
        anchor="당신은 meta.self의 심층 분석 에이전트입니다.",
        state=["current_phase=testing"]
    )
    print(json.dumps(messages, indent=2, ensure_ascii=False))