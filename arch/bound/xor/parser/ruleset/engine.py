# xphi.arch.bound.xor.parser.ruleset.engine
import json
import re
import orjson
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generic, TypeVar, Tuple, Callable
from xphi.arch.bound.xor.secret.redact import redact_string, sanitize_payload
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ruleset.engine")

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')
EngineT = TypeVar('EngineT')

class CompiledEngine(ABC, Generic[InputT, OutputT]):
    @abstractmethod
    def execute(self, payload: InputT) -> OutputT:
        pass

class AbstractRulesetParser(ABC, Generic[EngineT]):
    @abstractmethod
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> EngineT:
        pass

class FastRegexRedactionEngine(CompiledEngine[bytes, bytes]):
    """@desc: 외부 모듈(redact_string) 또는 주입된 정규식을 통해 바이트 스트림 마스킹"""
    def __init__(self, patterns: Optional[List[str]] = None):
        self._use_custom_patterns = bool(patterns)
        if self._use_custom_patterns:
            self._compiled_regexes = [re.compile(p.encode('utf-8'), re.IGNORECASE) for p in patterns]
            self.mask_token = b"***REDACTED***"

    def execute(self, payload: bytes) -> bytes:
        if self._use_custom_patterns:
            redacted = payload
            for r in self._compiled_regexes:
                redacted = r.sub(self.mask_token, redacted)
            return redacted
        else:
            try:
                return redact_string(payload.decode('utf-8')).encode('utf-8')
            except UnicodeDecodeError:
                return payload

class StructuralRedactionEngine(CompiledEngine[bytes, bytes]):
    """@desc: JSON 파싱 후 마스킹. 실패 시 컴파일 시 주입된 fallback 엔진으로 위임"""
    def __init__(self, fallback_engine: CompiledEngine[bytes, bytes]):
        self._fallback = fallback_engine

    def execute(self, payload: bytes) -> bytes:
        try:
            parsed_obj = orjson.loads(payload)
            sanitized_obj = sanitize_payload(parsed_obj)
            return orjson.dumps(sanitized_obj)
        except orjson.JSONDecodeError:
            return self._fallback.execute(payload)

class StructuralExtractionEngine(CompiledEngine[bytes, Dict[str, Any]]):
    def __init__(self, extract_paths: Dict[str, List[str]]):
        self.paths = extract_paths

    def execute(self, payload: bytes) -> Dict[str, Any]:
        try:
            parsed = orjson.loads(payload)
            result = {}
            for field_name, path_list in self.paths.items():
                val = parsed
                for p in path_list:
                    if isinstance(val, dict) and p in val:
                        val = val[p]
                    else:
                        val = None
                        break
                result[field_name] = val
            return result
        except orjson.JSONDecodeError:
            return {}

class StructuralExtractionParser(AbstractRulesetParser[StructuralExtractionEngine]):
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> StructuralExtractionEngine:
        extract_paths = {}
        for target in ruleset.get("targets", []):
            field_name = target.get("tag")
            path_str = target.get("path", "")
            if field_name and path_str:
                extract_paths[field_name] = path_str.split(".")
                
        log.info(f"[Parser] Compiled StructuralExtractionEngine targeting: {list(extract_paths.keys())}")
        return StructuralExtractionEngine(extract_paths)

class AuditRulesetParser(AbstractRulesetParser[CompiledEngine[bytes, bytes]]):
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> CompiledEngine[bytes, bytes]:
        regex_engine = FastRegexRedactionEngine()
        inspection_level = ruleset.get("global_config", {}).get("inspection_level", "structural")
        if inspection_level == "structural":
            log.info("[Parser] Compiling StructuralRedactionEngine (Tree Traversal Active).")
            return StructuralRedactionEngine(fallback_engine=regex_engine)
        
        log.info("[Parser] Compiling FastRegexRedactionEngine (Flat Regex Scan Active).")
        return regex_engine


# =====================================================================
# [Legacy] 추후 StreamTaggingParser로 마이그레이션 완료 시 제거 가능
# =====================================================================
class FastLifecycleEngine(CompiledEngine[str, List[str]]):
    """@desc: C 레벨 정규식으로 컴파일된 패턴을 활용해 O(1)에 가까운 속도로 스트림을 분류합니다."""
    def __init__(self, compiled_rules: List[Tuple[re.Pattern, str]]):
        self.rules = compiled_rules

    def execute(self, payload: str) -> List[str]:
        return [tag for pattern, tag in self.rules if pattern.search(payload)]

class LifecycleRegexParser(AbstractRulesetParser[FastLifecycleEngine]):
    """@desc: JSON 룰셋의 AND/OR 조건을 정규식 패턴으로 변환 후 컴파일합니다."""
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> FastLifecycleEngine:
        compiled_rules = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            keywords = target.get("keywords", [])
            if not tag or not keywords: continue

            regex_parts = []
            for group in keywords:
                if "AND" in group:
                    lookaheads = "".join(f"(?=.*{re.escape(word)})" for word in group["AND"])
                    regex_parts.append(f"^{lookaheads}.*")
                elif "OR" in group:
                    ors = "|".join(re.escape(word) for word in group["OR"])
                    regex_parts.append(f"(?:{ors})")

            if regex_parts:
                final_regex = "|".join(regex_parts)
                compiled_pattern = re.compile(final_regex, re.IGNORECASE)
                compiled_rules.append((compiled_pattern, tag))

        return FastLifecycleEngine(compiled_rules)

class LocalStreamEngine(CompiledEngine[str, List[str]]):
    def __init__(self, evaluators: List[Tuple[Callable[[str], bool], str]]):
        self.evaluators = evaluators

    def execute(self, payload: str) -> List[str]:
        return [tag for eval_fn, tag in self.evaluators if eval_fn(payload)]

class LocalStreamRulesetParser(AbstractRulesetParser[LocalStreamEngine]):
    """@desc: JSON 룰셋을 로컬 스트림 평가 엔진(LocalStreamEngine)으로 컴파일합니다."""
    def _keywords_to_evaluator(self, keywords: List[Dict[str, List[str]]]) -> Optional[Callable[[str], bool]]:
        if not keywords: return None
        
        def evaluator(line: str) -> bool:
            line_lower = line.lower()
            for group in keywords:
                if "AND" in group and group["AND"]:
                    if not all(val.lower() in line_lower for val in group["AND"]):
                        return False
                elif "OR" in group and group["OR"]:
                    if not any(val.lower() in line_lower for val in group["OR"]):
                        return False
            return True
            
        return evaluator

    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> LocalStreamEngine:
        compiled_evaluators = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            if target_tags and tag not in target_tags: continue
            
            kw_evaluator = self._keywords_to_evaluator(target.get("keywords", []))
            if kw_evaluator:
                compiled_evaluators.append((kw_evaluator, tag))
                
        return LocalStreamEngine(compiled_evaluators)


# =====================================================================
# [New] 결함(버그) 해결 및 인터페이스가 정렬된 통합 파서
# =====================================================================
class StreamTaggingParser(AbstractRulesetParser[CompiledEngine[str, List[str]]]):
    """@desc: JSON 룰셋을 파싱하여 정규식(C-Level) 또는 로컬(Python) 스트림 태깅 엔진으로 컴파일합니다.
              (Outer OR 논리 정합성 및 클로저 스코프 이슈 픽스 버전)"""
    
    def __init__(self, engine_type: str = "local", target_tags: Optional[List[str]] = None):
        self.engine_type = engine_type
        self.target_tags = target_tags

    def parse_ruleset(self, ruleset: Dict[str, Any]) -> CompiledEngine[str, List[str]]:
        config_engine = ruleset.get("global_config", {}).get("engine", self.engine_type)
        
        if config_engine == "regex":
            return self._compile_regex_engine(ruleset)
        else:
            return self._compile_local_engine(ruleset)

    def _compile_local_engine(self, ruleset: Dict[str, Any]) -> LocalStreamEngine:
        compiled_evaluators = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            keywords = target.get("keywords", [])
            
            if not tag or not keywords: 
                continue
            if self.target_tags and tag not in self.target_tags: 
                continue

            # 파이썬 지연 바인딩 버그 방지를 위해 kw_groups로 현재 루프의 keywords 캡처
            def evaluator(line: str, kw_groups=keywords) -> bool:
                line_lower = line.lower()
                for group in kw_groups:
                    group_match = True
                    
                    if "AND" in group and group["AND"]:
                        if not all(val.lower() in line_lower for val in group["AND"]):
                            group_match = False
                            
                    if "OR" in group and group["OR"]:
                        if not any(val.lower() in line_lower for val in group["OR"]):
                            group_match = False
                    
                    # 그룹 내 조건(AND, OR)을 하나라도 완벽히 통과하면 즉시 매칭 성공 (Outer OR)
                    if group_match:
                        return True
                        
                return False

            compiled_evaluators.append((evaluator, tag))
            
        # 기존 평가 엔진 재사용
        return LocalStreamEngine(compiled_evaluators)

    def _compile_regex_engine(self, ruleset: Dict[str, Any]) -> FastLifecycleEngine:
        compiled_rules = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            keywords = target.get("keywords", [])
            
            if not tag or not keywords: 
                continue
            if self.target_tags and tag not in self.target_tags: 
                continue

            regex_parts = []
            for group in keywords:
                lookaheads = ""
                # AND와 OR 조건이 함께 있을 경우 전방 탐색을 중첩하여 논리 교집합 보장
                if "AND" in group and group["AND"]:
                    lookaheads += "".join(f"(?=.*{re.escape(word)})" for word in group["AND"])
                if "OR" in group and group["OR"]:
                    ors = "|".join(re.escape(word) for word in group["OR"])
                    lookaheads += f"(?=.*(?:{ors}))"
                
                if lookaheads:
                    regex_parts.append(f"^{lookaheads}.*")

            if regex_parts:
                final_regex = "|".join(regex_parts)
                compiled_pattern = re.compile(final_regex, re.IGNORECASE)
                compiled_rules.append((compiled_pattern, tag))

        return FastLifecycleEngine(compiled_rules)