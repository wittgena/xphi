# xphi.arch.xor.parser.ruleset.engine
import re
import orjson
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generic, TypeVar, Tuple, Callable
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ruleset.engine")

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
EngineT = TypeVar("EngineT")

class CompiledEngine(ABC, Generic[InputT, OutputT]):
    @abstractmethod
    def execute(self, payload: InputT) -> OutputT:
        pass

class AbstractRulesetParser(ABC, Generic[EngineT]):
    @abstractmethod
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> EngineT:
        pass

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

class FastLifecycleEngine(CompiledEngine[str, List[str]]):
    """@desc: C 레벨 정규식으로 컴파일된 패턴을 활용해 스트림 라인을 태깅합니다 (StreamTaggingParser regex 모드용)."""
    def __init__(self, compiled_rules: List[Tuple[re.Pattern, str]]):
        self.rules = compiled_rules

    def execute(self, payload: str) -> List[str]:
        return [tag for pattern, tag in self.rules if pattern.search(payload)]

class LocalStreamEngine(CompiledEngine[str, List[str]]):
    def __init__(self, evaluators: List[Tuple[Callable[[str], bool], str]]):
        self.evaluators = evaluators

    def execute(self, payload: str) -> List[str]:
        return [tag for eval_fn, tag in self.evaluators if eval_fn(payload)]

class StreamTaggingParser(AbstractRulesetParser[CompiledEngine[str, List[str]]]):
    """@desc: JSON 룰셋을 파싱하여 정규식(C-Level) 또는 로컬(Python) 스트림 태깅 엔진으로 컴파일합니다."""
    def __init__(self, engine_type: str = "local", target_tags: Optional[List[str]] = None):
        self.engine_type = engine_type
        self.target_tags = target_tags

    def parse_ruleset(self, ruleset: Dict[str, Any]) -> CompiledEngine[str, List[str]]:
        config_engine = ruleset.get("global_config", {}).get("engine", self.engine_type)
        if config_engine == "regex":
            return self._compile_regex_engine(ruleset)
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
                    if group_match:
                        return True
                return False

            compiled_evaluators.append((evaluator, tag))
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