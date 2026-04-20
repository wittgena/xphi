# phi.transcript
import asyncio
import json
import inspect
import ast
import re
import yaml
from abc import abstractmethod
from typing import Any, Dict, List, Tuple
from bound.emitter import get_logger
from arch.proto.flow import ProtoFlow, FlowState, Transduction
from contract.registry import contract, registry
from contract.block.parser.md import MdAstParser
from contract.block.extractor import BlockExtractor

log = get_logger("phi.transcript")

class BaseTranscript(Transduction):
    """@flow: Ψ → Φ transformer (transcription + translation boundary)"""
    def __init__(self, base_node: Any):
        self.base_node = base_node
        self.manifold = base_node.local_manifold
        self.role = "base_transcript"
        self.node_context = {
            "instruction": "System Materialization Kernel",
            "role": self.role
        }

    def transduce(self, flow: ProtoFlow, ator_node: Any) -> ProtoFlow:
        """@phase: Projection (Ψ_reflect)"""
        file_path = flow.payload
        log.info(f"  [Projection] Reflecting source: {file_path}")
        projected_topology = self._reflect_source(file_path)
        return self._close(projected_topology, flow, ator_node)

    def _execute_transformation(self, topology: Dict[str, Any], instruction: str) -> Dict[str, Any]:
        """Translation (Ψ → Φ_materialized)"""
        log.info("    [Kernel] Materializing Topology into Node Instances")
        runtime_nodes = {}
        for node_id, config in topology.items():
            node_type = config["type"]
            spec = config["spec"]
            
            if node_type not in self.manifold:
                raise ValueError(f"Unknown node type '{node_type}'")
            
            NodeClass = self.manifold[node_type].node_class
            node_instance = NodeClass(spec)
            target_operator = spec.get("operator")
            if target_operator:
                operator_instance = registry.create_component("ator", {"type": target_operator})
                node_instance.bound_operator = operator_instance

            runtime_nodes[node_id] = node_instance
        return runtime_nodes
    
    @abstractmethod
    def _reflect_source(self, file_path: str) -> Dict[str, Any]:
        """소스를 해석하여 Dict(Topology)를 반환하는 메서드 (Subclass must implement)"""
        pass

@contract.ator("phi.transcript")
class PhiTranscript(BaseTranscript):
    """@flow: Ψ → Φ transformer (transcription + translation boundary)"""
    def __init__(self, base_node: Any):
        super().__init__(base_node)
        self.role = "transcript"

    def _reflect_source(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'XPHI':
                        return ast.literal_eval(node.value)
        raise ValueError(f"XPHI not found in {file_path}")



@contract.ator("phi.transcript.md")
class MdPhiTranscript(BaseTranscript):
    """@flow: Ψ(Markdown Document) → Φ(Extracted Topology) → Φ_materialized transformer"""
    
    def __init__(self, base_node: Any):
        super().__init__(base_node)
        self.role = "md_transcript"
        self.parser_cls = MdAstParser
        self.extractor = BlockExtractor()

    def _register_dynamic_ator(self, name: str, code: str, lang: str):
        """MD 내에서 @define.ator로 선언된 코드를 레지스트리에 동적 등록 (Placeholder)"""
        log.info(f"    [Define] Registering dynamic ator: {name} (lang: {lang})")
        ## TODO: 실제 구현 시 `exec()`를 통해 코드를 로컬 스코프에 올리고 registry에 등록합니다.
        ## registry.register_dynamic_component("ator", name, compiled_code)
        pass

    def _reflect_source(self, source: str, is_file: bool = True) -> Dict[str, Any]:
        if is_file:
            with open(source, "r", encoding="utf-8") as f:
                md_content = f.read()
        else:
            md_content = source

        doc = self.parser_cls(md_content).parse()
        blocks = self.extractor.extract(doc)

        doc = self.parser_cls(file_path).parse()
        blocks = self.extractor.extract(doc)
        
        defined_regimes = {}
        xphi_topology = {}
        
        ## [Pass 1] Hoisting: 정의(Definition) 수집 및 레지스트리 적재
        current_def_regime = None
        current_def_ator = None
        for b in blocks:
            if b.get("block_type") == "heading":
                content = b.get("content", "")
                
                ## Ator 정의 수집
                if "@define.ator" in content:
                    match = re.search(r'@define\.ator\("([^"]+)"\)', content)
                    if match:
                        current_def_ator = match.group(1)
                        current_def_regime = None
                
                ## Regime 정의 수집
                elif "@define.regime" in content:
                    match = re.search(r'@define\.regime\("([^"]+)"\)', content)
                    if match:
                        current_def_regime = match.group(1)
                        defined_regimes[current_def_regime] = []
                        current_def_ator = None
                
                ## Regime 내부의 하위 노드들을 매크로로 저장
                elif current_def_regime and ("@project." in content or "@contract." in content):
                    defined_regimes[current_def_regime].append(b)

            ## 코드 블록 처리 (Ator 구현체 추출)
            elif b.get("block_type") in ("python", "bash", "sh", "yaml", "json"):
                if current_def_ator:
                    self._register_dynamic_ator(current_def_ator, b.get("content", ""), b.get("block_type"))
                    current_def_ator = None  # 수집 완료 후 초기화
                elif current_def_regime:
                    defined_regimes[current_def_regime].append(b)


        ## [Pass 2] Linking: 위상 전개 및 투영 트리 구성
        current_proj_node = None
        current_sub_block = None
        
        for b in blocks:
            if b.get("block_type") == "heading":
                content = b.get("content", "")
                
                ## Project Regime / Ator 발견 (이전 호환성을 위해 contract도 지원)
                proj_match = re.search(r'@(project|contract)\.(regime|ator)\("([^"]+)"\)', content)
                if proj_match:
                    _, node_kind, node_id = proj_match.groups()
                    current_proj_node = node_id
                    
                    ## 노드 기본 구조(XPHI 껍데기) 생성
                    xphi_topology[current_proj_node] = {
                        "type": "regime" if node_kind == "regime" else "ator",
                        "spec": {
                            "operator": node_id if node_kind == "ator" else None,
                            "flow": {}
                        }
                    }
                    
                    ## Regime일 경우 Pass 1에서 수집한 정의(매크로 블록) 병합
                    if node_kind == "regime" and node_id in defined_regimes:
                        xphi_topology[current_proj_node]["spec"]["macro_blocks"] = defined_regimes[node_id]
                        
                    current_sub_block = node_kind
                
                ## 흐름 제어 블록 발견
                elif "@phase.flow" in content or "@flow" in content:
                    current_sub_block = "flow"

            ## YAML / JSON 파라미터 컨텍스트 병합
            elif b.get("block_type") in ("yaml", "json") and current_proj_node:
                try:
                    parsed_content = yaml.safe_load(b.get("content", "")) or {}
                    if current_sub_block == "flow":
                        xphi_topology[current_proj_node]["spec"]["flow"].update(parsed_content)
                    elif current_sub_block in ("ator", "regime"):
                        ## ator의 `context` 등 기타 spec 속성 업데이트
                        xphi_topology[current_proj_node]["spec"].update(parsed_content)
                except yaml.YAMLError as e:
                    log.error(f"[Φ:error] 파라미터 파싱 실패 in {current_proj_node}: {e}")

        ## Implicit Sequence Binding
        node_keys = list(xphi_topology.keys())
        for i, node_id in enumerate(node_keys):
            spec = xphi_topology[node_id]["spec"]
            flow_rules = spec.get("flow", {})
            
            ## 명시적으로 next가 지정되지 않았다면 마크다운 상의 물리적 다음 노드를 지정
            if "next" not in flow_rules:
                if i < len(node_keys) - 1:
                    spec["next"] = node_keys[i+1]
                    spec["flow"]["next"] = node_keys[i+1]
                else:
                    spec["next"] = "UGA"  # 종결 코돈
                    spec["flow"]["next"] = "UGA"
            else:
                spec["next"] = flow_rules["next"]

        if not xphi_topology:
            raise ValueError(f"No valid XPHI topology found in {file_path}")
        return xphi_topology