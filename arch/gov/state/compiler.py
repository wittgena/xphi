# ops.tester.state.compiler
import json
from typing import List, Dict, Any

from arch.gov.state.vocab import NodeType, EdgeMode, SigType 
from arch.gov.state.schema import Fragment, FragmentSig, AgentAttributes, EdgeRelation

from watcher.plane.emitter import get_emitter

log = get_emitter("state.compiler")

class StateCompiler:
    """@phase: Φ⁺ (Evolved Schema / Mutated State) → Φ′ (Topology)"""
    
    def compile_from_schema(self, schema: Dict[str, Any]) -> FragmentSig:
        meta = schema.get("meta", {})
        nodes_data = schema.get("nodes", [])
        edges_data = schema.get("topos_edges", [])
        entry_id = nodes_data[0]["id"] if nodes_data else "unknown_entry"
        log.info(f"Starting compilation from Evolved Schema. Entry: {entry_id}")

        xe = FragmentSig(entry_point=entry_id, meta=meta)

        for n in nodes_data:
            frag_id = n["id"]
            layer = n.get("layer", "execution")
            
            # 반환값이 NodeType Enum으로 보장됨
            node_type = self._infer_type_from_layer(layer, frag_id)
            pressure_val = min(1.0, n.get("failure_rate", 0.0) * 2.0)
            attributes = AgentAttributes(
                instructions=n.get("instructions"),
                pressure=pressure_val,
                max_failures=n.get("max_failures", 3),
                allow_parallel=n.get("allow_parallel", False),
                extras={
                    "file_path": n.get("file_path"),
                    "is_topos": n.get("is_topos", False),
                    "optimization_epoch": n.get("optimization_epoch", 0),
                }
            )
            xe.nodes[frag_id] = Fragment(id=frag_id, type=node_type, attributes=attributes)

        ## 2. Edges -> EdgeRelation 변환
        for e in edges_data:
            src = e.get("source")
            dst_node = e.get("target")
            condition = e.get("condition") 
            
            if src in xe.nodes:
                if condition == EdgeMode.FALLBACK.value: # "fallback" 문자열 검사
                    edge_type = EdgeMode.FALLBACK
                else:
                    edge_type = EdgeMode.CONDITIONAL if condition else EdgeMode.DIRECT
                
                relation = EdgeRelation(
                    target=dst_node,
                    edge_type=edge_type,
                    condition=condition if edge_type == EdgeMode.CONDITIONAL else None
                )
                xe.nodes[src].relations.append(relation)
                
        self._validate_phase(xe)
        return xe

    def compile(self, signature_state: Dict[str, Any]) -> FragmentSig:
        """기존 Dict(mutated_rules) 기반 하위 호환성 유지 컴파일 (V1 -> V2 어댑터)"""
        entry_id = signature_state.get("module_id", "unknown_signature")
        
        xe = FragmentSig(entry_point=entry_id)
        xe.meta = {
            "basis_ref": signature_state.get("basis_snapshot", "genesis"),
            "tension_at_crystallization": signature_state.get("tension", 0.0),
            "projections": signature_state.get("output_fields", []) 
        }

        main_attrs = AgentAttributes(
            instructions=signature_state.get("base_instructions", ""),
            extras={
                "version": signature_state.get("version", 1),
                "inputs": signature_state.get("input_fields", [])
            }
        )
        main_frag = Fragment(id=entry_id, type=NodeType.PROJECTOR, attributes=main_attrs)
        xe.nodes[entry_id] = main_frag

        for idx, rule in enumerate(signature_state.get("mutated_rules", [])):
            rule_id = f"{entry_id}_op_{idx}"
            rule_attrs = AgentAttributes(
                pressure=rule.get("pressure", 0.0),
                extras={
                    "logic": rule.get("logic"),
                    "source_basis": rule.get("basis_ref")
                }
            )
            rule_frag = Fragment(id=rule_id, type=NodeType.OPERSIG, attributes=rule_attrs)
            xe.nodes[rule_id] = rule_frag
            main_frag.relations.append(EdgeRelation(target=rule_id, edge_type=EdgeMode.DIRECT))
            target_id = rule.get("target_module")
            if target_id:
                aspect_val = rule.get("aspect", f"aspect_{idx}")
                rule_frag.relations.append(
                    EdgeRelation(target=target_id, edge_type=EdgeMode.CONDITIONAL, condition=aspect_val)
                )
                
                if target_id not in xe.nodes:
                    act_attrs = AgentAttributes(extras={"type": "external_coupling", "direction": "outbound"})
                    xe.nodes[target_id] = Fragment(id=target_id, type=NodeType.ACT, attributes=act_attrs)
                    
        self._validate_phase(xe)
        return xe

    def _infer_type_from_layer(self, layer: str, frag_id: str) -> NodeType:
        """Layer 명칭을 Projector가 이해하는 NodeType 규격으로 정규화"""
        mapping = {
            "llm": NodeType.PROJECTOR,
            "tool": NodeType.OPERSIG,
            "evaluation": NodeType.REFLECT,
            "io": NodeType.ACT,
            "wait": NodeType.PENDING
        }
        return mapping.get(layer.lower(), NodeType.OPERSIG)

    def _validate_phase(self, signature: FragmentSig):
        """DAG의 위상학적 결함 검증"""
        if signature.entry_point not in signature.nodes:
            raise ValueError(f"Entry point '{signature.entry_point}' is missing in nodes.")
            
        end_node_id = SigType.END.value 
        for frag_id, fragment in signature.nodes.items():
            for edge in fragment.relations:
                if edge.target not in signature.nodes and edge.target != end_node_id:
                    raise ValueError(f"Fragment '{frag_id}' points to non-existent target '{edge.target}'.")