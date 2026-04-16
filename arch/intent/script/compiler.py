# arch.intent.script.compiler
import json
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any
from bound.emitter import get_emitter

log = get_emitter("action.compiler")

@dataclass
class Fragment:
    # @topos.role: minimal unit
    id: str
    label: str  # 'projector', 'operator', 'boundary' 등의 위상적 역할
    attributes: Dict[str, Any] = field(default_factory=dict)
    relations: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class SignatureScript:
    # @topos.role: Φ′ (projected structural field)
    entry_point: str
    nodes: Dict[str, Fragment] = field(default_factory=dict)
    projections: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)  # Basis Footprint 보존

    def get_fragment(self, frag_id: str) -> Fragment:
        if frag_id not in self.nodes:
            raise KeyError(f"Fragment '{frag_id}' not found in PhaseScript.")
        return self.nodes[frag_id]

    def export_boundary(self) -> Dict[str, Any]:
        """@role: Serialize for BoundaryRenderer (Φ′ → ∂Φ)"""
        return {
            "entry_point": self.entry_point,
            "meta": self.meta,
            "projections": self.projections,
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()}
        }

class ScriptCompiler:
    """@phase: Φ⁺ (Mutated State) → Φ′ (Topology)"""
    def compile(self, signature_state: Dict[str, Any]) -> SignatureScript:
        entry_id = signature_state.get("module_id", "unknown_ator")
        log.info(f"Starting compilation for ator topology: {entry_id}")
        
        script = SignatureScript(
            entry_point=entry_id,
            projections=signature_state.get("output_fields", [])
        )

        ## Script Meta (원인 보존)
        script.meta = {
            "basis_ref": signature_state.get("basis_snapshot", "genesis"),
            "tension_at_crystallization": signature_state.get("tension", 0.0)
        }

        ## Main Entry Fragment (Projector)
        main_attrs = {
            "instructions": signature_state.get("base_instructions", ""),
            "version": signature_state.get("version", 1),
            "inputs": signature_state.get("input_fields", [])
        }
        main_frag = Fragment(id=entry_id, label="projector", attributes=main_attrs)
        script.nodes[entry_id] = main_frag
        rules = signature_state.get("mutated_rules", [])
        for idx, rule in enumerate(rules):
            rule_id = f"{entry_id}_op_{idx}"
            rule_frag = Fragment(
                id=rule_id, 
                label="operator", 
                attributes={
                    "logic": rule.get("logic"),
                    "pressure": rule.get("pressure"),
                    "source_basis": rule.get("basis_ref")
                }
            )
            script.nodes[rule_id] = rule_frag
            main_frag.relations.append({"target": rule_id, "rel": "flows_into" })
            target_id = rule.get("target_module")
            if target_id:
                rule_frag.relations.append({
                    "target": target_id, 
                    "rel": "produces_aspect",
                    "dst": rule.get("aspect", f"aspect_{idx}")
                })
                if target_id not in script.nodes:
                    script.nodes[target_id] = Fragment(
                        id=target_id, 
                        label="boundary",
                        attributes={
                            "type": "external_coupling",
                            "direction": "outbound"
                        }
                    )
        self._validate_phase(script)
        return script

    def _validate_phase(self, script: SignatureScript):
        if script.entry_point not in script.nodes:
            raise ValueError(f"Entry point '{script.entry_point}' is missing.")
        for frag_id, fragment in script.nodes.items():
            for edge in fragment.relations:
                target_id = edge.get("target")
                if target_id not in script.nodes:
                    raise ValueError(f"Fragment '{frag_id}' points to non-existent target '{target_id}'.")


class ScriptProjector:
    """@phase: Φ′ → Runtime Spec (Lowering to 'ator' seeds)"""
    def project(self, script: SignatureScript) -> Dict[str, Dict[str, Any]]:
        specs = {}
        for frag_id, frag in script.nodes.items():
            # Fragment Label을 실제 ator 하부 구조 타입으로 매핑
            if frag.label == "projector":
                ator_type = "ator.projector"
            elif frag.label == "operator":
                ator_type = "ator.operator"
            elif frag.label == "boundary":
                ator_type = "ator.actuator" # 경계는 실행 시점에 구동기(Actuator)가 됨
            else:
                ator_type = f"ator.{frag.label}"

            spec = {"type": ator_type}
            spec.update(frag.attributes)
            conditional_edges = [e for e in frag.relations if e.get("rel") == "produces_aspect"]
            unconditional_edges = [e for e in frag.relations if e.get("rel") != "produces_aspect"]
            if conditional_edges:
                # Operator 내부의 논리적 스위치
                switch_id = f"{frag_id}_switch"
                spec["next"] = switch_id
                rules = []
                for edge in conditional_edges:
                    rules.append({"if": {"aspect": edge.get("dst")}, "next": edge["target"]})
                specs[switch_id] = {"type": "ator.router", "rules": rules}
            elif unconditional_edges:
                targets = [e["target"] for e in unconditional_edges]
                spec["next"] = targets[0] if len(targets) == 1 else targets
            else:
                spec["next"] = "END"

            specs[frag_id] = spec

        return specs


if __name__ == "__main__":
    ## Dock에서 뱉어낸 가상의 상전이 상태 (agent가 아닌 ator 모듈)
    mock_mutated_state = {
        "module_id": "ator_omega",
        "base_instructions": "Project incoming streams to structural vectors.",
        "version": 2,
        "input_fields": ["payload", "tensor"],
        "output_fields": ["res", "state_vector"],
        "basis_snapshot": "basis::ator_omega::1715000000",
        "tension": 0.85,
        "mutated_rules": [
            {
                "logic": "Filter divergent noise from tensor.",
                "aspect": "noise_flag",
                "pressure": 0.92,
                "basis_ref": "basis::ator_omega::1714999000",
                "target_module": "ator_nullifier"
            },
            {
                "logic": "Compress if payload entropy is high.",
                "aspect": "high_entropy",
                "pressure": 0.81,
                "basis_ref": "basis::ator_omega::1714998000",
                "target_module": "ator_compressor"
            }
        ]
    }

    print("\n## ScriptCompiler: Weaving Ator Topology...")
    compiler = ScriptCompiler()

    try:
        phase_script = compiler.compile(mock_mutated_state)
        print("## Boundary Export (For BoundaryRenderer Φs)")
        boundary_dict = phase_script.export_boundary()
        print(json.dumps(boundary_dict, indent=2, ensure_ascii=False))

        print("## Runtime Specifications (Lowering to 'ator' xe)")
        projector = ScriptProjector()
        runtime_specs = projector.project(phase_script)
        print(json.dumps(runtime_specs, indent=2, ensure_ascii=False))
    except Exception as e:
        log.error(f"Compilation failed: {e}")