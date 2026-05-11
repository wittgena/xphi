# xphi.model.context.compiler
## @lineage: topos.xor.model.context.compiler
## @lineage: phase.xor.model.context.compiler
import json
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any
from topos.bound.plane.emitter import get_emitter

log = get_emitter("context.compiler")

@dataclass
class Fragment:
    id: str
    label: str  # 'projector', 'opersig', 'boundary' 등의 위상적 역할
    attributes: Dict[str, Any] = field(default_factory=dict)
    relations: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class ContextSig:
    # @topos.role: Φ′ (projected structural field)
    entry_point: str
    nodes: Dict[str, Fragment] = field(default_factory=dict)
    projections: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)  # Basis FootLog.info 보존

    def get_fragment(self, frag_id: str) -> Fragment:
        if frag_id not in self.nodes:
            raise KeyError(f"Fragment '{frag_id}' not found in Phasesignature.")
        return self.nodes[frag_id]

    def export_boundary(self) -> Dict[str, Any]:
        """@role: Serialize for BoundaryRenderer (Φ′ → ∂Φ)"""
        return {
            "entry_point": self.entry_point,
            "meta": self.meta,
            "projections": self.projections,
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()}
        }

class ContextCompiler:
    """@phase: Φ⁺ (Mutated State) → Φ′ (Topology)"""
    def compile(self, signature_state: Dict[str, Any]) -> ContextSig:
        entry_id = signature_state.get("module_id", "unknown_signature")
        log.info(f"Starting compilation for signature topology: {entry_id}")
        
        xe = ContextSig(
            entry_point=entry_id,
            projections=signature_state.get("output_fields", [])
        )

        ## signature Meta (원인 보존)
        xe.meta = {
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
        xe.nodes[entry_id] = main_frag
        rules = signature_state.get("mutated_rules", [])
        for idx, rule in enumerate(rules):
            rule_id = f"{entry_id}_op_{idx}"
            rule_frag = Fragment(
                id=rule_id, 
                label="opersignature", 
                attributes={
                    "logic": rule.get("logic"),
                    "pressure": rule.get("pressure"),
                    "source_basis": rule.get("basis_ref")
                }
            )
            xe.nodes[rule_id] = rule_frag
            main_frag.relations.append({"target": rule_id, "rel": "flows_into" })
            target_id = rule.get("target_module")
            if target_id:
                rule_frag.relations.append({
                    "target": target_id, 
                    "rel": "produces_aspect",
                    "dst": rule.get("aspect", f"aspect_{idx}")
                })
                if target_id not in xe.nodes:
                    xe.nodes[target_id] = Fragment(
                        id=target_id, 
                        label="boundary",
                        attributes={
                            "type": "external_coupling",
                            "direction": "outbound"
                        }
                    )
        self._validate_phase(xe)
        return xe

    def _validate_phase(self, signature: ContextSig):
        if signature.entry_point not in signature.nodes:
            raise ValueError(f"Entry point '{signature.entry_point}' is missing.")
        for frag_id, fragment in signature.nodes.items():
            for edge in fragment.relations:
                target_id = edge.get("target")
                if target_id not in signature.nodes:
                    raise ValueError(f"Fragment '{frag_id}' points to non-existent target '{target_id}'.")


class ContextProjector:
    """@phase: Φ′ → Runtime Spec (Lowering to 'signature' seeds)"""
    def project(self, signature: ContextSig) -> Dict[str, Dict[str, Any]]:
        specs = {}
        for frag_id, frag in signature.nodes.items():
            # Fragment Label을 실제 signature 하부 구조 타입으로 매핑
            if frag.label == "projector":
                signature_type = "signature.projector"
            elif frag.label == "opersignature":
                signature_type = "signature.opersig"
            elif frag.label == "boundary":
                signature_type = "signature.act" # 경계는 실행 시점에 구동기(Act)가 됨
            else:
                signature_type = f"signature.{frag.label}"

            spec = {"type": signature_type}
            spec.update(frag.attributes)
            conditional_edges = [e for e in frag.relations if e.get("rel") == "produces_aspect"]
            unconditional_edges = [e for e in frag.relations if e.get("rel") != "produces_aspect"]
            if conditional_edges:
                # Opersignature 내부의 논리적 스위치
                switch_id = f"{frag_id}_switch"
                spec["next"] = switch_id
                rules = []
                for edge in conditional_edges:
                    rules.append({"if": {"aspect": edge.get("dst")}, "next": edge["target"]})
                specs[switch_id] = {"type": "signature.router", "rules": rules}
            elif unconditional_edges:
                targets = [e["target"] for e in unconditional_edges]
                spec["next"] = targets[0] if len(targets) == 1 else targets
            else:
                spec["next"] = "END"

            specs[frag_id] = spec

        return specs


if __name__ == "__main__":
    ## Dock에서 뱉어낸 가상의 상전이 상태 (agent가 아닌 signature 모듈)
    mock_mutated_state = {
        "module_id": "signature_omega",
        "base_instructions": "Project incoming streams to structural vectors.",
        "version": 2,
        "input_fields": ["payload", "tensor"],
        "output_fields": ["res", "state_vector"],
        "basis_snapshot": "basis::signature_omega::1715000000",
        "tension": 0.85,
        "mutated_rules": [
            {
                "logic": "Filter divergent noise from tensor.",
                "aspect": "noise_flag",
                "pressure": 0.92,
                "basis_ref": "basis::signature_omega::1714999000",
                "target_module": "signature_nullifier"
            },
            {
                "logic": "Compress if payload entropy is high.",
                "aspect": "high_entropy",
                "pressure": 0.81,
                "basis_ref": "basis::signature_omega::1714998000",
                "target_module": "signature_compressor"
            }
        ]
    }

    log.info("\n## signature compiler: Weaving signature Topology...")
    compiler = ContextCompiler()

    try:
        phase_signature = compiler.compile(mock_mutated_state)
        log.info("## Boundary Export (For BoundaryRenderer Φs)")
        boundary_dict = phase_signature.export_boundary()
        log.info(json.dumps(boundary_dict, indent=2, ensure_ascii=False))

        log.info("## Runtime Specifications (Lowering to 'signature' xe)")
        projector = ContextProjector()
        runtime_specs = projector.project(phase_signature)
        log.info(json.dumps(runtime_specs, indent=2, ensure_ascii=False))
    except Exception as e:
        log.error(f"Compilation failed: {e}")