# phi.transcript
import asyncio
import json
import inspect
import ast
from typing import Any, Dict, List, Tuple
from bound.emitter import get_logger
from arch.proto.flow import ProtoFlow, FlowState, Transduction
from contract.registry import ator_contract, registry

log = get_logger("phi.transcript")

@ator_contract("phi.transcript")
class PhiTranscript(Transduction):
    """@flow: Ψ → Φ transformer (transcription + translation boundary)"""
    def __init__(self, base_node: Any):
        self.base_node = base_node
        self.manifold = base_node.local_manifold
        self.role = "transcript"
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
