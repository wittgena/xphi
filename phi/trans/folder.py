# phi.trans.folder
"""@flow: AUG(Φ_declared) → reflect → Ψ → materialize → Φ_materialized → entry(anchor)"""
import asyncio
import json
import inspect
import ast
from typing import Any, Dict, List, Tuple
from bound.emitter import get_logger
from arch.proto.flow import ProtoFlow, FlowState, Transduction
from contract.registry import contract, discover_modules, registry
from node.runtime import NodeRuntime
from phi.runtime import PhiRuntime
from bound.resolver import find_current_self, resolve_path

log = get_logger("trans.folder")

PAYLOAD = {
  "source_path": inspect.getsourcefile(lambda: None), # 자기 자신을 가리키는 포인터
  "task": {
      "task_id": "init-trigger",
      "requirement": "..."
  }
}

@contract.ator("trans.folder")
class TransFolder(Transduction):
    def __init__(self):
      self.manifold = None

    def bind(self, manifold: Any):
        self.manifold = manifold

    def transduce(self, flow: ProtoFlow, ator_node: Any) -> ProtoFlow:
        topology = flow.payload
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
                operator_instance = registry.create_component(
                    "ator", {"type": target_operator}
                )
                node_instance.bound_operator = operator_instance
            runtime_nodes[node_id] = node_instance
        return self._close(runtime_nodes, flow, ator_node)
