# phi.script.materializer
"""@flow: AUG(Φ_declared) → reflect → Ψ → materialize → Φ_materialized → entry(anchor)"""
import asyncio
import json
import inspect
import ast
from typing import Any, Dict, List, Tuple
from plane.emitter import get_logger
from flow.ator import AtorFlow, FlowState, Transduction
from contract.registry import ator_contract, discover_modules, registry
from plane.node.runtime import NodeRuntime
from bridge.ator.runtime import AtorRuntime
from anchor.resolver import find_current_self, resolve_path

log = get_logger("transcript.materializer")

PAYLOAD = {
  "source_path": inspect.getsourcefile(lambda: None), # 자기 자신을 가리키는 포인터
  "task": {
      "task_id": "init-trigger",
      "requirement": "..."
  }
}


## @phase: Φ_declared
PHI = {
  "reflect": {
    "type": "ator",
    "spec": {
      "role": "source_reflector",
      "next": "validate",
      "operator": "source_reflect_ator"
    }
  },
  "validate": {
    "type": "resonance",
    "spec": {
      "role": "topology_validator",
      "next": "route_after_validation",
      "operator": "aug_validator"
    }
  },
  "route_after_validation": {
    "type": "judgment",
    "spec": {
      "rules": {
        "stable": "materialize",
        "retry": "reflect"
      },
      "operator": "contract.judgmentr"
    }
  },
  "materialize": {
    "type": "ator",
    "spec": {
      "role": "node_materializer",
      "next": "align",
      "operator": "transcript.materializer"
    }
  },
  "align": {
    "type": "aligner",
    "spec": {
      "target": "runtime_field",
      "next": "UGA",
      "operator": "runtime.aligner"
    }
  }
}

@ator_contract("transcript.materializer")
class TranscriptMaterializer(Transduction):
    def __init__(self):
      self.manifold = None

    def bind(self, manifold: Any):
        self.manifold = manifold

    def transduce(self, flow: AtorFlow, ator_node: Any) -> AtorFlow:
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
