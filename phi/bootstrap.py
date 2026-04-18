# phi.bootstrap
"""@flow: PHI(Φ_declared) → reflect → Ψ → materialize → Φ_materialized → entry(anchor)"""
import asyncio
import json
import inspect
import ast
from pathlib import Path
from typing import Any, Dict, List, Tuple
from bound.emitter import get_logger
from arch.proto.flow import ProtoFlow, FlowState, Transduction
from contract.registry import contract, discover_modules, registry
from phi.transcript import PhiTranscript, MdPhiTranscript
from phi.runtime import PhiRuntime
from phase.node.runtime import NodeRuntime
from bound.resolver import find_current_self, resolve_path, load_bound

log = get_logger("phi.bootstrap")
SELF_ROOT = find_current_self()
REPOS = load_bound(SELF_ROOT).get('around', None)

## @phase: Φ_declared
XPHI = {
  "activator": {
    "type": "ator",
    "spec": {
      "role": "planner",
      "next": "topos_validator",
      "context": {
        "instruction": "Create a 3-step project roadmap",
        "temperature": 0.2,
        "inject_state": ["project_id"]
      },
      "operator": "genai.transductor"
    }
  },
  "topos_validator": {
    "type": "resonance",
    "spec": {
      "role": "x.validator",
      "next": "evaluator",
      "operator": "x.validator"
    }
  },
  "evaluator": {
    "type": "ator",
    "spec": {
      "role": "security.auditor",
      "next": "interference",
      "context": {
        "instruction": "Perform OWASP Top 10 scan on the provided code",
        "metrics": ["injection", "broken_auth", "data_exposure"]
      }
    }
  },
  "interference": {
    "type": "resonance",
    "spec": {
      "next": "feedback",
      "operator": "security.resonance"
    }
  },
  "feedback": {
    "type": "judgment",
    "spec": {
      "rules": { "retry": "activator", "stable": "projection" },
      "operator": "contract.judgment"
    }
  },
  "projection": {
    "type": "aligner",
    "spec": {
      "target": "./output/secure_api.py",
      "next": "UGA"
    }
  }
}

async def bootstrap(
    topology_path: str, 
    redis_url: str = "redis://localhost:6379",
    repos: List[str] = REPOS
) -> Tuple[NodeRuntime, PhiRuntime, str]:
    is_md = topology_path.lower().endswith('.md')
    log_msg = "via MD Transcript" if is_md else "via Transcript"
    log.info(f">>> Launching Complex Phase-Field Task {log_msg}...")

    discover_modules(find_current_self())
    base_node = NodeRuntime(redis_url=redis_url, executor=None)
    bootstrap_flow = ProtoFlow(payload=topology_path, aspect="bootstrap")

    transcript_cls = MdPhiTranscript if is_md else PhiTranscript
    transcript = transcript_cls(base_node)

    final_flow = transcript.transduce(bootstrap_flow, ator_node=transcript)
    runtime_nodes = final_flow.payload

    entry_node = next(iter(final_flow.payload))
    flow_controller = PhiRuntime(entry=entry_node, nodes=runtime_nodes, runtime_node=base_node)
    flow_controller.attach()
    return base_node, flow_controller, entry_node

async def main():
  topology_path = inspect.getsourcefile(lambda: None)
  base_node, flow_controller, entry_node = await bootstrap(topology_path)

  try:
    ## @emit: external task → Ψ injection into field
    initial_payload = {
        "task_id": "REQ-101",
        "requirement": "User profile update API with rate limiting",
        "security_level": "High"
    }
    initial_ctx = FlowState(ProtoFlow(payload=initial_payload, aspect="init"), state={})
    log.info(f"Submitting task {initial_payload['task_id']} to the field...")
    await base_node.psi_queue.put(("activator", initial_ctx))
    await base_node.psi_queue.join()
    log.info(">>> All tasks processed.")
  except Exception as e:
    ## @shutdown: field collapse / loop termination
    log.error(f"Execution Error: {e}", exc_info=True)
  finally:
    base_node.running = False

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass