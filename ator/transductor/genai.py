# ator.transductor.genai
import json
import asyncio
import inspect
from typing import Any
from anchor.log import get_logger
from flow.ator import AtorFlow, FlowState, Transduction, Resonance, Judgment, Align
from manifold.contract.registry import ator_contract, discover_modules
from anchor.resolver import find_current_self
from topos.node.runtime import NodeRuntime
from surface.ator.runtime import AtorRuntime
from phi.bootstrap import bootstrap 
from bridge.client.llama import LLMClient
from anchor.resolver import resolve_path

log = get_logger("genai.transductor")

@ator_contract("genai.transductor")
class GenaiTransductor(Transduction):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.llm_client = LLMClient()

    def _project(self, flow: AtorFlow, ator_node: Any) -> dict:
        log.info(f"  [Projection] Opening state for LLM inference: {ator_node.role}")
        payload = flow.payload
        
        context = ator_node.spec.get("context", {})
        system_prompt = context.get("instruction", "You are a helpful expert.")
        if "metrics" in context:
            system_prompt += f"\nTarget metrics: {context['metrics']}"
            
        input_data = payload.get('raw_input') or payload
        user_prompt = f"Input Context:\n{json.dumps(input_data, indent=2, ensure_ascii=False)}"

        log.info(f"  [LLM] Requesting completion for role: {ator_node.role}")
        
        try:
            response = self.llm_client.chat(
                system_prompt=system_prompt, 
                user_prompt=user_prompt,
                timeout=60
            )
            log.info(f"  [LLM] Inference successful for {ator_node.role}")
        except Exception as e:
            log.error(f"  [LLM] Inference failed: {e}", exc_info=True)
            response = f"ERROR_DURING_INFERENCE: {str(e)}"
        return { **payload, "llm_output": response }

async def main():
    input_path = resolve_path('jobs') / 'transcript' / 'meta_debug.py'
    # current_script_path = inspect.getsourcefile(lambda: None)
    base_node, flow_controller, entry_id = await bootstrap(input_path)

    try:
        initial_payload = {
            "task_id": "REQ-101",
            "requirement": "User profile update API with rate limiting",
            "security_level": "High"
        }
        initial_ctx = FlowState(AtorFlow(payload=initial_payload, aspect="init"), state={})
        log.info(f"Submitting task to entry node [{entry_id}]...")
        
        await base_node.psi_queue.put((entry_id, initial_ctx))
        await base_node.psi_queue.join()
        log.info(">>> Field Stabilized: Execution Complete.")
    except Exception as e:
        log.error(f"Execution Error: {e}", exc_info=True)
    finally:
        base_node.running = False

if __name__ == "__main__":
    asyncio.run(main())