# phase.wasm.executor
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, AsyncGenerator, Optional, Protocol

from arch.xor.parser.block.contract import Contract, CoherenceState
from arch.contract.event.next import next_id, generate_parity_triplet, parse_phase_id
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter
from watcher.dphi.cgroup import Tier

log = get_emitter("wasm.executor")

class EffectResolver(Protocol):
    async def resolve(self, payload: Dict[str, Any], instruction: str) -> Dict[str, Any]:
        ...

@dataclass
class TaskContext:
    payload: Dict[str, Any]
    task_type: str = "default"
    tier: str = Tier.STANDARD.value
    topos_id: str = field(default_factory=next_id) 
    phase_id: Optional[int] = None
    nexus_id: Optional[int] = None

    def __post_init__(self):
        if self.phase_id is None or self.nexus_id is None:
            triplet = generate_parity_triplet(topo=0, press=0)
            self.topos_id = triplet["topos_id"]
            self.phase_id = triplet["phase_id"]
            self.nexus_id = triplet["nexus_id"]

SOURCE_NAME = "wasm_executor"
WASM_KIND_INFO = "INFO"
WASM_KIND_TRANSITION = "TRANSITION"
WASM_MSG_IO_REQUIRED = "IO_REQUIRED"

class WasmExecutor:
    def __init__(self, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = WasmBroker()
        self.resolvers = resolvers or {}

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Contract, None]:
        log.info(f"[{SOURCE_NAME}] Injecting task (ToposID: {context.topos_id})")
        current_payload = context.payload
        
        while True:
            parsed_phase = parse_phase_id(context.phase_id)
            topos_context = {
                "injected_anchor": context.nexus_id, 
                "injected_tick": parsed_phase["tick"],
                "timestamp": int(time.time() * 1000)
            }

            request_data = {
                "action": context.task_type,
                "tier": context.tier,
                "payload": current_payload
            }

            exec_result = await self.broker.invoke(
                target_func=WasmMethod.EXECUTE_TRANSITION, 
                context=topos_context,
                payload=json.dumps(request_data)
            )
            
            if not exec_result.success:
                log.warning(f"[{SOURCE_NAME}] Divergence: {exec_result.error}")
                triplet = generate_parity_triplet(topo=0, press=0, rupture=True)
                yield Contract(
                    id=next_id(),
                    topos_id=context.topos_id,
                    phase_id=triplet["phase_id"],
                    nexus_id=triplet["nexus_id"],
                    kind="divergence",
                    source=SOURCE_NAME,
                    state=CoherenceState.FRAGMENTED,
                    payload={"reason": str(exec_result.error)}
                )
                break
                
            raw_res = json.loads(exec_result.output)
            if "data" in raw_res and "success" in raw_res:
                res = raw_res["data"]
            else:
                res = raw_res
            
            if not res.get("is_authorized", True):
                triplet = generate_parity_triplet(topo=0, press=0, rupture=True)
                yield Contract(
                    id=next_id(),
                    topos_id=context.topos_id,
                    phase_id=triplet["phase_id"],
                    nexus_id=triplet["nexus_id"],
                    kind="anomaly",
                    source=SOURCE_NAME,
                    state=CoherenceState.FRAGMENTED,
                    payload={"detail": res.get("rejection_reason", "unauthorized")}
                )
                break
                
            cycles = res.get("cycles", 0)
            residues = res.get("all_residues", [])
            triplet = generate_parity_triplet(topo=cycles, press=len(residues), rupture=False)
            
            # (이후 상태 전이 방출 및 Resolver Delegate 등은 이전 정렬과 동일)
            yield Contract(
                id=next_id(),
                topos_id=context.topos_id,
                phase_id=triplet["phase_id"],
                nexus_id=triplet["nexus_id"],
                kind="transition",
                source=SOURCE_NAME,
                state=CoherenceState.STREAMING,
                payload={"data": res}
            )
            
            io_request = next(
                (r for r in residues if r.get("kind") == WASM_KIND_INFO and WASM_MSG_IO_REQUIRED in r.get("msg")), 
                None
            )
            
            if io_request:
                req_msg = io_request.get("msg", "")
                target_key = io_request.get("target") 
                
                resolver = self.resolvers.get(target_key)
                
                if resolver:
                    current_payload = await resolver.resolve(current_payload, instruction=req_msg)
                    context.phase_id = triplet["phase_id"]
                    context.nexus_id = triplet["nexus_id"]
                    continue 
                else:
                    triplet = generate_parity_triplet(topo=cycles, press=len(residues), rupture=True)
                    yield Contract(
                        id=next_id(),
                        topos_id=context.topos_id,
                        phase_id=triplet["phase_id"],
                        nexus_id=triplet["nexus_id"],
                        kind="orphan",
                        source=SOURCE_NAME,
                        state=CoherenceState.FRAGMENTED,
                        payload={"target": target_key}
                    )
                    break
                    
            if not any(r.get("kind") == WASM_KIND_TRANSITION for r in residues):
                final_triplet = generate_parity_triplet(topo=cycles, press=0, rupture=False)
                yield Contract(
                    id=next_id(),
                    topos_id=context.topos_id,
                    phase_id=final_triplet["phase_id"],
                    nexus_id=final_triplet["nexus_id"],
                    kind="coherence",
                    source=SOURCE_NAME,
                    state=CoherenceState.COHERENT,
                    payload={
                        "root": res.get("root"), 
                        "cycles": cycles               
                    }
                )
                break