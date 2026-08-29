# xphi.kernel.space.sandbox.executor
## @lineage: kernel.space.sandbox.executor
## @lineage: kernel.dphi.sandbox.executor
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, AsyncGenerator, Optional, Protocol
from enum import Enum

from xphi.xor.parser.block.contract import Contract, CoherenceState
from xphi.arch.event.next import next_id, generate_parity_triplet, parse_phase_id
from xphi.kernel.dphi.broker import DphiBroker, DphiMethod
from xphi.kernel.dphi.cgroup import Tier
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.executor")

class SandboxEnv(str, Enum):
    LOCAL = "local"     # 프로세스 내 순수 논리 시뮬레이션 (DAG Dry-run 등)
    DENO = "deno"       # Pyodide 기반 JS/Python 격리 샌드박스
    WASM = "wasm"       # 순수 WASM 바이너리 커널
    DOCKER = "docker"   # 향후 지원할 Heavy-duty 컨테이너 격리

class EffectResolver(Protocol):
    async def resolve(self, payload: Dict[str, Any], instruction: str, env: SandboxEnv, tier: str) -> Dict[str, Any]:
        ...

@dataclass
class TaskContext:
    payload: Dict[str, Any]
    task_type: str = "default"
    tier: str = Tier.STANDARD.value
    sandbox_env: SandboxEnv = SandboxEnv.DENO
    topos_id: str = field(default_factory=next_id) 
    phase_id: Optional[int] = None
    nexus_id: Optional[int] = None

    def __post_init__(self):
        if self.phase_id is None or self.nexus_id is None:
            triplet = generate_parity_triplet(topo=0, press=0)
            self.topos_id = triplet["topos_id"]
            self.phase_id = triplet["phase_id"]
            self.nexus_id = triplet["nexus_id"]

SOURCE_NAME = "sandbox_executor"
WASM_KIND_INFO = "INFO"
WASM_KIND_TRANSITION = "TRANSITION"
WASM_MSG_IO_REQUIRED = "IO_REQUIRED"

class SandboxExecutor:
    def __init__(self, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = DphiBroker()
        self.resolvers = resolvers or {}

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Contract, None]:
        log.info(f"[{SOURCE_NAME}] Injecting task (ToposID: {context.topos_id} | Env: {context.sandbox_env.value})")
        current_payload = context.payload
        
        while True:
            parsed_phase = parse_phase_id(context.phase_id)
            topos_context = {
                "injected_anchor": context.nexus_id, 
                "injected_tick": parsed_phase["tick"],
                "timestamp": int(time.time() * 1000)
            }

            phase_root_node = StateAdapter.build_core_node(
                name="sandbox_topos_context",
                content=json.dumps(topos_context),
                children={}
            )

            evo_ctx = StateAdapter.build_evolution_context(phase_root=phase_root_node)
            intent_payload = {
                "tier": context.tier,
                "env": context.sandbox_env.value,
                "data": current_payload
            }

            transition_payload = StateAdapter.build_transition_payload(
                intent_action=context.task_type,
                intent_payload=intent_payload,
                evolution_ctx=evo_ctx
            )

            exec_result = await self.broker.invoke(
                target_func=DphiMethod.EXECUTE_TRANSITION, 
                payload=StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8'),
                tier=context.tier  # 브로커 레벨 Cgroup 주입
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
            res = raw_res.get("data", raw_res) if isinstance(raw_res, dict) else raw_res
            
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
                    ## 런타임 환경(env)과 권한(tier)을 어댑터에 전달하여 올바른 샌드박스로 라우팅 유도
                    current_payload = await resolver.resolve(
                        current_payload, 
                        instruction=req_msg, 
                        env=context.sandbox_env,
                        tier=context.tier
                    )
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