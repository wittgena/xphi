# xphi.kernel.space.sandbox.resolver
import os
import json
import time
import asyncio
from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, AsyncGenerator, Optional, Protocol, Union

from xphi.arch.contract.space.state import Contract, CoherenceState
from xphi.arch.bound.event.next import next_id, generate_parity_triplet, parse_phase_id
from xphi.kernel.wasm.broker import DphiBroker, DphiMethod
from xphi.kernel.wasm.cgroup import CgroupPolicy, Tier
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.kernel.space.sandbox.config import fuel_config, tier_config
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.resolver")

SOURCE_NAME = "sandbox_executor"
WASM_KIND_INFO = "INFO"
WASM_KIND_TRANSITION = "TRANSITION"
WASM_MSG_IO_REQUIRED = "IO_REQUIRED"

class SandboxEnv(str, Enum):
    LOCAL = "local"     # 프로세스 내 순수 논리 시뮬레이션 (DAG Dry-run 등)
    DENO = "deno"       # Pyodide 기반 JS/Python 격리 샌드박스
    WASM = "wasm"       # 순수 WASM 바이너리 커널
    DOCKER = "docker"   # 향후 지원할 Heavy-duty 컨테이너 격리

class EffectResolver(Protocol):
    async def resolve(self, payload: Dict[str, Any], instruction: str, env: SandboxEnv, tier: Union[Tier, str]) -> Dict[str, Any]:
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

@dataclass
class SandboxReport:
    is_valid: bool
    output: str = ""
    error: str = ""
    cgroup_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class MetabolicProfile:
    cgroup_policy: CgroupPolicy = field(default_factory=CgroupPolicy.standard)
    max_compute_time: float = 3.0
    max_threads: int = 1
    max_node_capacity: int = 3
    max_simulation_ticks: int = 50

@dataclass
class BenchResult:
    status: str
    fuel_consumed: int
    tier_applied: str
    reason: Optional[str] = None

class SandboxResolver(EffectResolver):
    def __init__(self, broker: DphiBroker, profile: Optional[MetabolicProfile] = None):
        self.profile = profile or MetabolicProfile()
        self.broker = broker

    def _get_policy_from_tier(self, tier: Tier) -> CgroupPolicy:
        if tier == Tier.SYSTEM: 
            return CgroupPolicy.system()
        elif tier == Tier.UNLIMITED: 
            return CgroupPolicy.custom(1024, 10_000_000_000)
        return CgroupPolicy.standard()

    async def resolve(self, payload: Dict[str, Any], instruction: str, env: SandboxEnv, tier: Union[Tier, str]) -> Dict[str, Any]:
        if isinstance(tier, str):
            try:
                target_tier = Tier(tier.upper())
            except ValueError:
                target_tier = Tier.STANDARD
        else:
            target_tier = tier

        log.info(f"[SandboxResolver] Routing instruction '{instruction}' to Env '{env.value}' (Tier: {target_tier.value})")
        self.profile.cgroup_policy = self._get_policy_from_tier(target_tier)

        schema = payload.get("schema", {})
        files = schema.get("files", {})
        entry_file = payload.get("entry", "main.py")
        
        code = payload.get("code") or files.get(entry_file, "")
        variables = payload.get("variables", {})

        report = SandboxReport(is_valid=False)
        if env in (SandboxEnv.DENO, SandboxEnv.WASM, SandboxEnv.LOCAL):
            start_time = time.time()
            
            # 🚨 [LOG INJECTION: 실행할 순수 코드 및 변수 덤프]
            log.error(f"[DEBUG-TRACE: 5] ➔ SandboxResolver.broker.execute 호출. Env: {env.value}")
            log.error(f"[DEBUG-TRACE: 5] ➔ Code Preview: {code[:200]}")
            log.error(f"[DEBUG-TRACE: 5] ➔ Variables: {variables}")

            try:
                exec_res = await self.broker.execute(code=code, variables=variables, tier=target_tier.value)
                elapsed_ms = (time.time() - start_time) * 1000
                
                # 🚨 [LOG INJECTION: 실행 결과 확인]
                log.error(f"[DEBUG-TRACE: 6] 🟢 코드 실행 응답 완료. Success: {exec_res.success}")
                if not exec_res.success:
                    log.error(f"[DEBUG-TRACE: 6] 🔴 코드 실행 실패 사유: {exec_res.error}")
                
                report.is_valid = exec_res.success
                report.output = exec_res.output if exec_res.success else ""
                report.error = str(exec_res.error) if not exec_res.success else ""
                
            except Exception as e:
                elapsed_ms = (time.time() - start_time) * 1000
                # 🚨 [LOG INJECTION: 브로커 크래시 추적]
                log.error(f"[DEBUG-TRACE: 7] 💥 코드 실행 중 파이썬 예외 크래시: {str(e)}", exc_info=True)
                report.is_valid = False
                report.error = f"Broker Execution Crash: {str(e)}"
            
            fuel_consumed = 0
            mem_usage_bytes = 0
            if report.is_valid:
                try:
                    out_data = json.loads(report.output)
                    fuel_consumed = out_data.get("fuel_consumed", 0)
                    mem_usage_bytes = out_data.get("mem_usage_bytes", 0)
                except json.JSONDecodeError:
                    pass

            report.cgroup_metrics = {
                "tier": target_tier.value,
                "env": env.value,
                "elapsed_ms": round(elapsed_ms, 2),
                "fuel_consumed": fuel_consumed,
                "mem_usage_bytes": mem_usage_bytes,
                "mem_limit_bytes": self.profile.cgroup_policy.max_memory_bytes
            }
        elif env == SandboxEnv.DOCKER:
            report.error = "Docker environment is provisioned in the architecture but not yet attached."
        else:
            report.error = f"Unsupported Sandbox Environment: {env}"
            
        result_data = {**payload, "sandbox_report": report.to_dict()}
        if report.is_valid:
            result_data["execution_output"] = report.output
            
        return result_data

class SandboxExecutor:
    def __init__(self, broker: DphiBroker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = broker
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

            canonical_bytes = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
            
            # 🚨 [LOG INJECTION: 직렬화된 데이터 덤프 및 크기 확인]
            log.error(f"[DEBUG-TRACE: 1] ➔ 브로커로 진입 직전. Payload Size: {len(canonical_bytes)} bytes")
            log.error(f"[DEBUG-TRACE: 1] ➔ Canonical Payload Dump: {canonical_bytes[:500]} ... (truncated)")
            
            try:
                # 🚨 [LOG INJECTION: 실제 대기 시간 측정]
                invoke_start_time = time.time()
                
                exec_result = await asyncio.wait_for(
                    self.broker.invoke(
                        target_func=DphiMethod.EXECUTE_TRANSITION, 
                        payload=canonical_bytes,
                        tier=context.tier 
                    ),
                    timeout=11.0 # 10초 타임아웃 후 1초 여유 마진
                )
                
                invoke_elapsed = time.time() - invoke_start_time
                log.error(f"[DEBUG-TRACE: 2] 🟢 브로커 응답 수신 완료! 소요시간: {invoke_elapsed:.3f}초 | success: {exec_result.success}")
                
            except asyncio.TimeoutError:
                # 🚨 [LOG INJECTION: 타임아웃 확정 지점]
                log.error(f"[DEBUG-TRACE: 3] 🛑 브로커 11초 타임아웃 발생! (ToposID: {context.topos_id})")
                yield Contract(
                    id=next_id(), topos_id=context.topos_id, phase_id=context.phase_id, nexus_id=context.nexus_id,
                    kind="divergence", source=SOURCE_NAME, state=CoherenceState.FRAGMENTED,
                    payload={"reason": "Sandbox invoke timed out (Internal Deadlock)"}
                )
                break
            except Exception as e:
                log.error(f"[DEBUG-TRACE: 4] 💥 브로커 호출 중 예외 발생: {str(e)}", exc_info=True)
                yield Contract(
                    id=next_id(), topos_id=context.topos_id, phase_id=context.phase_id, nexus_id=context.nexus_id,
                    kind="divergence", source=SOURCE_NAME, state=CoherenceState.FRAGMENTED,
                    payload={"reason": f"Invoke Crash: {str(e)}"}
                )
                break
            
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

class BenchProfile:
    def __init__(self, broker: Optional[DphiBroker] = None):
        self.broker = broker or DphiBroker()

    def _resolve_profile(self, tier: Tier) -> MetabolicProfile:
        if tier == Tier.SYSTEM:
            policy = CgroupPolicy.system()
            return MetabolicProfile(
                cgroup_policy=policy,
                max_threads=tier_config.system_max_threads, 
                max_compute_time=float(policy.cpu_fuel_quota / tier_config.fuel_to_seconds_ratio), 
                max_node_capacity=tier_config.system_max_node_capacity,
                max_simulation_ticks=tier_config.system_max_simulation_ticks
            )
        else:
            policy = CgroupPolicy.standard()
            return MetabolicProfile(cgroup_policy=policy)

    def _charge_account(self, client_id: str, fuel_consumed: int):
        billed_amount = (fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
        log.info(f"[Billing] Charged ${billed_amount:.4f} for {fuel_consumed:,} fuel units. Agent: {client_id}")

    async def execute(
        self, 
        client_id: str, 
        schema: Dict[str, Any], 
        entry: str, 
        depth: int, 
        tier: Tier = Tier.STANDARD,
        dry_run: bool = False
    ) -> BenchResult:
        profile = self._resolve_profile(tier)
        log.info(f"[{client_id}] Target Execution Tier mapped to: {tier.value}")
        
        # 🚨 [LOG INJECTION: 상위 진입점 상태 확인]
        log.error(f"[DEBUG-TRACE: 8] ➔ BenchProfile 진입. Client: {client_id[:8]}, Dry_run: {dry_run}, Tier: {tier.value}")
        
        sandbox_resolver = SandboxResolver(broker=self.broker, profile=profile)
        executor = SandboxExecutor(broker=self.broker, resolvers={"SANDBOX": sandbox_resolver}) 
        
        flat_payload = {"schema": schema, "entry": entry, "depth": depth}
        context = TaskContext(
            task_type="execute_sandbox_schema",
            tier=tier.value, 
            sandbox_env=SandboxEnv.DENO,
            payload=flat_payload
        )
        
        latest_contract = None
        try:
            async for contract in executor.execute_stream(context):
                latest_contract = contract
                # 🚨 [LOG INJECTION: Contract Yield 확인]
                log.error(f"[DEBUG-TRACE: 9] ➔ Contract 방출됨: Kind={contract.kind}, State={contract.state.name}")
                
        except Exception as e:
            log.error(f"[{context.topos_id}] Host Crash during execution: {e}", exc_info=True)
            return BenchResult(status="HOST_DIVERGENCE", fuel_consumed=0, tier_applied=tier.value, reason=str(e))

        if not latest_contract:
            return BenchResult(status="NO_OUTPUT", fuel_consumed=0, tier_applied=tier.value, reason="Stream yielded no contracts.")

        payload_data = latest_contract.payload.get("data", latest_contract.payload)
        
        sandbox_report = payload_data.get("sandbox_report", {})
        cgroup_fuel = sandbox_report.get("cgroup_metrics", {}).get("fuel_consumed", 0)
        dag_cycles = payload_data.get("cycles", 0)
        
        final_fuel_consumed = cgroup_fuel if cgroup_fuel > 0 else dag_cycles
        final_status = latest_contract.kind.upper()
        reason = payload_data.get("reason") or payload_data.get("detail") or "Execution completed successfully"
        
        if not dry_run:
            self._charge_account(client_id, final_fuel_consumed)
        else:
            log.info(f"[Billing] Dry-run complete. Estimated {final_fuel_consumed:,} units for Agent: {client_id}")
        
        return BenchResult(
            status=final_status, 
            fuel_consumed=final_fuel_consumed, 
            tier_applied=tier.value,
            reason=reason
        )