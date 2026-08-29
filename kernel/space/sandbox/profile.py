# xphi.kernel.space.sandbox.profile
import os
import json
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional, Union

from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.cgroup import CgroupPolicy, Tier
from xphi.arch.eco.config import fuel_config, tier_config
from xphi.kernel.space.sandbox.executor import SandboxExecutor, TaskContext, SandboxEnv, EffectResolver
from xphi.watcher.plane.emitter import get_logger

log = get_logger("sandbox.profile")

# =====================================================================
# 1. Sandbox Report & Resolvers
# =====================================================================
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

class SandboxResolver(EffectResolver):
    def __init__(self, profile: Optional[MetabolicProfile] = None):
        self.profile = profile or MetabolicProfile()
        self.broker = DphiBroker(timeout=self.profile.max_compute_time)

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
            exec_res = await self.broker.execute(code=code, variables=variables, tier=target_tier.value)
            elapsed_ms = (time.time() - start_time) * 1000
            
            report.is_valid = exec_res.success
            report.output = exec_res.output if exec_res.success else ""
            report.error = str(exec_res.error) if not exec_res.success else ""
            
            fuel_consumed = 0
            mem_usage_bytes = 0
            if exec_res.success:
                try:
                    out_data = json.loads(exec_res.output)
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


# =====================================================================
# 2. Execution Profile (BenchProfile)
# =====================================================================
@dataclass
class BenchResult:
    status: str
    fuel_consumed: int
    tier_applied: str
    reason: Optional[str] = None

class BenchProfile:
    """
    에이전트 인텐트(코드)의 실제 샌드박스 실행 및 Fuel(과금 단위) 측정을 담당합니다.
    인가 및 보안 검증 로직은 Gateway 계층으로 위임하고 순수 리소스 할당 및 실행에 집중합니다.
    """
    def __init__(self):
        # 불필요한 expected_fueld_id 의존성 제거
        pass

    def _resolve_profile(self, tier: Tier) -> MetabolicProfile:
        """
        외부(Policy Engine 또는 Handler)에서 주입받은 Tier를 바탕으로
        샌드박스 실행 리소스 제한(CgroupPolicy) 프로필을 설정합니다.
        """
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

    def _charge_account(self, agent_id: str, fuel_consumed: int):
        """
        내부 회계 및 로깅을 수행합니다. 
        실제 지갑 차감이나 원장 동기화는 상위 어댑터(EcoExchange)에서 처리하는 것을 권장합니다.
        """
        billed_amount = (fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
        log.info(f"[Billing] Charged ${billed_amount:.4f} for {fuel_consumed:,} fuel units. Agent: {agent_id}")

    async def execute(
        self, 
        agent_id: str, 
        schema: Dict[str, Any], 
        entry: str, 
        depth: int, 
        tier: Tier = Tier.STANDARD,
        dry_run: bool = False
    ) -> BenchResult:
        """
        주어진 스키마(코드)를 샌드박스 환경에서 실행하고 결과 메트릭을 반환합니다.
        """
        profile = self._resolve_profile(tier)
        
        log.info(f"[{agent_id}] Target Execution Tier mapped to: {tier.value}")
        
        sandbox_resolver = SandboxResolver(profile=profile)
        executor = SandboxExecutor(resolvers={"SANDBOX": sandbox_resolver}) 
        
        flat_payload = {"schema": schema, "entry": entry, "depth": depth}
        context = TaskContext(
            task_type="execute_agent_schema",
            tier=tier.value, 
            sandbox_env=SandboxEnv.DENO,
            payload=flat_payload
        )
        
        latest_contract = None
        try:
            async for contract in executor.execute_stream(context):
                latest_contract = contract
                log.debug(f"[{contract.topos_id}] Event Stream -> State: {contract.state.name} | Kind: {contract.kind}")
                
        except Exception as e:
            log.error(f"[{context.topos_id}] Host Crash during execution: {e}")
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
            self._charge_account(agent_id, final_fuel_consumed)
        else:
            log.info(f"[Billing] Dry-run complete. Estimated {final_fuel_consumed:,} units for Agent: {agent_id}")
        
        return BenchResult(
            status=final_status, 
            fuel_consumed=final_fuel_consumed, 
            tier_applied=tier.value,
            reason=reason
        )