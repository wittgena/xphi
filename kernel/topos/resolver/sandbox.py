# kernel.topos.resolver.sandbox
## @lineage: kernel.arch.resolver.sandbox
## @lineage: arch.kernel.resolver.sandbox
## @lineage: arch.bound.profile.sandbox
import time
import json
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional, Union

from kernel.dphi.wasm.executor import EffectResolver, SandboxEnv
from watcher.plane.emitter import get_logger
from kernel.dphi.broker import WasmBroker
from kernel.dphi.cgroup import CgroupPolicy, Tier

log = get_logger("adapter.sandbox")

@dataclass
class SandboxReport:
    """Execution report returned from the dynamic sandbox environments."""
    is_valid: bool
    output: str = ""
    error: str = ""
    cgroup_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class MetabolicProfile:
    """Defines the resource limits for execution"""
    cgroup_policy: CgroupPolicy = field(default_factory=CgroupPolicy.standard)
    max_compute_time: float = 3.0  # Network timeout (Seconds)
    max_threads: int = 1
    max_node_capacity: int = 3
    max_simulation_ticks: int = 50

class SandboxResolver(EffectResolver):
    def __init__(self, profile: Optional[MetabolicProfile] = None):
        self.profile = profile or MetabolicProfile()
        self.broker = WasmBroker(timeout=self.profile.max_compute_time)

    def _get_policy_from_tier(self, tier: Tier) -> CgroupPolicy:
        if tier == Tier.SYSTEM: 
            return CgroupPolicy.system()
        elif tier == Tier.UNLIMITED: 
            return CgroupPolicy.custom(1024, 10_000_000_000)
        return CgroupPolicy.standard()

    async def resolve(self, payload: Dict[str, Any], instruction: str, env: SandboxEnv, tier: Union[Tier, str]) -> Dict[str, Any]:
        ## Tier 타입 안전성 확보
        if isinstance(tier, str):
            try:
                target_tier = Tier(tier.upper())
            except ValueError:
                target_tier = Tier.STANDARD
        else:
            target_tier = tier

        log.info(f"[SandboxResolver] Routing instruction '{instruction}' to Env '{env.value}' (Tier: {target_tier.value})")
        
        ## Update dynamic Cgroup policy based on requested tier
        self.profile.cgroup_policy = self._get_policy_from_tier(target_tier)

        ## Extract execution targets
        code = payload.get("code", "")
        variables = payload.get("variables", {})

        ## Route to Execution Plane
        report = SandboxReport(is_valid=False)
        if env in (SandboxEnv.DENO, SandboxEnv.WASM, SandboxEnv.LOCAL):
            start_time = time.time()
            exec_res = await self.broker.execute(code=code, variables=variables, tier=target_tier.value)
            elapsed_ms = (time.time() - start_time) * 1000
            
            report.is_valid = exec_res.success
            report.output = exec_res.output if exec_res.success else ""
            report.error = str(exec_res.error) if not exec_res.success else ""
            
            ## 과금을 위한 Cgroup 데이터 추출 (Fuel, Memory)
            fuel_consumed = 0
            mem_usage_bytes = 0
            if exec_res.success:
                try:
                    out_data = json.loads(exec_res.output)
                    fuel_consumed = out_data.get("fuel_consumed", 0)
                    mem_usage_bytes = out_data.get("mem_usage_bytes", 0)
                except json.JSONDecodeError:
                    pass

            ## Cgroup telemetry
            report.cgroup_metrics = {
                "tier": target_tier.value,
                "env": env.value,
                "elapsed_ms": round(elapsed_ms, 2),
                "fuel_consumed": fuel_consumed,
                "mem_usage_bytes": mem_usage_bytes,
                "mem_limit_bytes": self.profile.cgroup_policy.max_memory_bytes
            }
        elif env == SandboxEnv.DOCKER:
            # TODO: Docker 제어를 위한 독립적인 스트림(BrokerChannel.DOCKER_EXECUTE) 호출로 연결 가능
            report.error = "Docker environment is provisioned in the architecture but not yet attached."
        else:
            report.error = f"Unsupported Sandbox Environment: {env}"
            
        ## State 병합 및 결과 반환
        result_data = {**payload, "sandbox_report": report.to_dict()}
        if report.is_valid:
            result_data["execution_output"] = report.output
            
        return result_data