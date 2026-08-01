# kernel.state.resonance
from __future__ import annotations
import asyncio
import json
import math
from typing import List, Dict, Optional, Any

from arch.contract.event.psi import PsiCarrier, PsiEvent
from arch.contract.interface import ICriticalDetector, IPhaseField, IDynamicsKernel
from arch.contract.registry.unified import registry, contract
from arch.contract.discovery import discover_modules

from phase.bind.resolver import find_current_self
from phase.executor.dynamics import DynamicsExecutor
from kernel.phase.runtime.node import NodeRuntime

from phase.executor.flow.cont import LoopCarrier
from watcher.plane.emitter import get_emitter

@contract.watcher("kernel.inversion")
class KernelInversion(ICriticalDetector):
    """
    @role: Field의 Pressure(피로도) 가속도를 감시, 망델브로 경계(최고점, dS/dt=0)에 도달하는 순간 파열(Rupture) 대신 역-반전(Inversion) 댐핑을 주입
    """
    def __init__(self, **kwargs):
        self.peak_threshold = kwargs.get("peak_threshold", 353.0)
        self.anchor_target = kwargs.get("anchor_target", 5.0)
        
        ## The Observer's Memory
        self.last_pressure = 0.0
        self.last_dp = 0.0

    def extract(self, field: IPhaseField) -> Dict[str, float]:
        return {"pressure": getattr(field, "pressure", 0.0)}

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        metrics = self.extract(field)
        current_pressure = metrics.get("pressure", 0.0)

        ## 1차 미분(속도)과 2차 미분(가속도) 계산
        dp = current_pressure - self.last_pressure
        ddp = dp - self.last_dp

        ## 상태 업데이트
        self.last_pressure = current_pressure
        self.last_dp = dp

        ## 망델브로 경계면 판별: 압력이 높고, 팽창 속도(dp)가 꺾여 0에 수렴하는 극대점 (dS/dt ≈ 0)
        is_peak_reached = (current_pressure >= self.peak_threshold) and (dp <= 0.01) and (ddp < 0)

        if is_peak_reached:
            expected = current_pressure - self.anchor_target
            carrier = PsiCarrier(
                kind="INVERSION", 
                tag="ARBITRAGE_SETTLEMENT", 
                payload={
                    "peak_tension": current_pressure,
                    "target": self.anchor_target,
                    "expected_yield": expected
                }
            )
            
            return PsiEvent(
                event_id=f"isorhesis-inversion-{current_tick}",
                parent_id=None,
                source_id="kernel.inversion",
                scope="GLOBAL",
                tick=current_tick,
                carrier=carrier,
                context={"state": "damping_injected", "action": "execute_bridge_tx"}
            )
        return None

@contract.watcher("kernel.singularity")
class KernelSingularity(ICriticalDetector):
    def __init__(self, **kwargs):
        self.candidate_limit = kwargs.get("candidate_limit", 10.0)
        self.rupture_limit = kwargs.get("rupture_limit", 25.0)

    def extract(self, field: IPhaseField) -> Dict[str, float]:
        return {"pressure": getattr(field, "pressure", 0.0)}

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        metrics = self.extract(field)
        pressure = metrics.get("pressure", 0.0)
        if pressure >= self.rupture_limit:
            carrier = PsiCarrier(kind="RUPTURE", tag="CRITICAL", payload={"pressure": pressure})
            return PsiEvent(
                event_id="system-rupture",
                parent_id=None,
                source_id="kernel.singularity",
                scope="GLOBAL",
                tick=current_tick,
                carrier=carrier,
                context={"state": "collapse"}
            )
        return None

@contract.kernel("kernel.resonance")
class KernelResonance(IDynamicsKernel):
    """@role: Kuramoto(물리적 동량화)와 AtorSensor(인지적 파벌 형성)의 힘을 중첩(Superposition)"""
    def __init__(self, **kwargs):
        ## 내부적으로 두 개의 독립된 세계(Kernel)를 생성
        self.kuramoto = registry.create_component("kernel", {"type": "kuramoto", "params": kwargs.get("kuramoto_params", {})})
        self.ator = registry.create_component("kernel", {"type": "ator", "params": kwargs.get("ator_params", {})})
        ## 물리적 인력(Kuramoto)과 인지적 인력(Ator)의 반영 비율 (0.0 ~ 1.0)
        self.alpha = kwargs.get("alpha", 0.5) 

    def compute_step(self, states, dt):
        ## 각각의 세계에서 다음 틱(dt)의 변화량을 계산
        k_deltas = self.kuramoto.compute_step(states, dt)
        a_deltas = self.ator.compute_step(states, dt)

        deltas = {}
        for node_id in states:
            ## 위상 중첩: 물리적 중력과 인지적 파벌의 벡터 합산
            d_phase = (k_deltas[node_id]["d_phase"] * self.alpha) + (a_deltas[node_id]["d_phase"] * (1.0 - self.alpha))
            ## 텐션 누적: 두 세계에서 발생하는 스트레스를 합산하여 극대화
            tension = k_deltas[node_id]["target_tension"] + a_deltas[node_id]["target_tension"]
            deltas[node_id] = {"d_phase": d_phase, "target_tension": tension}
        return deltas

    def render_state(self, states):
        ## 시각화는 인지적 가설(AtorSensor의 🟦🟩🟨🟥) 방식을 차용
        return self.ator.render_state(states)


async def main():
    discover_modules(find_current_self())
    log = get_emitter("rhythm.watcher", phase="BOOT")
    
    ## 보정된 컴포넌트들을 완벽히 매핑한 JSON Payload
    redis_payload = """
    {
      "system_type": "DUAL_RESONANCE_ATTRACTOR",
      "runtime": { "seed": 99, "max_ticks": 1000, "sleep_interval": 0.05, "dt": 0.1 },
      "kernel": { 
          "type": "kernel.resonance", 
          "params": { 
              "alpha": 0.4,
              "kuramoto_params": { "global_coupling": 1.2 },
              "ator_params": { "global_coupling": 1.5 }
          } 
      },
      "field": { 
          "type": "node.network",
          "params": { "size": 30, "init_phase_range": [0, 6.28], "omega_range": [0.2, 0.5] } 
      },
      "watcher": { 
          "type": "kernel.singularity",
          "params": { "candidate_limit": 10.0, "rupture_limit": 30.0 } 
      },
      "regime": { 
          "type": "node.regime",
          "params": {} 
      },
      "ators": []
    }
    """
    
    config_dict = json.loads(redis_payload)
    field_size = config_dict["field"]["params"]["size"]
    
    ## 인지적 결단(NodeAtor)을 내리는 30개의 에이전트 동적 생성
    config_dict["ators"] = [
        {
            "type": "node.ator", 
            "id": f"node_{i}", 
            ## 10%의 노드는 처음부터 극단주의자(REFLECTOR)로 배치하여 긴장 유발
            "initial_state": "REFLECTOR" if i % 10 == 0 else "NORMAL",
            "params": {"tolerance_threshold": 8.0}
        }
        for i in range(field_size)
    ]

    ## 코어 시스템 Executor 및 LoopCarrier 바인딩
    watcher_xe = DynamicsExecutor(config_dict=config_dict)
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=config_dict["runtime"]["max_ticks"], 
        interval=config_dict["runtime"]["sleep_interval"]
    )
    node = NodeRuntime(executor=loop_xe)
    
    ## 심장 박동 (Boot Pulse) 주입
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting Dual Resonance Boot Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id="boot-tick-resonance",
            parent_id=None,
            source_id="system.boot",
            scope="GLOBAL",
            tick=1,
            carrier=seed_carrier,
            phase_id=0,
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Watcher Node launching with Dual Resonance System (Kuramoto x Ator)...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())