# watcher.system
from __future__ import annotations
import asyncio
import json
import random
from bridge.psi import PsiCarrier, PsiEvent
from flow.emitter import get_emitter
from bound.resolver import find_current_self
from node.runtime import NodeRuntime
from contract.discover import discover_modules
from contract.executor.dynamics import DynamicsExecutor, LoopCarrier

log = get_emitter("watcher.system", phase="BOOT")

async def main():
    discover_modules(find_current_self())
    redis_payload = """
    {
      "system_type": "topos.attractor",
      "runtime": { "seed": 31, "max_ticks": 1301, "sleep_interval": 0.1, "dt": 0.1 },
      "kernel": { 
          "type": "kuramoto", 
          "params": { "tension_rate": 0.1, "phase_rate": 0.2, "drift_max": 0.2 } 
      },
      "field": { 
          "type": "network.topos.field", 
          "params": { "size": 15, "init_phase_range": [0, 0.5], "omega_range": [0.8, 1.2] } 
      },
      "watcher": { 
          "type": "singularity.watcher",
          "params": { "candidate_limit": 10.0, "rupture_limit": 17.0 } 
      },
      "regime": { 
          "type": "phase.transition.regime",
          "params": {} 
      },
      "ators": []
    }
    """
    
    ## JSON 파싱 및 동적 패치
    config_dict = json.loads(redis_payload)
    field_size = config_dict["field"]["params"]["size"]
    
    ## 동적 패치: Ator(노드 에이전트) 생성 및 특정 노드를 REFLECTOR로 강제 지정
    ators_config = []
    reflector_ids = ["3", "10"] ## 기존 flow.attractor에서 3번, 10번 노드 역할
    
    for i in range(field_size):
        node_id = str(i)
        initial_state = "REFLECTOR" if node_id in reflector_ids else "NORMAL"
        
        ators_config.append({
            "type": "phase.node.ator", 
            "id": node_id, 
            "initial_state": initial_state,
            "params": { "reflector_boost": 0.5 }
        })
        
    config_dict["ators"] = ators_config

    ## 코어 시스템 Executor 생성 - DynamicsExecutor 내부에서 SystemBuilder.build(config_dict)가 호출되며 조립됨
    watcher_xe = DynamicsExecutor(config_dict=config_dict)

    ## LoopCarrier 래핑 및 Node 바인딩
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=config_dict["runtime"]["max_ticks"], 
        interval=config_dict["runtime"]["sleep_interval"]
    )
    node = NodeRuntime(executor=loop_xe)
    
    ## 부팅 심장박동 (Boot Pulse)
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting Phase Transition Boot Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id="boot-tick-attractor",
            parent_id=None,
            source_id="system.boot",
            scope="GLOBAL",
            tick=1,
            carrier=seed_carrier,
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Watcher Node launching Phase Singularity Attractor System...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())