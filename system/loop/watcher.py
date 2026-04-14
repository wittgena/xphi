# system.loop.watcher
from __future__ import annotations
import asyncio
import json
from bridge.psi import PsiCarrier, PsiEvent
from node.runtime import NodeRuntime
from bound.emitter import get_emitter
from bound.resolver import find_current_self
from contract.registry import discover_modules
from contract.executor.dynamics import LoopCarrier, DynamicsExecutor

async def main():
    discover_modules(find_current_self())
    log = get_emitter("rhythm.watcher", phase="BOOT")
    
    # 1. Registry 기반 동적 빌더 규격에 맞춘 순수 JSON Payload
    redis_payload = """
    {
      "system_type": "PHASE_OSCILLATOR",
      "runtime": { "seed": 99, "max_ticks": 200, "sleep_interval": 0.1, "dt": 0.1 },
      "kernel": { 
          "type": "kuramoto", 
          "params": { "global_coupling": 1.2, "dissipation_rate": 0.90 } 
      },
      "field": { 
          "type": "global.field",
          "params": { "size": 30, "init_phase_range": [0, 6.28], "omega_range": [0.2, 0.5] } 
      },
      "watcher": { 
          "type": "bound.observer",
          "params": { "rupture_limit": 0.9 } 
      },
      "regime": { 
          "type": "rupture.regime",
          "params": { "target_state": "REFLECTOR", "reset_tension": true } 
      },
      "ators": []
    }
    """
    
    ## 2. JSON 파싱 및 동적 패치 (ConfigTopos 제거)
    config_dict = json.loads(redis_payload)
    
    ## 동적 패치: 커플링 상수 변경 및 Ator 30개 동적 생성 로직 주입
    config_dict["kernel"]["params"]["global_coupling"] = 2.0
    field_size = config_dict["field"]["params"]["size"]
    
    config_dict["ators"] = [
        {
            "type": "ToposAtor", 
            "id": str(i), 
            "initial_state": "ATTRACTOR" if i == 0 else "NORMAL",
            "params": {"reflector_boost": 0.5, "attractor_gain": 1.2}
        }
        for i in range(field_size)
    ]

    ## 3. 코어 시스템 Executor 생성 (순수 Dict 전달)
    watcher_xe = DynamicsExecutor(config_dict=config_dict)

    ## 4. LoopCarrier 래핑 및 Node 바인딩
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=config_dict["runtime"]["max_ticks"], 
        interval=config_dict["runtime"]["sleep_interval"]
    )
    node = NodeRuntime(executor=loop_xe)
    
    ## 5. 부팅 심장박동 (Boot Pulse)
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting Initial Loop Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id="boot-tick-1",
            parent_id=None,
            source_id="system.boot",
            scope="GLOBAL",
            tick=1,
            carrier=seed_carrier,
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Watcher Node launching with Registry-based Dynamic System...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())