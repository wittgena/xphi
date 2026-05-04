# bound.watcher.exchange
from __future__ import annotations
import asyncio
import json
from phase.field.event.psi import PsiCarrier, PsiEvent
from bound.surface.emitter import get_emitter
from bound.resolver import find_current_self
from phase.node.runtime import NodeRuntime
from phase.contract.discover import discover_modules
from phase.node.executor.dynamics import LoopCarrier, DynamicsExecutor

async def main():
    discover_modules(find_current_self())
    log = get_emitter("watcher.exchange", phase="BOOT")
    
    ## ExchangeSensor(시장 평균장)에 맞춘 JSON Payload
    redis_payload = """
    {
      "system_type": "EXCHANGE_MEAN_FIELD_ATTRACTOR",
      "runtime": { "seed": 77, "max_ticks": 1000, "sleep_interval": 0.05, "dt": 0.1 },
      "kernel": { 
          "type": "exahange", 
          "params": { 
              "global_coupling": 1.5,
              "herd_threshold": 0.35 
          } 
      },
      "field": { 
          "type": "node.network",
          "params": { "size": 30, "init_phase_range": [0, 6.28], "omega_range": [0.1, 0.8] } 
      },
      "watcher": { 
          "type": "singularity.watcher",
          "params": { "candidate_limit": 10.0, "rupture_limit": 25.0 } 
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
    
    ## 시장 참여자(Ator) 동적 생성: 마켓 메이커(Whale) vs 일반 투자자(Retail)
    config_dict["ators"] = [
        {
            "type": "node.ator", 
            "id": f"trader_{i}", 
            ## 10%의 노드는 시장 평균에 휩쓸리지 않는 절대적 닻(Market Maker) 역할
            "initial_state": "ATTRACTOR" if i % 10 == 0 else "NORMAL",
            ## 버티다 못해 흑화(REFLECTOR/공매도)하는 인지 부조화 한계점
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
    
    ## 심장 박동 (Market Open Pulse) 주입
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting Market Mean-Field Boot Pulse (Bell Ringing)... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="MARKET_OPEN", payload={})
        seed_event = PsiEvent(
            event_id="boot-tick-exchange",
            parent_id=None,
            source_id="system.exchange",
            scope="GLOBAL",
            tick=1,
            carrier=seed_carrier,
            phase_id=0,
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Exchange Node launching Macroscopic Herd Dynamics (Exa-Hange)...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())