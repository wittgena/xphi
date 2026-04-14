# system.builder
from __future__ import annotations
import asyncio
import math
import random
from typing import List, Dict, Optional, Any, Type, Callable
from bridge.bus import AsyncEventBus
from bridge.psi import PsiCarrier, PsiEvent
from bound.interface import IPhaseAtor, IPhaseField, ICriticalDetector, ISystemRegime
from bound.emitter import get_emitter
from contract.registry import registry, field_contract, watcher_contract, ator_contract, regime_contract

class DynamicSystem:
    def __init__(
        self, 
        dt: float,
        field: IPhaseField,
        ators: Dict[str, IPhaseAtor],
        watcher: ICriticalDetector,
        bus: AsyncEventBus,
        regime: ISystemRegime
    ):
        self.dt = dt
        self.tick = 0
        self.field = field
        self.ators = ators
        self.watcher = watcher
        self.bus = bus
        self.regime = regime 

        self._bus_buffer: List[PsiEvent] = []

    async def _collect_bus_events(self, event: PsiEvent):
        self._bus_buffer.append(event)

    async def process_step(self, psi: PsiEvent) -> list:
        self.tick += 1
        
        # 1. 상태 진행 (다형성: field 구현체에 따라 내부 동작이 다름)
        self.field.evolve(dt=self.dt)
        
        # 2. 액터 반응
        await self._step_react(parent_psi=psi)
        
        out_events = [psi]
        
        # 3. 옵저버 평가 및 체제 개입
        rupture_event = await self._step_evaluate(parent_psi=psi)
        if rupture_event:
            out_events.append(rupture_event)
            
        out_events.extend(self._bus_buffer)
        self._bus_buffer.clear()
        return out_events

    async def _step_react(self, parent_psi: PsiEvent):
        tick_carrier = PsiCarrier(kind="TICK", tag="LOCAL", payload={})
        tick_event = PsiEvent(
            event_id=f"t-{self.tick}", parent_id=parent_psi.event_id, source_id="SYSTEM",
            scope="LOCAL", tick=self.tick, carrier=tick_carrier, context={"phase": "loop"}
        )
        tasks = [ator.react(tick_event, self.field, self.bus) for ator in self.ators.values()]
        await asyncio.gather(*tasks)

    async def _step_evaluate(self, parent_psi: PsiEvent) -> Optional[PsiEvent]:
        rupture_event = self.watcher.evaluate(self.field, [], self.tick, parent_psi)
        if rupture_event:
            target_id = rupture_event.carrier.payload["target_node"]
            target_ator = self.ators.get(target_id)
            print(f"\n[!!! RUPTURE AT TICK {self.tick} (Node: {target_id}) !!!] Applying Regime...")
            
            if target_ator:
                self.regime.constrain_ator(target_ator)
                self.regime.modify_field(self.field, target_id)
                
        return rupture_event

class SystemBuilder:
    @staticmethod
    def build(config: Dict[str, Any], bus: Optional[AsyncEventBus] = None) -> DynamicSystem:
        runtime_cfg = config.get("runtime", {})
        dt = runtime_cfg.get("dt", 0.1)
        rng = random.Random(runtime_cfg.get("seed", 42))
        
        kernel = registry.create_component('kernel', config["kernel"])
        field = registry.create_component("field", config['field'], kernel=kernel, rng=rng)
        watcher = registry.create_component("watcher", config['watcher'])
        regime = registry.create_component("regime", config['regime'])
        
        system_bus = bus or AsyncEventBus()
        ators = {}
        
        ## 4. Ator 생성 및 등록
        for ator_conf in config.get("ators", []):
            # ator_conf 안에 'type', 'id' 등이 포함된 구조
            ator = registry.create_component("ator", ator_conf, ator_id=ator_conf["id"])
            ators[ator.ator_id] = ator
            system_bus.subscribe(ator)
            
        return DynamicsSystem(dt=dt, field=field, ators=ators, watcher=watcher, bus=system_bus, regime=regime)
