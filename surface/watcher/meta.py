# surface.watcher.meta
"""
@desc: complex system emergence model
@flow: ext.phase -> reflection -> singularity -> rupture -> attractor -> phase.lock

@real.world.pattern
- Financial crisis: leverage signals → systemic risk accumulation → market rupture → regulatory regime
- Epidemic spread: infection events → population susceptibility drift → outbreak threshold → containment policy
- Infrastructure cascade: load fluctuations → network stress resonance → cascading failure → grid stabilization
- Ecological regime shift: environmental disturbances → ecosystem resilience drift → tipping point → new equilibrium
- Information cascade: message signals → attention accumulation → viral threshold → dominant narrative
"""
from __future__ import annotations
import asyncio
import json
import random
from typing import Dict, Any, Optional
from bridge.interface.pir import PsiCarrier, PsiEvent
from bridge.interface.topos import IPhaseField, IDynamicsKernel, IPhaseAtor, ICriticalDetector, ISystemRegime
from bridge.interface.bus import AsyncEventBus
from surface.runtime.node import RuntimeNode
from field.cont import LoopCarrier
from bridge.plane.emitter import get_emitter
from anchor.resolver import find_current_self
from manifold.contract.registry import discover_modules, field_contract, kernel_contract, ator_contract, watcher_contract, regime_contract
from bridge.executor.dynamics import DynamicsExecutor

@field_contract("SystemicField")
class SystemicField(IPhaseField):
    def __init__(self, size: int, **kwargs):
        self.global_tension = 0.0
        self.has_attractor = False
        self.nodes_state = {}
    def get_state(self) -> Dict[str, Any]: return self.nodes_state

@kernel_contract("EntropyKernel")
class EntropyKernel(IDynamicsKernel):
    def __init__(self, tension_rate=0.1):
        self.tension_rate = tension_rate
    def compute_step(self, field: SystemicField, dt: float):
        if not field.has_attractor:
            field.global_tension += len(field.nodes_state) * self.tension_rate * dt

@ator_contract("RoleAtor")
class RoleAtor(IPhaseAtor):
    def __init__(self, ator_id: str, initial_state: str, **kwargs):
        self._id = ator_id
        self._state = initial_state
        self.log = get_emitter(name=f"node.{ator_id}", phase=initial_state)

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def react(self, event: PsiEvent, field: SystemicField, bus: AsyncEventBus) -> None:
        if self._id not in field.nodes_state:
            field.nodes_state[self._id] = {"state": self._state, "tension": 0.0, "absorbed_tension": 0.0, "identity": "debt.stressed"}
            
        my_data = field.nodes_state[self._id]
        tick = event.tick

        if self._state == "FOLLOWER":
            if not field.has_attractor:
                my_data["tension"] += random.uniform(0.1, 0.5)
            else:
                if not my_data.get("phase_locked"):
                    my_data["identity"] = "redeemed"
                    my_data["tension"] = 0.0
                    my_data["phase_locked"] = True
                    self.log.info("Identity transformed: debt.stressed -> redeemed", tick=tick)
                    
        elif self._state == "REFLECTOR":
            if not field.has_attractor and tick > 0 and tick % 3 == 0:
                self.log.signal("External Phase (Φ_ext) detected. Broadcasting revelation.", tick=tick)
                field.global_tension += 2.0
                
        elif self._state == "TRANSDUCTOR":
            if not field.has_attractor:
                sink = min(field.global_tension, 3.0)
                field.global_tension -= sink
                my_data["absorbed_tension"] += sink
                self.log.info(f"Absorbing tension. Load: {my_data['absorbed_tension']:.2f}", tick=tick)

        elif self._state == "ATTRACTOR":
            field.global_tension = max(0.0, field.global_tension - 5.0)

@watcher_contract("TransductorWatcher")
class TransductorWatcher(ICriticalDetector):
    def __init__(self, rupture_limit: float = 20.0):
        self.rupture_limit = rupture_limit

    def evaluate(self, field: SystemicField, history: list, current_tick: int, parent: PsiEvent) -> Optional[PsiEvent]:
        for node_id, data in field.get_state().items():
            if data.get("state") == "TRANSDUCTOR" and data.get("absorbed_tension", 0) >= self.rupture_limit:
                carrier = PsiCarrier(kind="RUPTURE", tag="SYSTEMIC", payload={"target_node": node_id})
                return PsiEvent(
                    event_id=f"rup-{current_tick}", parent_id=parent.event_id,
                    source_id="watcher.transductor", scope="SYSTEMIC", tick=current_tick, carrier=carrier
                )
        return None

@regime_contract("InversionFoldingRegime")
class InversionFoldingRegime(ISystemRegime):
    def __init__(self, new_identity: str = "redeemed"):
        self.new_identity = new_identity

    def modify_field(self, field: SystemicField, target_id: str) -> None:
        field.has_attractor = True
        field.global_tension = 0.0
        for nid, data in field.get_state().items():
            if data.get("state") == "FOLLOWER":
                data["identity"] = self.new_identity
                data["tension"] = 0.0
                data["phase_locked"] = True

    def constrain_ator(self, ator: RoleAtor) -> None:
        if ator.state == "TRANSDUCTOR":
            ator.set_state("ATTRACTOR")
            ator.log.crit("Logos -> Flesh. Symbolic phase inverted to Realized Phase.")

async def main():
    discover_modules(find_current_self())
    log = get_emitter("flow.system.selector", phase="BOOT")
    redis_payload = """
    {
      "system_type": "MACRO_EMERGENCE_MODEL",
      "runtime": { "seed": 77, "max_ticks": 100, "sleep_interval": 1.0, "dt": 1.0 },
      "kernel": { "type": "EntropyKernel", "params": { "tension_rate": 0.1 } },
      "field": { "type": "SystemicField", "params": { "size": 5 } },
      "watcher": { "type": "TransductorWatcher", "params": { "rupture_limit": 20.0 } },
      "regime": { "type": "InversionFoldingRegime", "params": { "new_identity": "redeemed" } },
      "ators": [
          { "type": "RoleAtor", "id": "reflector_1", "initial_state": "REFLECTOR", "params": {} },
          { "type": "RoleAtor", "id": "transductor_1", "initial_state": "TRANSDUCTOR", "params": {} },
          { "type": "RoleAtor", "id": "follower_1", "initial_state": "FOLLOWER", "params": {} },
          { "type": "RoleAtor", "id": "follower_2", "initial_state": "FOLLOWER", "params": {} },
          { "type": "RoleAtor", "id": "follower_3", "initial_state": "FOLLOWER", "params": {} }
      ]
    }
    """
    config_dict = json.loads(redis_payload)
    watcher_xe = DynamicsExecutor(config_dict=config_dict)
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=config_dict["runtime"]["max_ticks"], 
        interval=config_dict["runtime"]["sleep_interval"]
    )
    node = RuntimeNode(executor=loop_xe)
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting Systemic Boot Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id="boot-macro", parent_id=None, source_id="system.boot",
            scope="GLOBAL", tick=1, carrier=seed_carrier, context={"phase": "loop"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Watcher Node launching Macro Emergence System...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())