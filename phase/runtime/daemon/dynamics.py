# phase.runtime.daemon.dynamics
import asyncio
import random
from typing import List, Tuple, Any, Dict
from types import SimpleNamespace

from arch.contract.event.bus import AsyncEventBus
from arch.contract.registry.unified import registry
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from arch.contract.event.next import next_id

from phase.runtime.daemon.base import AbstractDaemon
from arch.topos.flow.cont import LoopCarrier, DynamicsXe

class DynamicsDaemon(AbstractDaemon):
    """
    @psi.evolve: 스스로 레지스트리에서 커널을 찾아 동역학 루프를 구동하는 데몬
    """
    def __init__(self, bus: AsyncEventBus):
        super().__init__("Dynamics")
        self.bus = bus
        self.carriers: List[Tuple[str, LoopCarrier]] = []

    async def start(self) -> asyncio.Task:
        self.log.info("DynamicsDaemon scanning registry for sensor kernels...")
        kernels_map = getattr(registry, "_kernels", {})
        
        for name, kernel_class in kernels_map.items():
            if name.startswith("sensor."):
                try:
                    kernel_instance = kernel_class() 
                    xe_core = DynamicsXe(bound=kernel_instance, ex=f"genesis.{name}")
                    carrier = LoopCarrier(xe=xe_core, max_ticks=999999999, interval=0.5)
                    carrier.node = SimpleNamespace(bus=self.bus)
                    self.carriers.append((name, carrier))
                except Exception as e:
                    self.log.error(f"Failed to bootstrap kernel '{name}': {e}", exc_info=True)
                
        self.log.info(f"Discovered and bound {len(self.carriers)} kernels: {[c[0] for c in self.carriers]}")
        return await super().start()

    async def run(self):
        if not self.carriers:
            self.log.warn("No kernels found in registry. Dynamics daemon idling.")
            while self.running:
                await asyncio.sleep(1)
            return

        tasks = []
        for name, carrier in self.carriers:
            initial_psi = self._create_genesis_psi(name)
            tasks.append(asyncio.create_task(carrier.execute(initial_psi)))
            
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.log.warn("DynamicsDaemon execution cancelled.")
            
    def _create_genesis_psi(self, name: str) -> PsiEvent:
        carrier = PsiCarrier(
            kind="genesis",
            tag=name,
            payload={},
            carrier_type=CarrierType.FIXED
        )
        return PsiEvent(
            event_id=next_id(),
            parent_id=None,
            source_id=f"daemon.dynamics.{name}",
            scope="GLOBAL",
            tick=0,
            carrier=carrier,
            phase_id=0,
            context={"domain": "kernel.bootstrap"}
        )