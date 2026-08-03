# arch.topos.flow.field
## @lineage: phase.executor.flow.field
## @lineage: watcher.xe.topos.field
"""
@phase: Autonomous topological oscillation and perturbation routing
@flow: Tension Accumulation -> Projection -> Collapse -> Re-entry
@scale: Macro-field orchestration bound
"""
import asyncio
import uuid
import time
import json
from typing import Dict, Any, Optional

from arch.topos.tunnel.factory import TunnelFactory, UniversalFacade, from_url
from arch.contract.event.psi import PsiEvent, PsiCarrier

from kernel.phase.bind.rhythm.bridge import RhythmBridge
from watcher.plane.emitter import get_emitter
from arch.topos.flow.particle import ToposManifold, Particle
from arch.topos.flow.tension import TensionAccumulator, PhaseProjector, ToposCollapse, ReentryInversion

log = get_emitter("topos.field")

class ToposField:
    def __init__(self, connection_url: Optional[str] = None):
        self.connection_url = connection_url
        self.bridge: Optional[RhythmBridge] = None
        self.dynamics_task: Optional[asyncio.Task] = None
        self.listener_task: Optional[asyncio.Task] = None
        self.tunnel: Optional[UniversalFacade] = None

    async def _init_tunnel(self):
        """@point: Tunnel 인스턴스 지연 초기화 (Lazy Initialization)"""
        if not self.tunnel:
            if self.connection_url:
                self.tunnel = await from_url(self.connection_url)
            else:
                self.tunnel = await TunnelFactory.get_default()

    async def _flow_dynamics(self):
        log.info("## Topos Autonomous Dynamics Online")
        await self._init_tunnel()
        self.bridge = RhythmBridge(self.tunnel, "rhythm.topos")
        ToposManifold.ignite_manifold()
        TensionAccumulator(bridge=self.bridge)
        PhaseProjector(bridge=self.bridge)
        ToposCollapse(bridge=self.bridge)
        ReentryInversion(bridge=self.bridge)

        while True:
            await asyncio.sleep(1)
            log.info(f"[Monitor] Field active. Nodes: {len(ToposManifold._instances)} | Tick: {ToposManifold.global_tick}")

    async def _listen_signals(self):
        ## @phase: Signal ingress boundary (External -> Internal) via Universal Tunnel
        await self._init_tunnel()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("runtime:signal")
        
        log.info("[System] Dynamics Signal Listener Online via Universal Tunnel. Waiting for triggers...")
        async for msg in pubsub.listen():
            if msg["type"] != "message": 
                continue
            try:
                data = json.loads(msg["data"])
                sig_type = data.get("type")
                await self._handle_signal(sig_type, data)
            except Exception as e:
                log.info(f"[Signal Error] Failed to process perturbation: {e}")

    async def _handle_signal(self, sig_type: str, data: dict):
        """@regime.change: Contextual orbital intervention based on signal topology"""
        if sig_type == "topos:perturb":
            log.info(time.time(), "PERTURB", "[⚡] ENTROPY FLUSH: Global Phase Reset", "CRIT")
            tasks = [inst.phase_reset() for inst in ToposManifold._instances if hasattr(inst, 'phase_reset')]
            if tasks: await asyncio.gather(*tasks)
            for q in (ToposManifold.void_gap, ToposManifold.projection_flow, ToposManifold.collapse_field):
                while not q.empty(): q.get_nowait()
        elif sig_type == "topos:inject":
            log.info(time.time(), "INJECT", "[External] Demand Tension Injected", "WARN")
            await ToposManifold.void_gap.put({"id": f"rupture.inject.{uuid.uuid4().hex[:4]}", "parent_id": "ext-inject-event"})
        elif sig_type == "origin:run":
            if not ToposManifold._instances and self.dynamics_task is None:
                self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            else:
                log.info("[System] Dynamics field is already oscillating.")
        elif sig_type == "topos:tune_reentry":
            new_factor = float(data.get("factor", 1.0))
            log.info(time.time(), "TUNE", f"[External] Tuning Re-entry Multiplier to {new_factor}", "WARN")
            tasks = [inst.update_multiplier(new_factor) for inst in ToposManifold._instances if isinstance(inst, ReentryInversion)]
            if tasks: await asyncio.gather(*tasks)

    async def start(self, auto_run: bool = False):
        """@point: Physical execution root"""
        self.listener_task = asyncio.create_task(self._listen_signals())
        if auto_run:
            self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            await asyncio.gather(self.listener_task, self.dynamics_task)
        else:
            await self.listener_task