# phase.field.dynamics
"""
@phase: Autonomous topological oscillation and perturbation routing
@flow: Tension Accumulation -> Projection -> Collapse -> Re-entry
@scale: Macro-field orchestration bound
"""
import asyncio
import uuid
import time
import random
import json
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from phase.field.event.psi import PsiEvent, PsiCarrier
from bound.surface.plane import SurfacePlane
from bound.surface.emitter import get_emitter
from phase.field.bridge.rhythm import RhythmBridge
from bound.surface.emitter import get_emitter
from topos.state.manifold.particle import PhaseManifold, Particle
from topos.flow import TensionAccumulator, PhaseProjector, ToposCollapse, ReentryInversion

log = get_emitter("field.dynamics")

class FieldDynamics:
    """@scale: Macro-orchestrator mediating 4-stroke engine and external field signals"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.bridge: Optional[RhythmBridge] = None
        self.dynamics_task: Optional[asyncio.Task] = None
        self.listener_task: Optional[asyncio.Task] = None

    async def _flow_dynamics(self):
        log.info("## Topos Autonomous Dynamics Online")
        self.bridge = RhythmBridge(self.redis_url, "rhythm.topos")

        ## @point: Deploy topological particles onto the Field
        TensionAccumulator(bridge=self.bridge)
        PhaseProjector(bridge=self.bridge)
        ToposCollapse(bridge=self.bridge)
        ReentryInversion(bridge=self.bridge)

        while True:
            await asyncio.sleep(1)
            log.info(f"[Monitor] Field active. Nodes: {len(PhaseManifold._instances)} | Tick: {PhaseManifold.global_tick}")

    async def _listen_signals(self):
        ## @phase: Signal ingress boundary (External -> Internal)
        redis = redis_async.from_url(self.redis_url, decode_responses=True)
        pubsub = redis.pubsub()
        await pubsub.subscribe("runtime:signal")
        
        log.info("[System] Dynamics Signal Listener Online. Waiting for triggers...")
        async for msg in pubsub.listen():
            if msg["type"] != "message": continue
            try:
                data = json.loads(msg["data"])
                sig_type = data.get("type")
                await self._handle_signal(sig_type, data)
            except Exception as e:
                log.info(f"[Signal Error] Failed to process perturbation: {e}")

    async def _handle_signal(self, sig_type: str, data: dict):
        """@regime.change: Contextual orbital intervention based on signal topology"""
        
        if sig_type == "topos:perturb":
            ## @point: Global phase reset (Entropy Flush)
            SurfacePlane.record(time.time(), "PERTURB", "[⚡] ENTROPY FLUSH: Global Phase Reset", "CRIT")
            
            tasks = [
                inst.phase_reset() for inst in PhaseManifold._instances 
                if hasattr(inst, 'phase_reset')
            ]
            if tasks: await asyncio.gather(*tasks)
            
            ## @point: Dissipate all topological residue queues
            for q in (PhaseManifold.void_gap, PhaseManifold.projection_flow, PhaseManifold.collapse_field):
                while not q.empty(): q.get_nowait()

        elif sig_type == "topos:inject":
            ## @point: External demand tension injection
            SurfacePlane.record(time.time(), "INJECT", "[External] Demand Tension Injected", "WARN")
            await PhaseManifold.void_gap.put({
                "id": f"rupture.inject.{uuid.uuid4().hex[:4]}", 
                "parent_id": "ext-inject-event"
            })

        elif sig_type == "origin:run":
            ## @point: Initial engine ignition
            if not PhaseManifold._instances and self.dynamics_task is None:
                self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            else:
                log.info("[System] Dynamics field is already oscillating.")

        elif sig_type == "topos:tune_reentry":
            ## @regime.change: Update re-entry plasticity (Multiplier tuning)
            new_factor = float(data.get("factor", 1.0))
            SurfacePlane.record(time.time(), "TUNE", f"[External] Tuning Re-entry Multiplier to {new_factor}", "WARN")
            
            tasks = [
                inst.update_multiplier(new_factor) 
                for inst in PhaseManifold._instances 
                if isinstance(inst, ReentryInversion)
            ]
            if tasks: await asyncio.gather(*tasks)

    async def start(self, auto_run: bool = False):
        """@point: Physical execution root"""
        self.listener_task = asyncio.create_task(self._listen_signals())
        
        if auto_run:
            self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            await asyncio.gather(self.listener_task, self.dynamics_task)
        else:
            await self.listener_task

async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="Start dynamics immediately")
    parser.add_argument("--redis", default="redis://localhost:6379")
    args = parser.parse_args()

    orchestrator = FieldDynamics(args.redis)
    await orchestrator.start(auto_run=args.run)

if __name__ == "__main__":
    asyncio.run(main())