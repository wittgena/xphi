# data.loop
"""
@desc: Phase Loop (Ψ → Φ′ → Ψ′)

@flow:
Ψ (event)
 → Φ′ (ator evaluation)
 → {accept | transform | reject}
 → Φ (state evolution)
 → Ψ′ (next emission)
"""
import asyncio
import time
import json
import uuid
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from phase.event.psi import PsiEvent
from phase.event.bus import AsyncEventBus
from session.contract.proto.interface import IPhaseField, IPhaseAtor, IEventBus, IDynamicsKernel
from session.bound.plane import BoundPlane
from field.rhythm.coupler import RhythmCoupler

class SimpleKernel(IDynamicsKernel):
    def compute_step(self, states, dt):
        out = {}
        for k, v in states.items():
            tension = v["tension"]
            out[k] = {
                "d_phase": tension * 0.1,
                "target_tension": tension * 0.95
            }
        return out

class Field(IPhaseField):
    """
    Φ: phase manifold

    @role:
    - holds state (Φ)
    - applies kernel dynamics (Φ → Φ′Δ)
    """
    def __init__(self, kernel: Optional[IDynamicsKernel] = None):
        self.kernel = kernel or SimpleKernel()
        self.nodes_state = {
            "0": {
                "phase": 0.0,
                "tension": 0.1,
                "entropy": 0.1,
                "load": 0.1,
            }
        }

    def get_state(self) -> Dict[str, Any]:
        return self.nodes_state

    def compute_gradient(self) -> Dict[str, float]:
        return {k: v["tension"] for k, v in self.nodes_state.items()}

    def evolve(self, dt: float) -> None:
        """@flow: Φ → kernel → ΔΦ → Φ"""
        deltas = self.kernel.compute_step(self.nodes_state, dt)

        for node_id, delta in deltas.items():
            self.nodes_state[node_id]["phase"] += delta.get("d_phase", 0.0)
            if "target_tension" in delta:
                self.nodes_state[node_id]["tension"] = delta["target_tension"]

    def update(self, delta: float):
        self.nodes_state["0"]["tension"] += delta

    def reset(self):
        """
        @flow: collapse → re-anchor
        """
        self.nodes_state["0"]["tension"] = 0.2

class Evaluator:
    """Φ′: evaluation kernel (stateless)"""
    def evaluate(self, psi: PsiEvent, field: Field) -> str:
        strength = psi.payload.get("strength", 0.5)
        tension = field.nodes_state["0"]["tension"]

        threshold = 0.6 + tension * 0.3
        if strength > threshold:
            return "accept"
        elif strength < 0.2:
            return "reject"
        return "transform"

def to_event(psi: PsiEvent) -> PsiEvent:
    """@role: Ψ → structured event projection"""
    return PsiEvent(
        event_id=psi.tag,
        parent_id=None,
        event_type=psi.kind,
        source_id="tloop",
        scope="LOCAL",
        payload=psi.payload,
        tick=int(psi.tick),
    )

class CognitiveAtor(IPhaseAtor):
    """Φ′ ator"""
    def __init__(self, ator_id: str, threshold_base: float = 0.6):
        self._id = ator_id
        self.threshold_base = threshold_base

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return "ACTIVE"

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus) -> str:
        """@flow: Ψ → Φ′ → decision"""
        my_field = field.get_state().get(self._id, {"tension": 0.1})
        tension = my_field["tension"]

        strength = event.payload.get("strength", 0.5)
        threshold = self.threshold_base + tension * 0.3

        if strength > threshold:
            return "accept"
        elif strength < 0.2:
            return "reject"
        return "transform"

class FieldLoop:
    """
    @loop: circular stabilization loop
    @flow: Ψ → Φ′ → Φ → Ψ′
    """
    def __init__(self, redis, ator: IPhaseAtor):
        self.redis = redis
        self.ator = ator

        self.psi_stream = asyncio.Queue()
        self.reentry_stream = asyncio.Queue()

        self.field = Field()  # kernel-aligned 유지
        self.version = 0
        self.running = True

    async def emit(self, kind, payload):
        """@flow: Φ → Ψ emission"""
        psi = PsiEvent(
            kind=kind,
            tag=uuid.uuid4().hex[:6],
            payload=payload,
            tick=time.time()
        )
        await self.psi_stream.put(psi)

    async def interpret(self):
        """@flow: Ψ → Φ′ → routing"""
        while self.running:
            psi = await self.psi_stream.get()
            decision = await self.ator.react(
                to_event(psi),
                self.field,
                None
            )

            if decision == "accept":
                await self.reentry_stream.put(psi)

            elif decision == "transform":
                psi.payload["strength"] *= 1.1
                await self.reentry_stream.put(psi)

            self.psi_stream.task_done()

    async def reentry(self):
        """@flow: state update + projection"""
        while self.running:
            psi = await self.reentry_stream.get()
            self.version += 1
            delta = psi.payload.get("strength", 0.5) * 0.5

            self.field.update(delta)
            
            ## ∂Φ (boundary extraction)
            dphi = self.field.compute_gradient()
            
            await self.redis.publish(
                "phase:decision",
                json.dumps({
                    "tension": self.field.get_state()["0"]["tension"],
                    "dphi": dphi,
                    "version": self.version
                })
            )
            self.reentry_stream.task_done()

    async def pulse(self):
        """@role: endogenous driver (internal Ψ generator)"""
        tick = 0
        while self.running:
            tick += 1
            self.field.evolve(1.0)
            await self.emit("internal:pulse", {
                "strength": 0.4 + (tick % 4) * 0.1
            })

            if self.field.get_state()["0"]["tension"] > 1.2:
                self.field.reset()
                self.version = 0
                await asyncio.sleep(2.0)

            await asyncio.sleep(2.0)

async def main():
    redis = redis_async.from_url("redis://localhost:6379", decode_responses=True)
    bus = AsyncEventBus(redis)
    loop = FieldLoop(redis, bus=bus)
    coupler = RhythmCoupler(loop, redis, bus=bus)
    await asyncio.gather(
        loop.pulse(),
        loop.interpret(),
        loop.reentry(),
        coupler.start(),
    )

if __name__ == "__main__":
    asyncio.run(main())