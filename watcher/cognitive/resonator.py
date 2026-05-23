# cognitive.resonator
"""
@flow: Ψ (event) -> Φ′ (ator evaluation) -> {accept | transform | reject} -> Φ (state ego) -> Ψ′ (next emission)
@tick.step: $xe$ -> 파동 생성 -> 필터링(경계) -> 장(Field) 형성 -> Redis 기록 -> 붕괴/재구축
"""
import asyncio
import time
import json
import uuid
import math
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from arch.contract.event.next import next_id, next_phase_id
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.event.bus import AsyncEventBus
from cognitive.reflect.rhythm.coupler import RhythmCoupler
from arch.contract.interface import IPhaseField, IPhaseAtor, IEventBus, IDynamicsKernel
from phase.plane.surface import SurfacePlane
from phase.plane.emitter import get_emitter

log = get_emitter("cognitive.resonator")

class SimpleKernel(IDynamicsKernel):
    """내부 장력(Tension)의 감쇠와 위상 전이를 계산하는 커널"""
    def compute_step(self, states, dt):
        out = {}
        for k, v in states.items():
            tension = v["tension"]
            out[k] = {
                "d_phase": tension * 0.1,
                "target_tension": tension * 0.95  ## 5% decay
            }
        return out

class PhaseRhythm:
    """353 Oscillatory Runtime Rhythm"""
    def __init__(self):
        self.freq_a = (11 * math.pi) / 353
        self.freq_b = (4.0 * math.pi) / 353
        self.phase_a = self.freq_a * 14
        self.phase_b = 0.0
        self.threshold = math.cos(self.phase_a / 5)

    def evolve(self) -> float:
        self.phase_a += self.freq_a
        self.phase_b += self.freq_b
        return self.emit()

    def predict_future_load(self, steps_ahead: int) -> float:
        future_a = self.phase_a + (self.freq_a * steps_ahead)
        future_b = self.phase_b + (self.freq_b * steps_ahead)
        interference = math.sin(future_a) * math.cos(future_b)
        return abs(interference)

    def emit(self) -> float:
        interference = math.sin(self.phase_a) * math.cos(self.phase_b)
        if abs(interference) > self.threshold:
            return math.pi
        return 1.1

class PhaseField(IPhaseField):
    """Φ: Phase Manifold"""
    def __init__(self, kernel: Optional[IDynamicsKernel] = None):
        self.kernel = kernel or SimpleKernel()
        self.rhythm = PhaseRhythm()
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

    def evolve(self, dt: float) -> float:
        current_load = self.rhythm.evolve()
        self.nodes_state["0"]["load"] = current_load

        deltas = self.kernel.compute_step(self.nodes_state, dt)
        for node_id, delta in deltas.items():
            self.nodes_state[node_id]["phase"] += delta.get("d_phase", 0.0)
            if "target_tension" in delta:
                self.nodes_state[node_id]["tension"] = delta["target_tension"]
                
        return current_load

    def update(self, delta: float):
        self.nodes_state["0"]["tension"] += delta

    def reset(self):
        self.nodes_state["0"]["tension"] = 0.2

def to_event(psi: PsiEvent) -> PsiEvent:
    return PsiEvent(
        event_id=psi.tag,
        parent_id=None,
        source_id="cognitive",
        scope="LOCAL",
        tick=int(psi.tick),
        carrier=psi.carrier
    )

class CognitiveAtor(IPhaseAtor):
    """Φ′: Evaluation Kernel"""
    def __init__(self, ator_id: str, threshold_base: float = 0.6):
        self._id = ator_id
        self.threshold_base = threshold_base
        self._internal_state = {"status": "active"}

    @property
    def ator_id(self) -> str:
        return self._id
    
    @property
    def state(self) -> Dict[str, Any]:
        return self._internal_state

    def set_state(self, new_state: str) -> None:
        self._internal_state["status"] = new_state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus) -> str:
        my_field = field.get_state().get("0", {"tension": 0.1})
        tension = my_field["tension"]
        strength = event.payload.get("strength", 0.5)
        
        ## 위상의 장력이 높을수록 방어적
        threshold = self.threshold_base + tension * 0.3
        if strength > threshold:
            return "accept"
        elif strength < 0.2:
            return "reject"
        return "transform"

class CognitiveResonator:
    def __init__(self, redis, ator: IPhaseAtor, bus=None):
        self.redis = redis
        self.ator = ator
        self.bus = bus

        self.psi_stream = asyncio.Queue()
        self.reentry_stream = asyncio.Queue()

        self.field = PhaseField()
        self.version = 0
        self.running = True

    async def emit(self, kind: str, payload: dict):
        """@role: 외부(Coupler) 자극 주입 엔드포인트"""
        event_tag = next_id()
        init_phase_id = next_phase_id(topo=100, press=int(payload.get("strength", 0.5) * 100), rupture=True)

        carrier = PsiCarrier(kind=kind, tag=event_tag, payload=payload)
        psi = PsiEvent(
            event_id=event_tag,
            parent_id=None,
            source_id="external_coupler",
            scope="GLOBAL",
            tick=int(time.time()),
            carrier=carrier,
            phase_id=init_phase_id
        )
        await self.psi_stream.put(psi)
        log.info(f"  [Resonator:Emit] 외부 자극 주입: {kind} (ID: {event_tag})")

    async def pulse(self):
        """@role: Endogenous driver (내부 자발적 Ψ 생성기)"""
        tick = 0
        while self.running:
            tick += 1
            current_load = self.field.evolve(1.0)
            
            event_tag = next_id()
            strength = 0.4 + (tick % 4) * 0.1
            current_tension = self.field.get_state()["0"]["tension"]
            p_id = next_phase_id(topo=int(current_load * 100), press=int(current_tension * 1000))

            carrier = PsiCarrier(
                kind="internal:pulse",
                tag=event_tag,
                payload={"strength": strength, "load": current_load}
            )
            psi = PsiEvent(
                event_id=event_tag,
                parent_id=None,
                source_id="endogenous_driver",
                scope="LOCAL",
                tick=int(time.time()),
                carrier=carrier,
                phase_id=p_id
            )
            await self.psi_stream.put(psi)
            log.info(f"  [Pulse] 자발적 파동: {event_tag} (PhaseID: {hex(p_id)})")

            current_tension = self.field.get_state()["0"]["tension"]
            if current_tension > 1.2:
                log.warning(f"  [Pulse] 위상 장력 과포화! - 붕괴 및 리셋 발생.")
                self.field.reset()
                self.version = 0
                await asyncio.sleep(2.0)
            await asyncio.sleep(1.0)

    async def interpret(self):
        """@flow: Ψ → Φ′ (ator evaluation) → routing"""
        while self.running:
            psi = await self.psi_stream.get()
            
            decision = await self.ator.react(to_event(psi), self.field, self.bus)
            log.info(f"  [Interpret] 판단자 평가 결과: {psi.tag} -> {decision.upper()}")

            if decision == "accept":
                await self.reentry_stream.put(psi)
            elif decision == "transform":
                psi.payload["strength"] *= 1.1
                await self.reentry_stream.put(psi)
            
            self.psi_stream.task_done()

    async def reentry(self):
        """@flow: Φ (state evo) → Ψ′ (next emission)"""
        while self.running:
            psi = await self.reentry_stream.get()
            self.version += 1
            
            delta = psi.payload.get("strength", 0.5) * 0.5
            self.field.update(delta)
            dphi = self.field.compute_gradient()
            
            tension = self.field.get_state()["0"]["tension"]
            log.info(f"  [Reentry] 위상 공간 장력 흡수: {tension:.3f} (Version: {self.version})")
            
            await self.redis.publish(
                "phase:decision",
                json.dumps({
                    "event_tag": psi.tag,
                    "decision_type": psi.kind,
                    "tension": tension,
                    "dphi": dphi,
                    "version": self.version
                })
            )
            self.reentry_stream.task_done()

async def main():
    log.info("="*59)
    log.info("Cognitive Resonator Boot Sequence Initiated...")
    log.info("="*59)
    
    redis = redis_async.from_url("redis://localhost:6379", decode_responses=True)
    bus = AsyncEventBus()
    ator = CognitiveAtor(ator_id="cognitive.ator")
    loop = CognitiveResonator(redis, ator=ator, bus=bus)
    coupler = RhythmCoupler(loop, redis, bus=bus)
    await asyncio.gather(
        loop.pulse(),
        loop.interpret(),
        loop.reentry(),
        coupler.start(),
    )

if __name__ == "__main__":
    asyncio.run(main())