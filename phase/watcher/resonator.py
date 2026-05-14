# phase.watcher.resonator
## @lineage: topos.watcher.resonator
## @lineage: cognitive.watcher.resonator
## @lineage: cognitive.field.resonator
## @lineage: cognitive.resonator
"""
@flow:
  Ψ (event)
  → Φ′ (ator evaluation)
  → {accept | transform | reject}
  → Φ (state evo)
  → Ψ′ (next emission)
"""
import asyncio
import time
import json
import uuid
import math
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from arch.model.event.psi import PsiEvent
from arch.model.event.bus import AsyncEventBus
from cognitive.rhythm.coupler import RhythmCoupler
from arch.contract.interface import IPhaseField, IPhaseAtor, IEventBus, IDynamicsKernel
from phase.plane.surface import SurfacePlane

class SimpleKernel(IDynamicsKernel):
    """내부 장력(Tension)의 감쇠와 위상 전이를 계산하는 커널"""
    def compute_step(self, states, dt):
        out = {}
        for k, v in states.items():
            tension = v["tension"]
            out[k] = {
                "d_phase": tension * 0.1,
                "target_tension": tension * 0.95  # 5% decay
            }
        return out

class PhaseRhythm:
    """
    353 기반의 위상 진동자 (Oscillatory Runtime Rhythm).
    우주(환경)의 거시적인 간섭(Interference)과 부하(Load)를 생성합니다.
    """
    def __init__(self):
        # 고유 주파수 (base: 353)
        self.freq_a = (11 * math.pi) / 353
        self.freq_b = (4.0 * math.pi) / 353
        self.phase_a = self.freq_a * 14
        self.phase_b = 0.0
        self.threshold = math.cos(self.phase_a / 5)

    def evolve(self) -> float:
        """시간에 따른 파동 진행 (Evolve)"""
        self.phase_a += self.freq_a
        self.phase_b += self.freq_b
        return self.emit()

    def predict_future_load(self, steps_ahead: int) -> float:
        """미래의 간섭(파동 겹침) 예측"""
        future_a = self.phase_a + (self.freq_a * steps_ahead)
        future_b = self.phase_b + (self.freq_b * steps_ahead)
        interference = math.sin(future_a) * math.cos(future_b)
        return abs(interference)

    def emit(self) -> float:
        """현재의 파동 간섭도 반환"""
        interference = math.sin(self.phase_a) * math.cos(self.phase_b)
        if abs(interference) > self.threshold:
            return math.pi
        return 1.1

class PhaseField(IPhaseField):
    """
    Φ: Phase Manifold (위상 공간)
    
    @role:
    - 리듬(Rhythm)을 내재하여 자발적 동력을 확보
    - 위상 상태 (nodes_state) 유지
    - 커널 역학 (Φ → Φ′Δ) 적용
    """
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
        """@flow: Φ → kernel → ΔΦ → Φ"""
        ## 내재된 리듬(Rhythm)의 진행 및 현재 Load 추출
        current_load = self.rhythm.evolve()
        self.nodes_state["0"]["load"] = current_load

        ## 커널을 통한 상태 미분 계산 및 갱신
        deltas = self.kernel.compute_step(self.nodes_state, dt)
        for node_id, delta in deltas.items():
            self.nodes_state[node_id]["phase"] += delta.get("d_phase", 0.0)
            if "target_tension" in delta:
                self.nodes_state[node_id]["tension"] = delta["target_tension"]
                
        return current_load

    def update(self, delta: float):
        """외부 자극(Ψ) 흡수를 통한 장력(Tension) 증가"""
        self.nodes_state["0"]["tension"] += delta

    def reset(self):
        """@flow: collapse → re-anchor"""
        self.nodes_state["0"]["tension"] = 0.2

def to_event(psi: PsiEvent) -> PsiEvent:
    """@role: raw event → structured Ψ projection"""
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
    """
    Φ′: Evaluation Kernel (판단자)
    @role: 유입된 자극(Ψ)이 현재 위상(Φ)에 흡수될지 판단
    """
    def __init__(self, ator_id: str, threshold_base: float = 0.6):
        self._id = ator_id
        self.threshold_base = threshold_base

    @property
    def ator_id(self) -> str:
        return self._id

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus) -> str:
        """@flow: Ψ → Φ′ → {accept | transform | reject}"""
        my_field = field.get_state().get("0", {"tension": 0.1})
        tension = my_field["tension"]

        strength = event.payload.get("strength", 0.5)
        
        # 위상의 장력(tension)이 높을수록 자극을 거부하는 방어 기제 형성
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

    async def pulse(self):
        """@role: Endogenous driver (내부의 자발적 Ψ 생성기)"""
        tick = 0
        while self.running:
            tick += 1
            ## 장(Field)과 리듬의 시간적 진화
            current_load = self.field.evolve(1.0)
            
            ## 내재적 파동을 기반으로 한 자극(Ψ) 방출
            psi = PsiEvent(
                kind="internal:pulse",
                tag=uuid.uuid4().hex[:6],
                payload={"strength": 0.4 + (tick % 4) * 0.1, "load": current_load},
                tick=time.time()
            )
            await self.psi_stream.put(psi)

            ## 과포화 시 붕괴(Collapse) 및 재귀
            if self.field.get_state()["0"]["tension"] > 1.2:
                self.field.reset()
                self.version = 0
                await asyncio.sleep(2.0)

            await asyncio.sleep(1.0)

    async def interpret(self):
        """@flow: Ψ → Φ′ (ator evaluation) → routing"""
        while self.running:
            psi = await self.psi_stream.get()
            
            ## 판단자(Ator)를 통한 자극 필터링
            decision = await self.ator.react(to_event(psi), self.field, self.bus)

            ## Routing
            if decision == "accept":
                await self.reentry_stream.put(psi)
            elif decision == "transform":
                psi.payload["strength"] *= 1.1
                await self.reentry_stream.put(psi)
            ## reject는 버려짐 (No-op)
            self.psi_stream.task_done()

    async def reentry(self):
        """@flow: Φ (state evo) → Ψ′ (next emission)"""
        while self.running:
            psi = await self.reentry_stream.get()
            self.version += 1
            
            ## 흡수 (Absorption): 위상 공간 변형
            delta = psi.payload.get("strength", 0.5) * 0.5
            self.field.update(delta)
            
            ## 경계 추출 (Boundary Gradient)
            dphi = self.field.compute_gradient()
            
            ## 다음 상태 방출 (Ψ′ Emission)
            await self.redis.publish(
                "phase:decision",
                json.dumps({
                    "event_tag": psi.tag,
                    "decision_type": psi.kind,
                    "tension": self.field.get_state()["0"]["tension"],
                    "dphi": dphi,
                    "version": self.version
                })
            )
            self.reentry_stream.task_done()

async def main():
    redis = redis_async.from_url("redis://localhost:6379", decode_responses=True)
    bus = AsyncEventBus(redis)
    ator = CognitiveAtor(ator_id="core_ator")
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