# phase.sphere.projector
import asyncio
import math
import random
import uuid
import datetime
import time
from typing import Dict, Any, List, Optional
from bound.event.bus import AsyncEventBus
from bound.event.psi import PsiCarrier, PsiEvent
from session.contract.proto.interface import IPhaseAtor, IPhaseField, IEventBus
from phase.receptor.lens.trajectory import (
    Point, 
    ContinuousTrajectory,
    DefaultBoundLensStrategy,
    SlidingWindowStrategy,
    WindowProjector
)
from phase.sphere.container.scale.emitter import ScaleEmitter
from bound.surface.emitter import get_emitter

class ToposField(IPhaseField):
    """
    @role: Φ
    @desc: Event-driven state evolution (no direct mutation)
    """
    def __init__(self):
        self.state = {
            "worker-1": {
                "tension": 0.1,
                "replicas": 2,
                "last_update": time.time()
            }
        }
        self.log = get_emitter("field.topos", phase="FIELD")

    def get_state(self) -> Dict[str, Any]:
        return self.state

    def evolve(self, dt: float):
        # 자연 감쇠
        for rid in self.state:
            self.state[rid]["tension"] *= 0.95

    def compute_gradient(self):
        return {}

    # 핵심: event 기반 상태 변경
    async def react(self, event: PsiEvent):
        if event.event_type == "action.scale.apply":
            rid = event.payload["target"]
            if rid not in self.state:
                return

            self.state[rid]["replicas"] += event.payload.get("delta", 1)
            self.state[rid]["tension"] += 0.2
            self.state[rid]["last_update"] = time.time()
            self.log.info(
                f"[FIELD] {rid} scaled → replicas={self.state[rid]['replicas']}"
            )

class KineticLensProjector(IPhaseAtor):
    """Lens  (unchanged logic + pruning)"""
    def __init__(self, ator_id: str):
        self._id = ator_id

        self.lens = DefaultBoundLensStrategy("kinematic")
        self.projector = WindowProjector(
            SlidingWindowStrategy(window_days=1, step_days=0.1)
        )

        self.trajectories: Dict[str, ContinuousTrajectory] = {}
        self.max_points = 100

        self.log = get_emitter(f"ator.{ator_id}", phase="PERCEPTION")

    @property
    def ator_id(self): return self._id
    @property
    def state(self): return {}

    async def react(self, event: PsiEvent, field, bus):
        if event.event_type != "metric.observed":
            return

        rid = event.source_id
        point = Point(
            timestamp=datetime.datetime.now(),
            value=event.payload["value"]
        )
        traj = self.trajectories.setdefault(
            rid,
            ContinuousTrajectory(identity=rid, points=[])
        )
        traj.points.append(point)

        # pruning
        if len(traj.points) > self.max_points:
            traj.points = traj.points[-self.max_points:]

        windows = self.projector.project(traj)
        if not windows:
            return

        analysis = self.lens.scan(windows[-1])
        if analysis["status"] == "valid":
            carrier = PsiCarrier(
                kind="metric.lens_analyzed",
                tag="metric",
                payload={
                    "target": rid,
                    "metrics": analysis["metrics"]
                }
            )
            
            await bus.publish(PsiEvent(
                event_id=f"lens-{uuid.uuid4().hex[:4]}",
                parent_id=event.event_id,  # 원인: metric.observed의 event_id
                source_id=self._id,
                scope="analysis",
                tick=event.tick,
                carrier=carrier
            ))

async def main():
    field = ToposField()
    bus = AsyncEventBus()
    bus.bind_field(field)

    # Field도 ator처럼 등록 (event 소비)
    class FieldAdapter(IPhaseAtor):
        def __init__(self, field): self._field = field
        @property
        def ator_id(self): return "field.adapter"
        @property
        def state(self): return {}
        async def react(self, event, f, bus):
            await self._field.react(event)

    bus.subscribe(KineticLensProjector("perception.lens"))
    bus.subscribe(ScaleEmitter("decision.praxis"))
    bus.subscribe(FieldAdapter(field))

    print("\n## [START] Event-Closed Simulation ##")
    for tick in range(1, 51):
        val = 20 + math.pow(tick, 1.15) + random.uniform(-1, 1)
        field.evolve(1.0)
        tension = field.get_state()["worker-1"]["tension"]

        print(f"[Tick {tick:02d}] value={val:.2f} | tension={tension:.3f}")
        carrier = PsiCarrier(
            kind="metric.observed",
            tag="metrics",
            payload={"value": val}
        )
        
        await bus.publish(PsiEvent(
            event_id=f"m-{tick}",
            parent_id=None,           # 시뮬레이터 자체의 발생이므로 Parent 없음
            source_id="worker-1",
            scope="sensor",
            tick=tick,
            carrier=carrier
        ))

        await asyncio.sleep(0.3)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP]")