# xphi.watcher.plane.metric.observer
## @lineage: watcher.plane.metric.observer
import asyncio
import math
import datetime
import time
import uuid
from typing import Dict, Any

from xphi.arch.event.bus import AsyncEventBus
from xphi.arch.event.psi import PsiEvent
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField
from xphi.watcher.plane.metric.trajectory import (
    Point, ContinuousTrajectory, DefaultBoundLensStrategy,
    SlidingWindowStrategy, WindowProjector
)
from xphi.watcher.plane.emitter import get_emitter

class ToposField(IPhaseField):
    """@desc: Event-driven state evolution (Source of Truth)"""
    def __init__(self):
        self.state = {
            "worker-1": {"tension": 0.1, "replicas": 2, "last_update": time.time()}
        }
        self.log = get_emitter("field.topos", phase="FIELD")

    def get_state(self) -> Dict[str, Any]: return self.state
    
    def evolve(self, dt: float):
        for rid in self.state: self.state[rid]["tension"] *= 0.95

    def compute_gradient(self): return {}

    async def react(self, event: PsiEvent):
        # [Fix] "apply"(의도)가 아닌 "applied"(결과)를 수신해야 함
        if event.event_type == "action.scale.applied":
            rid = event.payload["target"]
            actual_replicas = event.payload["replicas"]
            
            if rid not in self.state:
                self.state[rid] = {"tension": 0.0, "replicas": 0, "last_update": time.time()}

            # [Fix] delta 누적이 아닌 절대값(replicas) 덮어쓰기로 정렬
            self.state[rid]["replicas"] = actual_replicas
            self.state[rid]["tension"] = 0.0  # 팽창 완료 후 텐션 즉각 해소
            self.state[rid]["last_update"] = time.time()
            
            self.log.info(f"[FIELD] {rid} phase confirmed → replicas={actual_replicas}")

class KineticLensProjector(IPhaseAtor):
    """@desc: 원시 메트릭을 수집하여 가공 및 분석 신호를 방출"""
    def __init__(self, ator_id: str):
        self._id = ator_id
        self.lens = DefaultBoundLensStrategy("kinematic")
        self.projector = WindowProjector(SlidingWindowStrategy(window_days=1, step_days=0.1))
        self.trajectories: Dict[str, ContinuousTrajectory] = {}
        self.max_points = 100
        self.log = get_emitter(f"ator.{ator_id}", phase="PERCEPTION")

    @property
    def ator_id(self): return self._id
    @property
    def state(self): return {}

    async def react(self, event: PsiEvent, field, bus):
        if event.event_type != "metric.observed": return

        rid = event.source_id
        point = Point(timestamp=datetime.datetime.now(), value=event.payload["value"])
        traj = self.trajectories.setdefault(rid, ContinuousTrajectory(identity=rid, points=[]))
        traj.points.append(point)

        if len(traj.points) > self.max_points: traj.points = traj.points[-self.max_points:]

        windows = self.projector.project(traj)
        if not windows: return

        analysis = self.lens.scan(windows[-1])
        if analysis["status"] == "valid":
            # [Fix] ScaleProactor가 명확히 수신할 수 있도록 event_type 세팅 및 payload 분리
            await bus.publish(PsiEvent(
                event_id=f"lens-{uuid.uuid4().hex[:4]}",
                event_type="metric.lens_analyzed",  # Event Type 명시
                parent_id=event.event_id,
                source_id=self._id,
                scope="analysis",
                tick=event.tick,
                payload={"target": rid, "metrics": analysis["metrics"]}
            ))