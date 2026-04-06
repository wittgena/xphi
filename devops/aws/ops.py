# devops.aws.ops
"""
@desc: Topos-aligned Theoria Observation Loop (AWS)

@flow:
Ψ (external signal)
 → Adapter (Ψ → PsiEvent)
 → EventBus
 → ator.react(Ψ, Φ)  # pure evaluation
 → emit Ψ'
 → Fieldator consumes Ψ' → Φ update
 → Watcher (∂Φ) optional
"""
import asyncio
import time
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from bridge.interface.bus import AsyncEventBus
from bridge.interface.topos import (
    IPhaseAtor,
    IPhaseField,
    IEventBus,
    PsiEvent
)
from bound.metrics.aws import AWSMetricsSnapshot
from ext.projector.aws import AWSProjector
from bound.emitter import get_emitter

class AWSEventAdapter:
    """
    @role: Ψ → structured representation
    """

    def to_event(self, raw) -> PsiEvent:
        return PsiEvent(
            event_id=raw.id,
            parent_id=None,
            event_type="AWS_METRIC",
            source_id=raw.resource_id,
            scope="LOCAL",
            payload=raw.payload,
            tick=int(time.time())
        )

    def to_snapshot(self, event: PsiEvent) -> AWSMetricsSnapshot:
        payload = event.payload
        return AWSMetricsSnapshot(
            resource_id=event.source_id,
            timestamp=event.tick,
            tags=payload.get("tags", {}),
            desired_capacity=payload.get("desired_capacity", 0),
            running_instances=payload.get("running_instances", 0),
            failed_health_checks=payload.get("failed_health_checks", 0),
            scp_allows_scaling=payload.get("scp_allows_scaling", True)
        )

class AWSField(IPhaseField):
    """Φ: AWS state field (read-only for ators)"""
    def __init__(self):
        self.nodes_state: Dict[str, Dict[str, Any]] = {}

    def get_state(self) -> Dict[str, Any]:
        return self.nodes_state

    def compute_gradient(self) -> Dict[str, float]:
        return {
            rid: data.get("tension", 0.0)
            for rid, data in self.nodes_state.items()
        }

    def evolve(self, dt: float) -> None:
        for node in self.nodes_state.values():
            node["tension"] *= 0.98


class AWSFieldProjector(IPhaseAtor):
    """@role: Ψ → Φ projection - Field mutation은 반드시 ator를 통해서만 발생"""

    def __init__(self, ator_id="field.projector"):
        self._id = ator_id
        self._state = {}

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        if event.event_type != "AWS_METRIC":
            return

        payload = event.payload
        rid = event.source_id

        field.get_state()[rid] = {
            "tension": payload.get("failed_health_checks", 0)
                       / (payload.get("desired_capacity", 1) + 1),
            "state": "LOCKED" if not payload.get("scp_allows_scaling", True) else "STABLE",
            "updated_at": event.tick
        }

class AWSEvaluator(IPhaseAtor):
    """
    Φ′: pure evaluator
    - no field mutation
    - no control semantics
    - only emits ψ
    """

    def __init__(self, ator_id: str, baseline: Dict[str, str]):
        self._id = ator_id
        self._state = {"mode": "observe"}
        self.projector = AWSProjector(baseline)
        self.adapter = AWSEventAdapter()
        self.log = get_emitter(f"ator.{ator_id}", phase="THEORIA")

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        if event.event_type != "AWS_METRIC":
            return

        snapshot = self.adapter.to_snapshot(event)
        is_coherent = self.projector.project_and_eval(snapshot)
        if not is_coherent:
            self.log.warn(f"drift detected: {event.source_id}")

            await bus.publish(PsiEvent(
                event_id=f"scale-{event.event_id}",
                parent_id=event.event_id,
                event_type="AWS_SCALE_REQUEST",
                source_id=self._id,
                scope="LOCAL",
                payload={"target": event.source_id},
                tick=event.tick
            ))
        else:
            self.log.info(f"Φ⁺ stable: {event.source_id}")

class AWSObserver:
    """Topos-aligned observer system"""

    def __init__(self, baseline: Dict[str, str]):
        self.redis = None
        self.adapter = AWSEventAdapter()
        self.field = AWSField()
        self.bus = AsyncEventBus()
        self.evaluator = AWSEvaluator("aws.theoria", baseline)
        self.projector = AWSFieldProjector()

        self.log = get_emitter("system.aws", phase="OBSERVER")

    async def setup(self):
        self.redis = await redis_async.from_url("redis://localhost:6379")

        self.bus.bind_field(self.field)

        ## subscribe ators
        self.bus.subscribe(self.projector)
        self.bus.subscribe(self.evaluator)

        self.log.info("Topos Observer initialized")

    async def ingest(self, raw_psi):
        """Ψ → EventBus"""
        event = self.adapter.to_event(raw_psi)
        await self.bus.publish(event)

    async def run(self):
        while True:
            try:
                signals = await self._sense()
                for psi in signals:
                    await self.ingest(psi)

                self.field.evolve(1.0)
                await asyncio.sleep(1.0)

            except Exception as e:
                self.log.error(f"loop error: {e}")
                await asyncio.sleep(2)

    async def _sense(self):
        # placeholder (redis → ψ)
        return []