# xphi.watcher.plane.metric.scale.emitter
## @lineage: watcher.plane.metric.scale.emitter
import asyncio
from abc import ABC, abstractmethod
import math
import uuid
from typing import Dict, Any, List, Optional

from xphi.arch.event.psi import PsiEvent, PsiCarrier
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField
from xphi.arch.event.bus import AsyncEventBus
from xphi.arch.contract.registry.unified import contract
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scale.emitter")

class IScaleAdapter(ABC):
    """@desc: 인프라(K8s, AWS, Docker 등)에 의존하지 않는 범용 스케일링 인터페이스"""
    @abstractmethod
    async def initialize(self) -> None:
        pass
        
    @abstractmethod
    async def apply_scale(self, target_resource: str, replicas: int) -> bool:
        pass


@contract.ator("scale.emitter")
class ScaleEmitter(IPhaseAtor):
    """@desc: 제어 시그널(Ψ')을 해석하여 인프라 밀도를 변조하고, 결과를 상태장(Field)에 피드백하는 액추에이터"""
    def __init__(self, ator_id: str = "runtime.morpher", adapter: Optional[IScaleAdapter] = None, **kwargs):
        self._id = ator_id
        self._state = "IDLE"
        self._initialized = False
        self.adapter = adapter 
        self.phase_map = kwargs.get("phase_map", {
            "Φ0": 3,  # 기본 팽창
            "∂Φ": 1,  # 잉여 수축
            "Φ4": 0   # 감각/방어 수축
        })

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def _ensure_initialized(self):
        if self._initialized or not self.adapter: return
        await self.adapter.initialize()
        self._initialized = True
        log.info(f"[Φ(t)] Scale Adapter initialized for Projector ({self._id}).")

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        # [Fix] Carrier 검증 강화
        if not event.carrier or event.carrier.kind not in ("AWS_SCALE_REQUEST", "ACTION_SCALE"):
            return

        carrier = event.carrier
        target_resource = carrier.tag
        target_phase = carrier.payload

        log.info(f"[Φ(t) Modulation] Signal {event.event_id} routing '{target_resource}' to Phase '{target_phase}'")
        replicas = self.phase_map.get(target_phase)
        
        if replicas is None or not self.adapter:
            log.error(f"[Actuation Error] Missing Phase Map or Adapter for {self._id}")
            return

        await self._ensure_initialized()
        
        # 1. 물리적 인프라 제어 (Actuation)
        success = await self.adapter.apply_scale(target_resource, replicas)
        
        # 2. 제어 성공 시 닫힌 피드백 루프(Closed-Loop) 형성
        if success:
            self.set_state(f"PROJECTED_{target_phase}")
            # [Fix] 누락되었던 피드백 이벤트 발행 (ToposField가 이를 수신함)
            await bus.publish(PsiEvent(
                event_id=f"applied-{uuid.uuid4().hex[:4]}",
                event_type="action.scale.applied",  # 확정 시그널
                parent_id=event.event_id,
                source_id=self._id,
                scope="feedback",
                tick=event.tick,
                payload={"target": target_resource, "replicas": replicas, "phase": target_phase}
            ))

class ScaleProactor(IPhaseAtor):
    """@desc: 분석 결과를 바탕으로 물리적 스케일링 위상(Phase) 전환을 결정"""
    def __init__(self, ator_id: str):
        self._id = ator_id
        self.log = get_emitter(f"ator.{ator_id}", phase="PRAXIS")

    @property
    def ator_id(self): return self._id
    @property
    def state(self): return {}

    async def react(self, event: PsiEvent, field, bus):
        # [Fix] 분석 완료 이벤트 수신 대기
        if event.event_type != "metric.lens_analyzed":
            return

        m = event.payload["metrics"]
        rid = event.payload["target"]
        
        if m.get("trend", 0) > 0.4 and m.get("acceleration", 0) > 0.05:
            self.log.warn(f"[ACT] Proactive scaling triggered for {rid} -> Phase: Φ0")
            
            carrier = PsiCarrier(kind="ACTION_SCALE", tag=rid, payload="Φ0")
            
            # [Fix] event_type을 "action.scale.intent"로 명확히 지정하여 발행
            await bus.publish(PsiEvent(
                event_id=f"cmd-{uuid.uuid4().hex[:4]}",
                event_type="action.scale.intent",
                parent_id=event.event_id,
                source_id=self._id,
                scope="actuation",
                tick=event.tick,
                carrier=carrier
            ))