# contract.executor.dynamics
from __future__ import annotations
import uuid
import asyncio
import math
import random
from typing import List, Dict, Optional, Any, Type, Callable
from bridge.psi import PsiCarrier, PsiEvent
from system.builder import SystemBuilder
from contract.executor.base import BaseExecutor

class PhaseField(type(BaseExecutor)):
    """@phase.bound: assigns unique ∂Φ to each Xe instance"""
    def __new__(mcs, name, bases, namespace):
        namespace['bound_id'] = f"bound.{uuid.uuid4().hex[:4]}"
        return super().__new__(mcs, name, bases, namespace)

class XeCont(BaseExecutor, metaclass=PhaseField):
    """
    @entity: autonomous phase carrier
    @flow: Ψ → ∂Φ.absorb → τ → {rupture | saturation} → inversion → rebind
    """
    def __init__(self, bound, ex: str = "void", origin: str = "void"):
        super().__init__()
        self.trace_id = f"base.{uuid.uuid4().hex[:6]}"
        self.ex = ex
        self.origin = origin
        self.bound = bound

    async def execute(self, psi: PsiType) -> List[PsiType]:
        ## step.1: Ψ → ∂Φ
        batch_payload = [{"payload": psi.symbol}]
        self.bound.absorb(batch_payload)

        ## step.2: τ evaluation
        decision = self.bound.evaluate()
        if decision == "DEPOSIT":
            snap = self.bound.snapshot()
            self.bound.commit()
            ext_base = self._ext__()
            log.signal(f"[Rupture] {self.trace_id} -> {ext_base.trace_id}")

            self.ex = ext_base.ex
            self.origin = ext_base.trace_id
        else:
            log.info(f"[Saturation] pressure:{self.bound.pressure:.3f} ({self.trace_id})")

        ## step.3: Ψ identity 유지
        return [psi]

    def _ext__(self) -> 'XeCont':
        overflowed_ex = f"overflow.{self.ex}"
        inverted_state = f"inversion.{overflowed_ex}"
        base_state = f"Base.bind({inverted_state})"
        return XeCont(
            bound=self.bound,
            ex=base_state,
            origin=self.trace_id
        )

    def __repr__(self):
        return f"<XeCont {self.bound_id} id:{self.trace_id}, mem='{self.ex}'>"

class LoopCarrier(BaseExecutor):
    """
    @role: self-driven Ψ loop generator
    @flow: Ψₙ → Xe → Ψₙ → emit Ψₙ₊₁ → ...
    """
    def __init__(self, xe: XeCont, max_ticks: int = 100, interval: float = 0.1):
        super().__init__()
        self.xe = xe
        self.tick = 0
        self.max_ticks = max_ticks
        self.interval = interval

    async def execute(self, psi: PsiType) -> List[PsiType]:
        out = []

        ## Xe 처리
        xe_out = await self.xe.execute(psi)
        out.extend(xe_out)

        ## 내부 loop 생성
        if self.tick < self.max_ticks:
            await asyncio.sleep(self.interval)
            next_psi = psi.__class__(
                event_id=f"tick-{self.tick + 1}",
                parent_id=getattr(psi, "event_id", None),
                source_id="loop.carrier",
                scope=getattr(psi, "scope", "GLOBAL"),
                context=getattr(psi, "context", {}),
                tick=self.tick + 1,
                carrier=getattr(psi, "carrier", None)
            )

            out.append(next_psi)
            self.tick += 1
        else:
            log.info("[LoopCarrier] max_ticks reached")

        return out

class DynamicsExecutor(BaseExecutor):
    """@role: WatcherSystem을 RuntimeNode의 Executor 인터페이스에 맞추는 어댑터"""
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__()
        self.config = config_dict
        self.system = None ## 지연 초기화 대상

    async def execute(self, psi: PsiEvent) -> List[PsiEvent]:
        if self.system is None:
            self.system = SystemBuilder.build(self.config)
            
        return await self.system.process_step(psi)