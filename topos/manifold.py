# topos.manifold
## @lineage: cognitive.dynamics.manifold.particle
## @lineage: topos.dynamics.manifold.particle
"""@phase: Tension Accumulation $\rightarrow$ Projection $\rightarrow$ Collapse $\rightarrow$ Re-entry"""
import asyncio
import uuid
import time
import random
import json
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from phase.runtime.contract.event.psi import PsiEvent, PsiCarrier
from topos.bound.plane.emitter import get_emitter

class ToposManifold(type):
    """
    @registry: Track all active topological nodes for phase perturbation
    @flow: tension_rupture → projection → collapse → inversion/re-entry
    """
    MAX_LIMIT = 50
    _semaphore = None
    _instances = [] 

    void_gap = asyncio.Queue()      
    projection_flow = asyncio.Queue()  
    collapse_field = asyncio.Queue()

    psi_queue = asyncio.Queue()

    global_tick = 0

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        ToposManifold._instances.append(instance)
        
        if cls._semaphore is None:
            cls._semaphore = asyncio.Semaphore(cls.MAX_LIMIT)

        async def managed_exist():
            async with cls._semaphore:
                try:
                    await instance.exist()
                finally:
                    if instance in ToposManifold._instances:
                        ToposManifold._instances.remove(instance)

        asyncio.create_task(managed_exist())
        return instance

class Particle(metaclass=ToposManifold):
    """@scale: cellular-unit (위상 잔여물 및 기본 노드)"""
    def __init__(self, phase_name="TOPOS", bridge=None):
        self.trace_id = f"{self.__class__.__name__}.{uuid.uuid4().hex[:4]}"
        self.bridge = bridge
        self.log = get_emitter(self.trace_id, phase=phase_name)
        self.potential = 0.0

    async def shock_reset(self):
        """@phase: phase reset (Defibrillation)"""
        self.potential = 0.0
        self.log.warn(f"[⚡] Shock applied: Phase reset for {self.trace_id}")

    async def emit_external(self, kind: str, tag: str, payload: dict = None, parent_id: str = None) -> PsiEvent:
        """@phase: transduction (Internal -> External)"""
        ToposManifold.global_tick += 1
        carrier = PsiCarrier(kind=kind, tag=tag, payload=payload or {"timestamp": time.time()})
        event = PsiEvent(
            event_id=f"evt-{uuid.uuid4().hex[:6]}",
            parent_id=parent_id,
            source_id=self.trace_id,
            scope="GLOBAL",
            tick=ToposManifold.global_tick,
            carrier=carrier,
            context={"domain": "toposbeat", "phase": "rhythm"}
        )
        if self.bridge:
            await self.bridge.emit(event)
        return event

    async def exist(self): pass