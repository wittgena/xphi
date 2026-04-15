# receptor.lens.proactor
import math
import uuid
from typing import Dict, Any, List, Optional
from bound.interface import IPhaseAtor, PsiEvent
from bound.emitter import get_emitter

class ScaleProactor(IPhaseAtor):
    """Decision Ator (event emission only)"""
    def __init__(self, ator_id: str):
        self._id = ator_id
        self.log = get_emitter(f"ator.{ator_id}", phase="PRAXIS")

    @property
    def ator_id(self): return self._id
    @property
    def state(self): return {}

    async def react(self, event: PsiEvent, field, bus):
        if event.event_type != "metric.lens_analyzed":
            return

        m = event.payload["metrics"]
        rid = event.payload["target"]
        if m.get("trend", 0) > 0.4 and m.get("acceleration", 0) > 0.05:
            self.log.warn(f"[ACT] Proactive scaling for {rid}")
            await bus.publish(PsiEvent(
                event_id=f"cmd-{uuid.uuid4().hex[:4]}",
                event_type="action.scale.apply",
                source_id=self._id,
                payload={"target": rid, "delta": 1},
                tick=event.tick
            ))