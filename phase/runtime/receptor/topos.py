# phase.runtime.receptor.topos
import time
import json
from typing import Optional, Dict, List
from contextlib import suppress

from watcher.plane.sink import EmitterSink
from watcher.tracer.trajectory import TopologicalStructure
from watcher.plane.emitter import get_emitter

log = get_emitter("receptor.topos")

class ReceptorTopos:
    """@role: ∂Φ bound surface (Domain Layer)"""
    def __init__(self, sink: EmitterSink):
        self.sink = sink
        self.state_key = "meta.self:state:current_phase"
        self.signal_channel = "meta.self:signals:phase_mutation"
        self.psi_channel = "meta.self:signals:psi"

    async def get_current_phase(self) -> str:
        val = await self.sink.get_control_flag(self.state_key)
        return val or "Φ0"

    async def set_phase(self, phase: str):
        await self.sink.set(self.state_key, phase)

    async def emit_psi(self, event_type: str, weight: int = 1, payload: Optional[Dict] = None):
        merged_payload = payload.copy() if payload else {}
        merged_payload.update({
            "event": event_type,
            "weight": weight,
            "ts": time.time()
        })
        log.info(f"Ψ emit → {merged_payload}")
        await self.sink.publish(self.psi_channel, json.dumps(merged_payload))

def build_system_topos() -> List[TopologicalStructure]:
    structures = []
    core = []
    with suppress(ImportError): import phase.bind.resolver as m; core.append(m.__name__)
    with suppress(ImportError): import phase.runtime.receptor.bootstrap as m; core.append(m.__name__)
    with suppress(ImportError): import phase.runtime.receptor.kernel as m; core.append(m.__name__)

    if core:
        structures.append(TopologicalStructure(name="core.runtime", members=core))

    tracer = []
    with suppress(ImportError): import watcher.tracer.kernel as m; tracer.append(m.__name__)
    with suppress(ImportError): import watcher.tracer.source as m; tracer.append(m.__name__)
    with suppress(ImportError): import watcher.tracer.trajectory as m; tracer.append(m.__name__)

    if tracer:
        structures.append(TopologicalStructure(name="tracer.grid", members=tracer))

    return structures