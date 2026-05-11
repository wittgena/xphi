# xphi.transcript.resonance.judgment
## @lineage: topos.bound.resonator.judgment
## @lineage: phase.reflect.resonator.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from phase.reflect.proto.flow import ProtoFlow, FlowState, Transduction, Resonance, Judgment, Align
from phase.runtime.contract.registry.unified import contract
from topos.bound.plane.emitter import get_logger

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")