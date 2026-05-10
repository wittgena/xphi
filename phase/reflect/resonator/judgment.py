# phase.reflect.resonator.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from phase.flow import ProtoFlow, FlowState, Transduction, Resonance, Judgment, Align
from arch.contract.registry import contract
from topos.bound.plane.emitter import get_logger

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")