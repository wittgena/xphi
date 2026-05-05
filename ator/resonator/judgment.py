# ator.resonator.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from phase.proto.flow import ProtoFlow, FlowState, Transduction, Resonance, Judgment, Align
from phase.contract.registry import contract
from bound.surface.emitter import get_logger

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")