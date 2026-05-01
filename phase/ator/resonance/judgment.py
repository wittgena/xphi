# phase.ator.resonance.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from session.contract.proto.flow import ProtoFlow, FlowState, Transduction, Resonance, Judgment, Align
from session.contract.registry import contract
from meta.flow.surface.emitter import get_logger

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")