# phi.resonance.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from contract.proto.flow import ProtoFlow, FlowState, Transduction, Resonance, Judgment, Align
from contract.registry import contract
from flow.emitter import get_logger

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")