# ator.judgment
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from flow.ator import AtorFlow, FlowState, Transduction, Resonance, Judgment, Align
from manifold.contract.registry import ator_contract
from anchor.log import get_logger

@ator_contract("contract.judgment")
class ContractJudgment(Judgment):
    def judge(self, flow, rules):
        status = flow.payload.get("status")
        return rules.get(status, "UGA")