# ator.resonance
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from flow.ator import AtorFlow, FlowState, Transduction, Resonance, Judgment, Align
from contract.registry import ator_contract
from anchor.log import get_logger

log = get_logger('ator.resonance')

@ator_contract("resonance.feedback")
class ResonanceFeedback(Resonance):
    def interfere(self, gen, crit):
        """Ψ_gen ⊕ Ψ_crit → Δ → decision"""
        if "fail" in str(crit).lower():
            return {
                "status": "retry",
                "payload": gen
            }
        return {
            "status": "ok",
            "payload": f"[RESOLVED]\nGEN={gen}\nCRIT={crit}"
        }

@ator_contract("security.resonance")
class SecurityResonance(Resonance):
    def interfere(self, gen_payload, crit_payload):
        score = crit_payload.get("security_score", 0)
        vulnerabilities = crit_payload.get("vulnerabilities", [])
        if score < 80:
            return {"status": "retry", "payload": {"feedback": f"Score {score}. Fix {vulnerabilities}"}}
        return {"status": "stable", "payload": {"final_artifact": gen_payload.get("code")}}
