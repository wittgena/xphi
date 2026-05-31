# nexus.hub.manager
## @lineage: iso.gov.manager
## @lineage: gov.manager
from __future__ import annotations
import time
from decimal import Decimal
from typing import List, Optional, Dict, Any
from arch.code.exp.promise import future, Promise
from pydantic import BaseModel
import logging
from nexus.hub.policy.budget import Psi, Residue, ShiftDecision, TokenContext, TokenVerdict, AnyTokenPolicy
from nexus.hub.policy.iam import SystemicRisk, ActionDirective, PolicyVerdict, AnySystemPolicy, RiskSeverity
from watcher.plane.emitter import get_emitter

log = get_emitter("hub.manager")

## ---------------------------------------------------------------------------
## Promises (계약)
## ---------------------------------------------------------------------------
elasticity_promise = Promise(
    contract="에이전트의 토큰 소모는 max_elasticity를 초과할 수 없다.",
    invariant="current_tension <= max_elasticity",
    consequence="연쇄적 Rate Limit 도달 및 API 과금 폭탄",
)

## ---------------------------------------------------------------------------
## 1. Unified Governance Layer (텔레메트리 중앙집중화)
## ---------------------------------------------------------------------------
class GovernanceLayer:
    """IAM 보안 이벤트와 Token 재무 이벤트를 모두 수집하는 중앙 관측소"""
    def __init__(self):
        self.shift_history: List[Dict[str, Any]] = []
        self.rupture_history: List[Residue] = []
        self.security_blocks: List[Dict[str, Any]] = []  # IAM 차단 기록 추가
        self.state_observations: List[TokenContext] = [] 

    @future("Stream TokenContext via Kafka/gRPC to ClickHouse for real-time drift detection.")
    def observe_state(self, ctx: TokenContext) -> None:
        self.state_observations.append(ctx)

    def record_shift(self, ctx: TokenContext, verdict: TokenVerdict) -> None:
        self.shift_history.append({"ts": time.time(), "decision": verdict.decision.value, "rationale": verdict.rationale})

    @future("Background DSPy optimizer fetches Ruptures to compile cheaper counterfactual prompts.")
    def record_rupture(self, residue: Residue) -> None:
        self.rupture_history.append(residue)

    def record_security_block(self, risk: SystemicRisk, verdict: PolicyVerdict) -> None:
        log.warning(f"[Security Block] Source: {risk.source} | Rationale: {verdict.rationale}")
        self.security_blocks.append({"ts": time.time(), "risk": risk.model_dump(), "verdict": verdict.model_dump()})


## ---------------------------------------------------------------------------
## 2. Integrated Nexus Manager (The Gateway)
## ---------------------------------------------------------------------------
class AgentAction(BaseModel):
    """에이전트가 시스템에 가하려는 행동의 총체적 메타데이터"""
    risk: SystemicRisk
    psi: Psi

class HubManager:
    """IAM 정책과 Budget 정책을 직렬로 연결하는 최상위 게이트웨이"""
    def __init__(
        self, 
        name: str, 
        base_threshold: Decimal, 
        max_elasticity: Decimal,
        iam_policy: AnySystemPolicy,      # 1차 관문 정책
        budget_policy: AnyTokenPolicy,    # 2차 관문 정책
        governance: GovernanceLayer
    ):
        self.name = name
        self.base_threshold = base_threshold
        self.current_threshold = base_threshold
        self.max_elasticity = max_elasticity   
        self.current_tension = Decimal("0")    
        
        self.iam_policy = iam_policy
        self.budget_policy = budget_policy
        self.governance = governance

    def execute(self, action: AgentAction) -> Optional[Residue]:
        """행동 실행의 진입점: IAM 검증 -> Budget 검증 순으로 파이프라인 진행"""
        log.info(f"\n[{self.name}] ⚡ 행동 요청 접수: '{action.psi.name}'")

        # ----------------------------------------------------
        # Gate 1: IAM Policy (보안/사법 심사)
        # ----------------------------------------------------
        iam_verdict = self.iam_policy.evaluate(action.risk)
        
        if iam_verdict.directive == ActionDirective.QUARANTINE:
            self.governance.record_security_block(action.risk, iam_verdict)
            return self._collapse(action.psi, f"IAM Blocked: {iam_verdict.rationale}")

        if iam_verdict.directive == ActionDirective.REQUIRE_PROOF:
            # @future: 증명 요구 로직 호출 (현재는 차단으로 Fallback)
            return self._invoke_tribunal_verification(action, iam_verdict)

        log.info(" └─ [IAM 승인] 보안 검증 통과 (PROCEED)")

        # ----------------------------------------------------
        # Gate 2: Budget Policy (재무/한도 심사)
        # ----------------------------------------------------
        prospective_tension = self.current_tension + action.psi.amount
        
        ## Promise 가드레일
        if prospective_tension > self.max_elasticity:
            return self._collapse(action.psi, "Violated: elasticity_promise")

        ctx = TokenContext(
            current_tension=prospective_tension,
            current_threshold=self.current_threshold,
            max_elasticity=self.max_elasticity,
            psi=action.psi,
            history_summary=self._summarize_history()
        )
        self.governance.observe_state(ctx) 
        budget_verdict = self.budget_policy.evaluate(ctx)  

        return self._apply_budget(budget_verdict, ctx, action.psi)

    def _apply_budget(self, verdict: TokenVerdict, ctx: TokenContext, psi: Psi) -> Optional[Residue]:
        if verdict.decision == ShiftDecision.ABSORB:
            self.current_tension += psi.amount
            log.info(f" └─ [Budget 승인] ABSORB. 누적 소모={self.current_tension} IC")
            return None

        if verdict.decision in (ShiftDecision.EXPAND_SOFT, ShiftDecision.EXPAND_HARD):
            self.current_threshold = verdict.new_threshold
            self.current_tension += psi.amount
            self.governance.record_shift(ctx, verdict)
            log.info(f" └─ [Budget 승인] EXPAND. 누적 소모={self.current_tension} IC")
            return None

        if verdict.decision == ShiftDecision.STEP_UP_VERIFY:
            return self._invoke_tribunal_verification(AgentAction(risk=SystemicRisk(source="budget"), psi=psi), None)

        return self._collapse(psi, verdict.rationale)

    @future(
        "Zero-Knowledge & Consensus Tribunal: Handles both IAM 'REQUIRE_PROOF' and "
        "Budget 'STEP_UP_VERIFY' by routing the request to a peer DAO or asking for human cryptographic signature."
    )
    def _invoke_tribunal_verification(self, action: AgentAction, iam_verdict: Optional[PolicyVerdict]) -> Optional[Residue]:
        return self._collapse(action.psi, "Tribunal Consensus Not Yet Crystallized")

    def _collapse(self, psi: Psi, reason: str) -> Residue:
        residue = Residue(source_bound=self.name, requested_tokens=psi.amount, declined_amount=psi.amount, reason=reason)
        self.governance.record_rupture(residue)
        return residue

    def _summarize_history(self) -> Dict[str, Any]:
        return {
            "recent_shifts": len(self.governance.shift_history),
            "recent_ruptures": len(self.governance.rupture_history),
            "recent_security_blocks": len(self.governance.security_blocks)
        }