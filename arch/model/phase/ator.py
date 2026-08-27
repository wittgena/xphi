# xphi.arch.model.phase.ator
## @lineage: arch.model.phase.ator
"""
@module: arch.model.phase.ator
@desc: Phase Flow Ators aligned with the Unified Registry Architecture
"""
import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple
from xphi.watcher.plane.emitter import get_logger
from xphi.arch.model.phase.flow import (
    PhaseFlow, FlowState, Dispersion, Judgment, 
    Transduction, Align, Resonance, Gather
)
from xphi.arch.contract.protocol import Proto, proto
# [개선됨] manifold_node 제거 및 통합 contract 임포트
from xphi.arch.contract.registry.unified import contract, registry

log = get_logger('phase.ator')

class PhaseAtor(ABC):
    @abstractmethod
    async def run(self, flow: PhaseFlow, operator: Any, ctx: FlowState) -> List[Tuple[str, FlowState]]:
        raise NotImplementedError()

# [개선됨] @manifold_node -> @contract.ator 통합 규격 적용
@contract.ator("ator", role="phase_node")
@proto(Proto((PhaseFlow, Transduction, "List[Tuple]"), kind="transduction"))
class TransAtor(PhaseAtor):
    def __init__(self, spec):
        self.role = spec["role"]
        self.next = spec["next"]
        self.node_context = spec.get("context", {})
        self.spec = spec

    async def run(self, flow: PhaseFlow, operator: Transduction, ctx: FlowState):
        log.info(f"    [TransAtor] '{self.role}' initiates self-transmutation")
        injected_state = {k: ctx.state.get(k) for k in self.node_context.get("inject_state", [])}
        flow.payload = {
            "raw_input": flow.payload,
            "instructions": self.node_context.get("instruction"),
            "injected_state": injected_state,
            "hyperparams": {"temperature": self.node_context.get("temperature", 0.7)}
        }

        loop = asyncio.get_running_loop()
        new_flow = await loop.run_in_executor(None, operator.transduce, flow, self)
        ctx.flow = new_flow
        return [(self.next, ctx)]

@contract.ator("aligner", role="phase_node")
@proto(Proto((PhaseFlow, Align, "State"), kind="aligner"))
class AlignAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.spec = spec

    async def run(self, flow: PhaseFlow, operator: Align, ctx: FlowState):
        log.info(f"    [AlignAtor] reconcile ψ:{flow.id}")
        result = operator.align(flow, self.spec)
        ctx.state.update(result.get("state", {}))
        flow.payload = result.get("payload", flow.payload)
        self.next = result.get("next", self.next)
        return [(self.next, ctx)]

@contract.ator("judgment", role="phase_node")
@proto(Proto((PhaseFlow, Judgment, "str"), kind="judgment"))
class JudgmentAtor(PhaseAtor):
    def __init__(self, spec):
        self.rules = spec["rules"]
        op_name = spec.get("operator", "default_judgment")
        
        # [개선됨] 카테고리 인자("ator") 삭제 - 단일 풀에서 자동 탐색
        self.custom_op = registry.create_component({"type": op_name})

    async def run(self, flow: PhaseFlow, operator: Judgment, ctx: FlowState):
        # [버그 수정] 존재하지 않는 base_operator 대신 주입받은 operator 사용
        ator = self.custom_op or operator
        target = ator.dispatch(flow, self.rules)
        log.info(f"    [JudgmentAtor] ψ:{flow.id} → {target}")
        return [(target, ctx)]

@contract.ator("dispersion", role="phase_node")
@proto(Proto((PhaseFlow, Dispersion, "List[ProtoFlow]"), kind="dispersion"))
class DispersionAtor(PhaseAtor):
    def __init__(self, spec):
        self.aspects = spec["aspects"]
        self.next = spec["next"]

    async def run(self, flow: PhaseFlow, operator: Dispersion, ctx: FlowState):
        log.info(f"    [DispersionAtor] scatter ψ:{flow.id}")
        flows = operator.scatter(flow, self.aspects)
        return [(self.next, FlowState(f, ctx.state)) for f in flows]

@contract.ator("gather", role="phase_node")
@proto(Proto((PhaseFlow, Gather, "List[ProtoFlow]"), kind="gather"))
class GatherAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.buffer = {}
        self.expected = spec.get("expected_count", 2)

    async def run(self, flow: PhaseFlow, operator: Gather, ctx: FlowState):
        root = flow.root
        slot = self.buffer.setdefault(root, [])
        slot.append(flow)

        log.info(f"    [GatherAtor] buffer ψ:{flow.id} {len(slot)}/{self.expected}")
        if len(slot) < self.expected:
            return []

        flows = self.buffer.pop(root)
        new_flow = operator.merge(flows, root)
        return [(self.next, FlowState(new_flow, ctx.state))]

@contract.ator("resonance", role="phase_node")
@proto(Proto((PhaseFlow, Resonance, "ProtoFlow"), kind="resonance"))
class ResonanceAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.buffer = {}
        op_name = spec.get("operator", "default_resonance")
        
        # [개선됨] 카테고리 인자("ator") 삭제
        self.custom_op = registry.create_component({"type": op_name})

    async def run(self, flow: PhaseFlow, operator: Resonance, ctx: FlowState):
        root = flow.root
        tag = flow.aspect

        slot = self.buffer.setdefault(root, {})
        slot[tag] = flow.payload
        log.info(f"    [ResonanceAtor] interference update {tag}")
        if "code" not in slot or "logic" not in slot:
            return []

        data = self.buffer.pop(root)
        active_operator = self.custom_op or operator
        payload = active_operator.interfere(data["code"], data["logic"])
        
        new_flow = PhaseFlow(payload=payload, id=root, aspect="resonated", root=root)
        return [(self.next, FlowState(new_flow, ctx.state))]