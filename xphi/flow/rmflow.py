# xphi.flow.rmflow
## @lineage: meta.xphi.rmflow
## @lineage: xphi.rmflow
## @lineage: meta.ops.xphi.rmflow
import argparse
import asyncio
import random
from dataclasses import dataclass
from typing import List, AsyncGenerator, AsyncIterable
from phase.plane.emitter import get_emitter
from arch.contract.registry.unified import contract

log = get_emitter("xphi.rmflow")

@dataclass
class Psi:
    name: str

@dataclass
class Phi:
    name: str

@dataclass
class Bound:
    name: str

class PhiDecl:
    pass

@dataclass
class FlowDecl(PhiDecl):
    from_: Psi
    to: Phi
    bound: Bound

@dataclass
class CollapseDecl(PhiDecl):
    sources: List[Psi]
    bound: Phi

@dataclass
class StreamStepDecl:
    name: str
    body_stream: AsyncIterable[PhiDecl]

@dataclass
class StreamJudgment:
    steps: List[StreamStepDecl]

@dataclass
class Host:
    recognition_strength: float
    methylation_level: float
    damage: float = 0.0

@dataclass
class Phase:
    resistance: float
    replication_rate: float

@dataclass
class InteractionState:
    phase_load: float = 1.0
    time: int = 0

class JudgmentEvent:
    pass

@dataclass
class FlowEvent(JudgmentEvent):
    psi: Psi
    phi: Phi
    boundary: Bound
    phase: str

@dataclass
class CollapseEvent(JudgmentEvent):
    sources: List[Psi]
    surface: Phi
    phase: str

class ExecutionContext:
    def __init__(self):
        self.current_phase: str = "INIT"

    def phase(self, name: str):
        self.current_phase = name

    async def run_stream_step(self, step: StreamStepDecl) -> AsyncGenerator[JudgmentEvent, None]:
        self.phase(step.name)
        async for decl in step.body_stream:
            if isinstance(decl, FlowDecl):
                yield self.emit_flow(psi=decl.from_, phi=decl.to, bound=decl.bound)
            elif isinstance(decl, CollapseDecl):
                yield self.emit_collapse(sources=decl.sources, bound=decl.bound)
            else:
                log.warning(f"[runStep] unknown decl: {decl}")

    def emit_flow(self, psi: Psi, phi: Phi, bound: Bound) -> FlowEvent:
        return FlowEvent(psi=psi, phi=phi, boundary=bound, phase=self.current_phase)

    def emit_collapse(self, sources: List[Psi], bound: Phi) -> CollapseEvent:
        return CollapseEvent(sources=sources, surface=bound, phase=self.current_phase)

class FlowExecutor:
    async def execute(self, judgment: StreamJudgment) -> AsyncGenerator[JudgmentEvent, None]:
        ctx = ExecutionContext()
        for step in judgment.steps:
            async for event in ctx.run_stream_step(step):
                yield event

class RMFlow:
    def __init__(self, host: Host, phase: Phase, state: InteractionState, max_steps: int = 50):
        self.host = host
        self.phase = phase
        self.state = state
        self.max_steps = max_steps

    async def _generate_interaction(self) -> AsyncGenerator[PhiDecl, None]:
        """시뮬레이션 로직을 실시간으로 계산하며 위상(Decl)을 방출합니다."""
        while (
            self.state.time < self.max_steps and
            self.state.phase_load > 0.01 and
            self.host.damage < 1.0
        ):
            self.state.time += 1

            # Recognition pressure & Effective cleavage 계산
            recognition_pressure = self.host.recognition_strength * (1.0 - self.phase.resistance)
            effective_cleavage = recognition_pressure - self.host.methylation_level
            cleavage_probability = max(0.0, min(1.0, effective_cleavage))
            
            if random.random() < cleavage_probability:
                self.state.phase_load *= 0.5
                yield FlowDecl(from_=Psi("cleavage"), to=Phi("phase-reduced"), bound=Bound("host-defense"))
            else:
                self.state.phase_load *= self.phase.replication_rate
                yield FlowDecl(from_=Psi("replication"), to=Phi("phase-expanded"), bound=Bound("viral-dynamics"))

            self.host.damage += self.state.phase_load * 0.01
            await asyncio.sleep(0)

        if self.state.phase_load < 0.01:
            outcome = "restriction-victory"
        elif self.host.damage > 1.0:
            outcome = "lytic-collapse"
        else:
            outcome = "stable-coexistence"

        yield CollapseDecl(sources=[Psi("rm-dynamics")], bound=Phi(outcome))

    def _build_stream_judgment(self) -> StreamJudgment:
        """선언적 구조(Judgment)를 반환하지만, 내부는 실시간 스트림(generator)을 참조합니다."""
        return StreamJudgment(
            steps=[
                StreamStepDecl(
                    name="rm.interaction",
                    body_stream=self._generate_interaction()
                )
            ]
        )

    async def execute(self) -> AsyncGenerator[JudgmentEvent, None]:
        judgment = self._build_stream_judgment()
        async for event in FlowExecutor().execute(judgment):
            yield event

@contract.flow(name="xphi.rmflow", entry="rmflow_entry")
def rmflow_entry(cli_args: list = None, **payload):
    """외부 자극(Psi)의 초기 에너지(payload)를 받아 RMFlow 위상을 인스턴스화"""
    ## Payload가 있으면 사용하고, 없으면 기본값 사용
    rec_str = payload.get("recognition_strength", 0.8)
    rep_rate = payload.get("replication_rate", 1.5)
    steps = payload.get("steps", 50)

    host = Host(recognition_strength=rec_str, methylation_level=0.2)
    phase = Phase(resistance=0.3, replication_rate=rep_rate)
    state = InteractionState()
    return RMFlow(host, phase, state, max_steps=steps)

if __name__ == "__main__":
    from cognitive.flow.executor import dispatch_flow_cli
    dispatch_flow_cli(
        command_name="rmflow", 
        entry_func=rmflow_entry, 
        file_path=__file__
    )