# meta.phase.learn
## @lineage: meta.resolver.phase.learn
## @lineage: meta.debug.phase.learn
import asyncio
from typing import List, Any
from phase.bound.proto.signature import ProtoSignature, In, Out
from foldbox.scope.thch import ThCh
from foldbox.scope.manager import managed_scope
from phase.bound.folding import folding
from phase.plane.emitter import get_logger
from bridge.spi.types.module.meta import Module
from bridge.spi.dsp.predictor.cot import ChainOfThought
from bridge.spi.dsp.exam.prediction import Prediction

log = get_logger("phase.learn")

class ResonanceTranscription(ProtoSignature):
    """[Phase 1: Projection] 비선형적 감응 신호를 선형적 위상 구조(XPHI)로 전사합니다."""
    resonance_signal: str = In(desc="직관적 위화감, 시스템 파열음, 혹은 비정형적 요구사항")
    topology_map: str = Out(desc="노드(Node)와 엣지(Edge)로 구성된 선언적 위상 구조 JSON")
    rationale: str = Out(desc="위상적 정렬을 선택한 구조적 근거")

class TensionEvaluation(ProtoSignature):
    """[Phase 2: Judgment] 생성된 위상 구조 내의 잠재적 텐션(Tension)과 의존성 모순을 평가합니다."""
    topology_map: str = In()
    tension_score: float = Out(desc="0.0(안정) ~ 1.0(파열) 사이의 장력 점수")
    alignment_guide: str = Out(desc="장력 해소를 위한 위상적 재배치 가이드")

class ResonanceAligner:
    def __init__(self, state_path: str = "res/topology_brain_state.json"):
        self.projector = ThCh(ResonanceTranscription, state_path=state_path, state_key="projector")
        self.evaluator = ThCh(TensionEvaluation, state_path=state_path, state_key="evaluator")

    async def forward(self, resonance_signal: str, model_name: str = "local-gemma-3") -> dict:
        with managed_scope(use_spi=True, use_thch=True, spi_model=model_name):
            with folding(self, re_entry_limit=3) as protected_flow:
                return await protected_flow._align_topology(resonance_signal)

    async def _align_topology(self, resonance_signal: str) -> dict:
        projection = self.projector(resonance_signal=resonance_signal)
        evaluation = self.evaluator(topology_map=projection.topology_map)
        tension = float(evaluation.tension_score) if hasattr(evaluation, 'tension_score') else 1.0
        if tension > 0.7:
            raise RuntimeError(f"Tension Spike ({tension})")

        return {
            "topology": projection.topology_map,
            "tension": tension,
            "rationale": projection.rationale
        }

def train_resonance_model(trainset: List[Any]):
    from foldbox.spy.teleprompt import MIPROv2

    class AlignerTrainingFacade(Module):
        def __init__(self):
            super().__init__()
            from foldbox.scope.thch import _compile_to_sign
            self.projector = ChainOfThought(_compile_to_sign(ResonanceTranscription))
            self.evaluator = ChainOfThought(_compile_to_sign(TensionEvaluation))

        def forward(self, resonance_signal):
            proj = self.projector(resonance_signal=resonance_signal)
            eval_res = self.evaluator(topology_map=proj.topology_map)
            
            try:
                tension = float(eval_res.tension_score)
            except:
                tension = 1.0
                
            return Prediction(
                topology=proj.topology_map, 
                tension=tension, 
                rationale=proj.rationale
            )

    def alignment_metric(gold, pred, trace=None):
        return 1.0 - getattr(pred, "tension", 1.0)

    optimizer = MIPROv2(metric=alignment_metric, auto_errors_max_retries=3)
    
    facade = AlignerTrainingFacade()
    
    ## 파사드(Facade) 객체를 학습시킴
    compiled_facade = optimizer.compile(facade, trainset=trainset, num_trials=10)
    
    ## 학습된 파라미터를 저장하고, 나중에 순수 ResonanceAligner(ThCh)가 읽어가도록 구성
    ## compiled_facade.save("compiled_aligner_weights.json")
    return compiled_facade

if __name__ == "__main__":
    signal = "현재 Judger가 기저 계층에 있는데 LocalLM을 직접 소유하여 루프가 고착됨."
    aligner = ResonanceAligner()
    
    async def run():
        result = await aligner.forward(resonance_signal=signal)
        log.info(f"\n[Final Stable Topology]\n{result['topology']}")
        log.info(f"\n[Residual Tension]: {result['tension']}")

    asyncio.run(run())