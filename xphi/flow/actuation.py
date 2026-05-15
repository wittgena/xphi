# xphi.flow.actuation
## @lineage: meta.xphi.actuation
## @lineage: xphi.actuation
## @lineage: foldbox.flow.actuation
## @lineage: foldbox.debug.actuation
import asyncio
from typing import Tuple
from phase.bound.proto.signature import ProtoSignature, In, Out
from foldbox.scope.manager import managed_scope
from foldbox.scope.thch import ThCh
from meta.flow.scan.pulse import PulseScanner, PhaseState
from phase.plane.emitter import get_emitter

log = get_emitter("flow.actuation")

class ResolveToposRupture(ProtoSignature):
    """@phase: oscillating - 진동하는 코드를 받아, 위상적 모순을 파악하고 제어를 역전시켜 장력을 해소"""
    conflicting_lineages: str = In(desc="파일이 왕복하며 진동 중인 두 위상의 경로")
    current_code: str = In()
    topological_inversion_code: str = Out(desc="의존성이 반전되어 텐션이 해소된 새로운 코드")
    new_lineage: str = Out(desc="안착해야 할 새로운 위상 경로 (@lineage)")

class ExpandPhenotypeSurface(ProtoSignature):
    """@phase: mutation - 요구사항을 흡수하여 표현형(인터페이스)을 유연하게 확장"""
    current_code: str = In()
    user_requirement: str = In(desc="사용자로부터 유입된 새로운 텐션(요구사항)")
    expanded_code: str = Out(desc="요구사항이 반영된 코드")

class PhaseDebugger:
    def __init__(self, scanner: PulseScanner, model_name: str = "local-gemma-3"):
        self.scanner = scanner
        self.model_name = model_name
        
        # 💡 [개선 4] 인스턴스 생성 시점에는 DSPy가 로드되지 않음 (프록시 껍질만 생성)
        self.inverter = ThCh(ResolveToposRupture)
        self.expander = ThCh(ExpandPhenotypeSurface)

    async def debug(self, target_file: str, current_code: str, requirement: str = "") -> Tuple[str, str]:
        """Pulse 스캐너의 결과(∇Φ)를 바탕으로 ThCh 프록시의 Actuation 방향 결정"""
        node = self.scanner.registry.get(target_file)
        if not node:
            raise ValueError(f"위상 지도를 찾을 수 없습니다: {target_file}")

        state = node.state
        log.info(f"Target: {target_file} | Phase: {state}")

        # 수렴 상태는 지능(LLM)을 깨울 필요조차 없이 즉시 반환 (에너지 보존)
        if state == PhaseState.CONVERGED:
            log.warning("🔒 이 파일은 Attractor에 수렴했습니다. 기저 엔진의 훼손을 막기 위해 수정을 거부합니다.")
            return current_code, node.lineages[-1]

        def _execute_cognition():
            with managed_scope(use_dspy=True, use_thch=True, dspy_model=self.model_name):
                
                if state == PhaseState.OSCILLATING:
                    log.info("🔴 파열(Rupture) 감지. 기능 구현을 멈추고 [위상 반전]을 시작")
                    result = self.inverter(
                        conflicting_lineages=str(node.lineages[-3:]), 
                        current_code=current_code
                    )
                    log.info(f"-> 텐션 해소 완료. 새로운 안착 경로: {result.new_lineage}")
                    return result.topological_inversion_code, result.new_lineage

                elif state == PhaseState.MUTATING:
                    log.info("🔵 표면 장력 흡수 중. [표현형 확장]을 시작")
                    result = self.expander(
                        current_code=current_code,
                        user_requirement=requirement
                    )
                    log.info("-> 문명의 요구사항이 성공적으로 전사(Transcription)")
                    return result.expanded_code, node.lineages[-1]

        new_code, new_lineage = await asyncio.to_thread(_execute_cognition)
        return new_code, new_lineage

if __name__ == "__main__":
    async def main():
        ## 테스트 환경을 가정한 실행
        scanner = PulseScanner(root_dir=".")
        ## scanner.scan(...)
        debugger = PhaseDebugger(scanner)

        print("\n--- [Scenario 1: auth_middleware 편집 요청] ---")
        try:
            new_auth_code, new_auth_lineage = await debugger.debug(
                target_file="auth_middleware.py",
                current_code="def auth(): ...",
            )
        except ValueError as e:
            print(f"Skipped Scenario 1: {e}")

    asyncio.run(main())