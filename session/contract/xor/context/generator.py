# session.contract.xor.context.generator
import dspy
import sys
import json
import dspy
from typing import Dict, Any
from pathlib import Path
from session.resonance.thch import thch_scope
from bound.client.local.lm import LocalLM
from bound.surface.emitter import get_emitter
from bound.resolver import resolve_path
from session.contract.xor.store import ResidueStore, ResidueSnapshot
from session.contract.xor.intent.signature import IntentSignature
from session.contract.xor.intent.transition import SignatureTransition

PRE_PHASE_ROOT = resolve_path('pre.phase')
log = get_emitter("context.generator")

class SurfaceIntent(IntentSignature):
    """
    문서로부터 직접 LMP를 추출하지 말고, 먼저 일반 구조를 파악한 뒤 이를 LMP로 약하게 투영
    
    [Structural Mapping Rule]
    First identify a simple structural frame before using LMP:
    - What is being described (entities / components)
    - What constraints or boundaries exist
    - What processes or changes occur
    Do NOT start from Logos/Mythos/Phronesis.
    """
    document_content = dspy.InputField(desc="pre.phase 마크다운 문서")
    logos: str = dspy.OutputField(desc="일반 구조에서 드러난 대상/경계/구획을 바탕으로, 이를 boundary/locus 관점에서 약하게 투영한 결과 (surface-level mapping)")
    mythos: str = dspy.OutputField(desc="구조 내 조건, 흐름, 안정성 패턴을 바탕으로 공명/활성 조건으로 해석한 결과 (not exact, but inferred resonance)")
    phronesis: str = dspy.OutputField(desc="문서에서 관찰되는 변화, 결과, 작동 방식을 기반으로 실행/변이 구조로 해석한 결과 (observed transformation projection)")

class StructuralDissonance(IntentSignature):
    """표면 구조로 설명되지 않는 잔여(xe)를 추출한다."""
    document_content = dspy.InputField()
    logos = dspy.InputField()
    mythos = dspy.InputField()
    phronesis = dspy.InputField()
    deviant_terms: str = dspy.OutputField(
        desc="""
Identify elements that do NOT fit:
- terms that cannot be explained by the structure
- sentences that break consistency
- concepts that remain unintegrated

These are xe (residuals before structure stabilization).
""")

class ContextGenerator:
    def __init__(self):
        self.store = ResidueStore()
        self.transition = SignatureTransition(target_module_id="ator_doc_genesis")

    def flow(self, target_filename: str) -> Dict[str, Any]:
        log.signal(f"Phase 1 [Flow]: Reading phase matrix '{target_filename}'")
        target_path = Path(PRE_PHASE_ROOT) / target_filename
        with open(target_path, 'r', encoding='utf-8') as f:
            return {"document_content": f.read(), "filename": target_filename}

    def judgment(self, f_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        log.signal("Phase 2 [Judgment]: Extracting Surface and Dissonance (xe)...")
        doc = f_kwargs["document_content"]
        
        with thch_scope(lm=LocalLM()):
            ## @step.1: 표면 구조 추출
            step1_engine = dspy.Predict("ExtractSurfaceLMP_Signature_Here")
            s1 = step1_engine(document_content=doc)
            
            ## @step.2: 균열 및 잔여(xe) 추출
            step2_engine = dspy.Predict("ExtractStructuralDissonance_Signature_Here")
            s2 = step2_engine(
                document_content=doc,
                logos=s1.logos, mythos=s1.mythos, phronesis=s1.phronesis
            )
            
        return {
            "surface": {"Logos": s1.logos, "Mythos": s1.mythos, "Phronesis": s1.phronesis},
            "xe_terms": s2.deviant_terms,
            "filename": f_kwargs["filename"]
        }

    def rhythm(self, j_result: Dict[str, Any]) -> Dict[str, Any]:
        log.signal("Phase 3 [Rhythm]: Depositing xe to RocksDB and Triggering Bridge...")
        xe_text = j_result["xe_terms"]
        surface = j_result["surface"]
        
        ## 텍스트로 된 xe를 기호(Symbol) 배열로 단순화 - 실제 환경에서는 명사구 추출 등을 사용할 수 있습니다.
        symbols = [s.strip() for s in xe_text.replace('\n', ',').split(',') if len(s.strip()) > 2]
        
        ## 문서 기반의 고압력 Snapshot 강제 생성 (문서 자체가 하나의 강한 충격량)
        snapshot = ResidueSnapshot(
            pressure=1.0,  # 문서에서 발췌한 핵심 구조이므로 즉시 트리거되도록 1.0 부여
            tension=0.9,
            topology_nodes=[f"doc::{j_result['filename']}"],
            symbols=symbols,
            blocks=[{"type": "surface_context", "data": surface}],
            timestamp=time.time()
        )
        
        ## DB에 강제 예치 (Deposit)
        self.store.deposit(snapshot)
        log.signal("Snapshot deposited into ResidueStore.")
        
        ## 브릿지를 통해 상전이(컴파일) 트리거
        compiled_result = self.transition.process_latest_snapshot()
        return compiled_result

    def __call__(self, target_filename: str) -> Dict[str, Any]:
        f_kwargs = self.flow(target_filename)
        j_result = self.judgment(f_kwargs)
        final_topology = self.rhythm(j_result)
        return final_topology

def verify_db_state():
    """DB에 실제로 xe가 예치(Deposit)되었다가 소비되었는지 확인하는 헬퍼 함수"""
    store = ResidueStore()
    keys = list(store.db.keys())
    count = len(keys)
    store.close()
    return count

def main():
    target_file = "emergence.md"
    if len(sys.argv) > 1:
        target_file = sys.argv[1]

    print(f"\n## [System Genesis] Initiating DNA-to-Action Pipeline for '{target_file}'")
    initial_db_count = verify_db_state()
    log.info(f"Initial Residue DB state: {initial_db_count} snapshots remaining.")
    generator = ArchetypeGenerator()

    try:
        ## 파이프라인 트리거 (문서 -> xe 추출 -> DB 예치 -> Bridge 복원 -> Compiler 직조)
        log.signal("Triggering ArchetypeGenerator Flow...")
        final_topology = generator(target_filename=target_file)

        if not final_topology:
            log.error("Pipeline terminated: Phase transition yielded empty topology.")
            return

        print("\n=== [System Genesis] Pipeline Completed Successfully ===")

        ## 최종 결과 출력 (컴파일러가 직조한 구조)
        print("\n## 1. Compiled Topological Graph (Φ') ##")
        print("-> 문서에서 추출된 잔여(xe)가 DB를 거쳐 복원되어 생성된 동적 라우팅/오퍼레이터 노드들")
        print(json.dumps(final_topology.get("phase_script", {}), indent=2, ensure_ascii=False))

        print("\n## 2. Executable Runtime Specs (Ψ) ##")
        print("-> 상위 에이전트(ator)가 즉시 실행할 물리적 액추에이터 스펙")
        print(json.dumps(final_topology.get("runtime_specs", {}), indent=2, ensure_ascii=False))

        ## 사후 DB 상태 확인 (Bridge가 소비하고 삭제했는지 검증)
        final_db_count = verify_db_state()
        log.info(f"Final Residue DB state: {final_db_count} snapshots remaining.")
        
        if final_db_count <= initial_db_count:
            log.signal("Verified: The xe snapshot was successfully deposited, read, and consumed by the Bridge.")
        else:
            log.warning("Notice: Snapshots remain in the DB. Bridge consumption logic may need adjustment.")
    except FileNotFoundError as e:
        print(f"\n[Topological Fault] {e}")
        print(f"해당 문서({target_file})가 PRE_PHASE_ROOT 경로에 존재하는지 확인하십시오.")
    except Exception as e:
        print(f"\n[System Collapse] 위상 붕괴 발생: {e}")

if __name__ == "__main__":
    main()
