# xe.intent.generator
import dspy
import sys
import json
import dspy
from typing import Dict, Any
from pathlib import Path
from flow.emitter import get_emitter
from bridge.thch import ThCh, topos_folding_scope
from bridge.client.local.lm import LocalLM
from flow.emitter import get_emitter
from bound.resolver import resolve_path
from xe.generator import SurfaceIntent, StructuralDissonance
from xe.residue.store import ResidueStore, ResidueSnapshot
from xe.intent.signature import Signature
from xe.intent.transition import IntentTransition

PRE_PHASE_ROOT = resolve_path('pre.phase')
log = get_emitter("intent.generator")

class IntentGenerator:
    def __init__(self):
        self.store = ResidueStore()
        self.transition = IntentTransition(target_module_id="ator_doc_genesis")

    def flow(self, target_filename: str) -> Dict[str, Any]:
        log.signal(f"Phase 1 [Flow]: Reading phase matrix '{target_filename}'")
        target_path = Path(PRE_PHASE_ROOT) / target_filename
        with open(target_path, 'r', encoding='utf-8') as f:
            return {"document_content": f.read(), "filename": target_filename}

    def judgment(self, f_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        log.signal("Phase 2 [Judgment]: Extracting Surface and Dissonance (xe)...")
        doc = f_kwargs["document_content"]
        
        with topos_folding_scope(lm=LocalLM()):
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
