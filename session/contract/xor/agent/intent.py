# session.contract.xor.agent.intent
import dspy
import time
import asyncio
from typing import Dict, Any, List
from pathlib import Path
from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent
from llama_index.core.schema import TextNode, Document
from llama_index.core import VectorStoreIndex
from bound.surface.emitter import get_logger
from session.resonance.thch import thch_scope
from session.contract.xor.store import ResidueStore, ResidueSnapshot
from session.contract.xor.intent.transition import IntentTransition
from bound.folding import folding 
from bound.client.local.lm import LocalLM

log = get_logger("context.intent")

class DocumentLoadedEvent(Event):
    document: Document
    filename: str

class IntentExtractedEvent(Event):
    node: TextNode
    surface: Dict[str, Any]
    xe_terms: List[str]

class IntentRAGWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = ResidueStore()
        self.transition = IntentTransition(target_module_id="ator.genesis")

    @step
    async def flow(self, ev: StartEvent) -> DocumentLoadedEvent:
        target_filename = ev.get("target.filename")
        PRE_PHASE_ROOT = ev.get("pre.phase")
        
        log.info(f"Phase 1 [Flow]: Reading '{target_filename}'")
        target_path = Path(PRE_PHASE_ROOT) / target_filename
        
        # 파일 I/O도 논블로킹 처리를 위해 스레드 위임
        def _read_file():
            with open(target_path, 'r', encoding='utf-8') as f:
                return f.read()
                
        content = await asyncio.to_thread(_read_file)
            
        doc = Document(text=content, metadata={"filename": target_filename})
        return DocumentLoadedEvent(document=doc, filename=target_filename)

    @step
    async def judgment(self, ev: DocumentLoadedEvent) -> IntentExtractedEvent:
        log.info("Phase 2 [Judgment]: Extracting via DSPy...")
        content = ev.document.text
        
        # [핵심] DSPy의 동기적 LLM 추론을 내부 함수로 묶어 스레드로 분리
        def _run_dspy_inference():
            with thch_scope(lm=LocalLM()):
                step1_engine = dspy.Predict("ExtractSurfaceLMP_Signature_Here")
                s1 = step1_engine(document_content=content)
                
                step2_engine = dspy.Predict("ExtractStructuralDissonance_Signature_Here")
                s2 = step2_engine(
                    document_content=content,
                    logos=s1.logos, mythos=s1.mythos, phronesis=s1.phronesis
                )
                return s1, s2

        # foldbox가 여기서 타임아웃/에러를 잡아 재진입하더라도 메인 루프는 살아있음
        s1, s2 = await asyncio.to_thread(_run_dspy_inference)

        xe_terms = s2.deviant_terms.replace('\n', ',').split(',')
        symbols = [s.strip() for s in xe_terms if len(s.strip()) > 2]
        
        node = TextNode(
            text=content,
            metadata={
                "filename": ev.filename,
                "logos": s1.logos,
                "mythos": s1.mythos,
                "symbols": ", ".join(symbols)
            }
        )
        
        return IntentExtractedEvent(
            node=node,
            surface={"Logos": s1.logos, "Mythos": s1.mythos, "Phronesis": s1.phronesis},
            xe_terms=symbols
        )

    @step
    async def rhythm_and_index(self, ev: IntentExtractedEvent) -> StopEvent:
        log.info("Phase 3 [Rhythm]: Depositing to RocksDB & LlamaIndex...")
        
        snapshot = ResidueSnapshot(
            pressure=1.0,
            tension=0.9,
            topology_nodes=[f"doc::{ev.node.metadata['filename']}"],
            symbols=ev.xe_terms,
            blocks=[{"type": "surface.context", "data": ev.surface}],
            timestamp=time.time()
        )
        
        # [핵심] DB 쓰기 및 LlamaIndex 임베딩 생성(동기 I/O)을 스레드로 분리
        def _deposit_and_index():
            self.store.deposit(snapshot)
            compiled = self.transition.process_latest_snapshot()
            idx = VectorStoreIndex([ev.node])
            return compiled, idx

        compiled_result, index = await asyncio.to_thread(_deposit_and_index)
        
        result = {
            "transition.topos": compiled_result,
            "llama.index": index,
            "extracted.symbols": ev.xe_terms
        }
        return StopEvent(result=result)

async def main():
    ## 워크플로우 인스턴스 생성
    workflow = IntentRAGWorkflow(timeout=300.0)
    
    ## 런타임에 foldbox 위상막 적용 (LLM 환각이나 타임아웃 발생 시 3회까지 자가 치유)
    with folding(workflow, re_entry_limit=3) as protected_workflow:
        try:
            result = await protected_workflow.run(
                target_filename="architecture_v1.md", 
                pre_phase_root="./data"
            )
            print("\n[Topology Generated]", result["extracted_symbols"])
        except Exception as e:
            log.error(f"[System Halt] 최종 붕괴 발생: {e}")

if __name__ == "__main__":
    asyncio.run(main())