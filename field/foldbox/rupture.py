# field.foldbox.rupture
import contextlib
from typing import List
from llama_index.core.schema import NodeWithScore
from llama_index.core.retrievers import BaseRetriever
from llama_index.core.query_engine import RetrieverQueryEngine
from bound.surface.emitter import get_emitter

log = get_emitter("foldbox.rupture")

@contextlib.contextmanager
def foldbox_rupture(query_engine: RetrieverQueryEngine, re_entry_limit: int = 3):
    """정적으로 잠긴 파이프라인의 내부 캡슐(_retriever)을 직접 가로채어 자율적인 파열/재진입을 허용하는 위상적 샌드박스"""
    ## [상태 추출] LlamaIndex의 Setter 차단을 우회하기 위해 은닉 변수(_retriever)에 직접 접근
    original_retriever = query_engine._retriever 
    
    ## 여기서 기존에 작성하신 RuptureAwareRetriever를 인스턴스화 해야 합니다.
    meta_retriever = RuptureAwareRetriever(
        base_retriever=original_retriever, 
        re_entry_limit=re_entry_limit
    )
    
    ## [위상적 덮어쓰기] 심층 상태 주입 (Deep State Injection)
    query_engine._retriever = meta_retriever
    log.info("[Boundary Enter] System intercepted via deep state injection.")
    
    try:
        ## 제어권을 샌드박스 내부로 이양
        yield query_engine
    except Exception as e:
        log.error(f"[Fatal Rupture] Sandbox failed to contain tension: {e}")
        raise
    finally:
        ## [상태 복구] 블록이 끝나면 원래의 순정 상태로 흔적 없이 복원 (상태 오염 방지)
        query_engine._retriever = original_retriever
        log.info("[Boundary Exit] Interception lifted. Original state restored.")

class RuptureAwareRetriever(BaseRetriever):
    """
    기존 LlamaIndex Retriever를 감싸서, 검색 결과의 잔류 장력을 평가하고
    필요시 파열(Rupture)과 재진입(Re-entry)을 강제하는 프록시 클래스
    """
    def __init__(self, base_retriever: BaseRetriever, re_entry_limit: int = 3):
        self._retriever = base_retriever
        self.re_entry_limit = re_entry_limit
        super().__init__()

    def _retrieve(self, query_bundle, **kwargs) -> List[NodeWithScore]:
        re_entries = 0
        
        while re_entries < self.re_entry_limit:
            log.info(f"Attempting retrieval (Re-entry: {re_entries})...")
            ## 내부의 진짜 LlamaIndex Retriever 실행
            nodes = self._retriever._retrieve(query_bundle, **kwargs)
            
            ## [Intercept] 상태 평가 (잔류 장력 측정) - 예: 검색된 노드가 없거나, 최고 유사도가 너무 낮을 때 (0.5 이하)
            if not nodes or nodes[0].score < 0.5:
                re_entries += 1
                log.warning(f"[Rupture] High tension detected. Low relevance scores. Triggering re-entry.")
                
                ## 자율적 개입: 여기서 AI 스웜을 통해 쿼리를 재작성하거나, 내부 _retriever의 top_k 값을 동적으로 수정하는 등 '재합성' 수행
                query_bundle.custom_embedding_strs = [f"Expanded query: {query_bundle.query_str}"]
                continue
            
            ## 정합성 달성 시 코어 시스템으로 반환
            log.info("[Closure] Satisfactory nodes retrieved.")
            return nodes
        raise RuntimeError("System failed to retrieve valid context within re-entry bounds.")
