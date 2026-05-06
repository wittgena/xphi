# meta.flow.work.rag.search
import sys
import argparse
import asyncio
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core.schema import NodeWithScore
from meta.flow.xphi.xor import Xor, SearchResult 
from bound.surface.emitter import get_logger
from bound.resolver import resolve_path

log = get_logger("rag.search")

try:
    METADATA_ROOT = resolve_path("io") / "metadata"
    MANIFEST_FILE = METADATA_ROOT / "llama_manifest.json"
except Exception as e:
    log.error(f"[error] 경로 설정 실패: {e}")
    sys.exit(1)

class XorSearchEvent(Event):
    """Xor (Kotlin/Lucene) 검색 엔진으로 라우팅하기 위한 이벤트"""
    query: str
    block_type: Optional[str]

class LlamaSearchEvent(Event):
    """LlamaIndex 자체 VectorStore로 라우팅하기 위한 이벤트"""
    query: str
    block_type: Optional[str]
    persist_dir: str # 메타데이터에서 읽어온 경로 추가

class SearchWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xor_client = Xor()
        self.llama_index = None
        self._manifest_cache = None

    def _load_manifest(self) -> Optional[Dict[str, Any]]:
        """METADATA_ROOT에서 매니페스트 파일을 읽어옵니다."""
        if not MANIFEST_FILE.exists():
            log.warning(f"[Manifest] 상태 파일을 찾을 수 없습니다: {MANIFEST_FILE}")
            return None
        
        try:
            with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"[Manifest] 파일 읽기 실패: {e}")
            return None

    @step
    async def route_query(self, ev: StartEvent) -> XorSearchEvent | LlamaSearchEvent:
        """Step 1: 메타데이터를 확인하고 검색 엔진을 결정합니다."""
        query = ev.get("query")
        engine = ev.get("engine", "xor")
        block_type = ev.get("block_type")

        if not query:
            raise ValueError("검색어(query)가 필요합니다.")

        ## Llama 엔진 선택 시 메타데이터 확인
        if engine.lower() == "llama":
            manifest = self._load_manifest()
            if not manifest:
                log.error("LlamaIndex 메타데이터가 없어 XOR로 Fallback 합니다.")
                return XorSearchEvent(query=query, block_type=block_type)
            
            ## 메타데이터 정보 출력 (사용자 피드백)
            log.info(f"[Llama:Status] 인덱싱 시점: {manifest.get('indexed_at')}")
            log.info(f"[Llama:Status] 총 노드 수: {manifest.get('total_nodes')}")
            
            return LlamaSearchEvent(
                query=query, 
                block_type=block_type, 
                persist_dir=manifest.get("persist_dir")
            )
        
        return XorSearchEvent(query=query, block_type=block_type)

    @step
    async def execute_xor_search(self, ev: XorSearchEvent) -> StopEvent:
        """Step 2-A: 레거시 Xor 클라이언트를 통해 검색합니다."""
        log.info("[Search:Xor] 원격 서버 쿼리 중...")
        raw_results = await asyncio.to_thread(self.xor_client.search, ev.query, ev.block_type)
        
        results = [
            {
                "engine": "XOR",
                "score": r.score,
                "block_type": r.block_type,
                "file_path": r.file_path,
                "section_path": r.section_path
            }
            for r in raw_results
        ]
        return StopEvent(result=results)

    @step
    async def execute_llama_search(self, ev: LlamaSearchEvent) -> StopEvent:
        """Step 2-B: 메타데이터에 기록된 persist_dir에서 인덱스를 로드하여 검색합니다."""
        log.info(f"[Search:Llama] 데이터 로드 중: {ev.persist_dir}")
        
        if self.llama_index is None:
            try:
                # 메타데이터에서 가져온 경로를 사용하여 로드
                storage_context = await asyncio.to_thread(
                    StorageContext.from_defaults, persist_dir=ev.persist_dir
                )
                self.llama_index = await asyncio.to_thread(
                    load_index_from_storage, storage_context
                )
            except Exception as e:
                log.error(f"[Search:Llama] 인덱스 로드 실패: {e}")
                return StopEvent(result=[])

        filters = None
        if ev.block_type:
            from llama_index.core.vector_stores.types import MetadataFilters, ExactMatchFilter
            filters = MetadataFilters(
                filters=[ExactMatchFilter(key="block_type", value=ev.block_type)]
            )
            
        retriever = self.llama_index.as_retriever(similarity_top_k=10, filters=filters)
        nodes: List[NodeWithScore] = await asyncio.to_thread(retriever.retrieve, ev.query)
        
        results = [
            {
                "engine": "LLAMA",
                "score": n.score,
                "block_type": n.metadata.get("block_type", "unknown"),
                "file_path": n.metadata.get("file_path", "unknown"),
                "section_path": n.metadata.get("section_path", "unknown")
            }
            for n in nodes
        ]
        return StopEvent(result=results)

async def main():
    parser = argparse.ArgumentParser(description="Metadata-aware Hybrid Search")
    parser.add_argument("query", nargs="+", help="검색어")
    parser.add_argument("--engine", choices=["xor", "llama"], default="xor")
    parser.add_argument("--type", dest="block_type")
    args = parser.parse_args()
    query_str = " ".join(args.query)
    workflow = SearchWorkflow(timeout=30.0)
    
    try:
        results = await workflow.run(query=query_str, engine=args.engine, block_type=args.block_type)
        print(f"\n[Final Results] {len(results)} matches found\n")
        for r in results:
            print(f"{r['score']:.3f} | {r['block_type']} | {r['section_path']} | {r['file_path']}")
    except Exception as e:
        log.error(f"실행 중 오류: {e}")

if __name__ == "__main__":
    asyncio.run(main())