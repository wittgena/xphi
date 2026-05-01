# bridge.xor.agent.code
import sys
import argparse
import asyncio
from typing import List, Optional
from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core.schema import NodeWithScore
from llama_index.llms.openai import OpenAI
from llama_index.core.prompts import PromptTemplate
from flow.surface.emitter import get_logger
from bound.resolver import resolve_path

log = get_logger("context.code")

try:
    METADATA_ROOT = resolve_path("io") / "metadata"
    MANIFEST_FILE = METADATA_ROOT / "llama_manifest.json"
except Exception as e:
    log.error(f"[error] 경로 설정 실패: {e}")
    sys.exit(1)

class NodesRetrievedEvent(Event):
    """검색이 완료되어 AI에게 컨텍스트로 넘겨줄 노드들이 준비된 상태"""
    query: str
    nodes: List[NodeWithScore]

class CodeQAWorkflow(Workflow):
    def __init__(self, persist_dir: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.persist_dir = persist_dir
        self.llama_index = None
        ## 답변을 생성할 LLM 초기화
        self.llm = OpenAI(model="gpt-4o-mini", temperature=0.1) 

        ## 나중에 LlamaIndex를 걷어낼 때 그대로 재사용할 수 있는 순수 프롬프트
        self.qa_prompt = PromptTemplate(
            "당신은 우리 시스템의 시니어 엔지니어입니다. "
            "다음 [코드 컨텍스트]를 바탕으로 개발자의 [질문]에 답변하세요.\n"
            "코드에 없는 내용을 지어내지 말고, 답변 시 출처(파일 경로)를 명시하세요.\n\n"
            "[코드 컨텍스트]\n{context_str}\n\n"
            "[질문]\n{query_str}\n\n"
            "답변:"
        )

    @step
    async def retrieve_code_blocks(self, ev: StartEvent) -> NodesRetrievedEvent:
        """Step 1: 질문과 관련된 코드 블록(Node)을 검색합니다."""
        query = ev.get("query")
        if not query:
            raise ValueError("검색어(query)가 필요합니다.")

        log.info(f"[RAG:Retrieve] 질문 분석 및 관련 코드 검색 중: '{query}'")
        
        ## 인덱스 로드 (기존과 동일)
        if self.llama_index is None:
            storage_context = await asyncio.to_thread(StorageContext.from_defaults, persist_dir=self.persist_dir)
            self.llama_index = await asyncio.to_thread(load_index_from_storage, storage_context)

        ## 상위 5개의 가장 관련성 높은 코드 블록 추출
        retriever = self.llama_index.as_retriever(similarity_top_k=5)
        nodes: List[NodeWithScore] = await asyncio.to_thread(retriever.retrieve, query)
        
        return NodesRetrievedEvent(query=query, nodes=nodes)

    @step
    async def synthesize_answer(self, ev: NodesRetrievedEvent) -> StopEvent:
        """Step 2: 검색된 코드를 LLM의 컨텍스트로 주입하여 최종 답변을 생성합니다."""
        log.info(f"[RAG:Synthesize] {len(ev.nodes)}개의 코드 블록을 바탕으로 답변 생성 중...")
        
        if not ev.nodes:
            return StopEvent(result={"answer": "관련된 코드나 문서를 찾을 수 없습니다.", "sources": []})

        ## AI가 이해하기 좋게 컨텍스트 문자열 조립 (추출했던 메타데이터 활용)
        context_parts = []
        for n in ev.nodes:
            file_path = n.metadata.get('file_path', 'unknown')
            block_type = n.metadata.get('block_type', 'code')
            content = n.text
            context_parts.append(f"--- 파일: {file_path} ({block_type}) ---\n{content}")
            
        context_str = "\n\n".join(context_parts)

        ## 프롬프트 포맷팅 및 LLM 호출
        formatted_prompt = self.qa_prompt.format(context_str=context_str, query_str=ev.query)
        response = await self.llm.acomplete(formatted_prompt)

        ## 출처(Sources) 정리
        sources = [{"path": n.metadata.get('file_path'), "score": n.score} for n in ev.nodes]
        return StopEvent(result={
            "answer": str(response),
            "sources": sources
        })

async def main():
    parser = argparse.ArgumentParser(description="Code RAG CLI")
    parser.add_argument("query", nargs="+", help="코드베이스에 대해 질문하세요.")
    args = parser.parse_args()
    query_str = " ".join(args.query)

    workflow = CodeQAWorkflow(persist_dir="./io/metadata/llama_storage", timeout=60.0)
    
    try:
        result = await workflow.run(query=query_str)
        
        print("\n## [AI 엔지니어 답변]")
        print("=" * 60)
        print(result["answer"])
        print("-" * 60)
        print("## [참고한 출처]")
        for s in result["sources"]:
            print(f" - {s['path']} (관련도: {s['score']:.3f})")
            
    except Exception as e:
        log.error(f"오류 발생: {e}")

if __name__ == "__main__":
    asyncio.run(main())