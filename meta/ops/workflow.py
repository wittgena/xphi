# meta.ops.workflow
from typing import Any
from pydantic import PrivateAttr
from llama_index.core import Settings, Document, VectorStoreIndex
from llama_index.core.llms import CustomLLM, CompletionResponse, LLMMetadata
from llama_index.core.llms.callbacks import llm_completion_callback
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from bound.surface.emitter import get_emitter
from bound.folding import folding
from bound.client.local.engine import LLMEngine

log = get_emitter('agent.workflow')

class LocalAdapter(CustomLLM):
    """LlamaIndex Pipeline Interface와 물리적 실행 표면(LLMEngine)을 연결하는 어댑터"""
    context_window: int = 4096
    num_output: int = 512
    model_name: str = "local_gemma_1b"
    _client: LLMEngine = PrivateAttr()

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._client = LLMEngine()

    @property
    def metadata(self) -> LLMMetadata:
        """코어 파이프라인에 모델 스펙 명세 보고"""
        return LLMMetadata(
            context_window=self.context_window,
            num_output=self.num_output,
            model_name=self.model_name,
        )

    @llm_completion_callback()
    def complete(self, prompt: str, **kwargs: Any) -> CompletionResponse:
        """물리적 실행 표면으로의 상태 투영 및 결과 합성"""
        system_prompt = kwargs.get("system_prompt", "You are a strict system architecture synthesizer.")
        response_text = self._client.chat(system_prompt=system_prompt, user_prompt=prompt)
        return CompletionResponse(text=response_text)

    @llm_completion_callback()
    def stream_complete(self, prompt: str, **kwargs: Any):
        raise NotImplementedError("Streaming execution not implemented.")

def workflow():
    """파이프라인 위상 설정 및 샌드박스 기반 실행 제어"""
    try:
        # [Phase 0] 의존성 주입
        log.info("Initializing Execution Surface Adapter...")
        Settings.llm = LocalAdapter()
        Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

        # [Phase 1 & 2] 데이터 맵핑 및 인덱스 상태 저장
        log.info("Mapping topological documents and Generating Index...")
        doc_text = (
            "The architecture utilizes re-entry thresholds rather than termination conditions "
            "to manage system state. The physical execution surface is isolated via the OpenHands framework."
        )
        documents = [Document(text=doc_text)]
        index = VectorStoreIndex.from_documents(documents)

        # [Phase 3] 복수의 독립적 실행 표면 할당
        # 시스템 아키텍처 상 각기 다른 위상을 담당하는 두 개의 엔진 준비
        standard_retriever = index.as_retriever()
        standard_query_engine = index.as_query_engine()

        query_text = "Explain the execution surface isolation."
        log.info(f"Initiating state evaluation with query: {query_text}")

        try:
            ## 이질적인 두 엔진을 동시에 샌드박스에 통과시켜 위상적 막
            with folding(standard_retriever, standard_query_engine, re_entry_limit=5) as (meta_retriever, meta_engine):
                log.info("Executing within multi-surface re-entry threshold sandbox...")
                
                ## 단일 검색 위상(Retrieval Phase)에 대한 개별 통제 테스트
                nodes = meta_retriever.retrieve(query_text)
                log.info(f"Isolated Retrieval successfully mapped {len(nodes)} nodes.")

                ## 전체 쿼리 위상(Query Phase)에 대한 통제 테스트
                response = meta_engine.query(query_text)
                print(f"## Synthesized Output\n{response.response}")
        except RuntimeError as e:
            log.error(f"Re-entry threshold exceeded across synthesized surfaces: {e}")
    finally:
        ## [Phase 4] 물리적 인프라 정리
        if Settings.llm and hasattr(Settings.llm, "_client"):
            log.info("Deallocating physical execution surface...")
            if Settings.llm._client._process:
                Settings.llm._client._process.terminate()

if __name__ == "__main__":
    workflow()