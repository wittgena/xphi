# phase.reflect.cognitive.worker
import asyncio
from bound.surface.emitter import get_emitter
from bound.client.local.engine import LLMEngine
from field.kernel.state.aggregator import InternalContext
from session.contract.xor.context.assembler import ContextAssembler

log = get_emitter('cognitive.worker')

class CognitiveWorker:
    """@topos.worker: Cognitive Coupler로부터 InternalContext를 받아, 조립(Assemble)하고 판단(LLM)을 내리는 비동기 대뇌 피질"""
    def __init__(self, engine: LLMEngine, assembler: ContextAssembler):
        self.engine = engine
        self.assembler = assembler
        ## LlamaIndex의 전역 Settings에 의존하지 않고, 엔진을 직접 소유(Composition)합니다.

    async def process(self, context: InternalContext):
        """@flow: InternalContext -> Assembler -> Messages -> Async LLM -> Action"""
        log.info(f"Worker initiated processing for Psi({context.event.symbol})")

        try:
            ## 컨텍스트 조립 (Assembly)
            ## 이전 단계에서 만든 ContextAssembler를 활용하여 LLM이 먹을 수 있는 구조로 투영
            query_text = f"Resolve intention for symbol: {context.event.symbol}"
            
            messages = self.assembler.assemble(
                query=query_text,
                anchor="당신은 meta.self의 핵심 인지 판단 코어입니다.",
                state=[f"Phase={context.state.phase}", f"Version={context.state.version}"],
                ## surface_signals를 Xor 검색의 키워드로 활용하거나 YAML로 넘길 수 있음
                use_xor=True 
            )

            ## 물리적 표면(LLM)으로의 비동기 투사 (Execution)
            log.info("Projecting to local LLM Engine...")
            response_text = await asyncio.to_thread(
                self.engine.chat, 
                system_prompt=messages[0]['content'], # 단순화된 예시
                user_prompt=messages[-1]['content']
            )

            ## 결과 해석 및 후속 조치 (Rupture or Align)
            log.info(f"Cognitive resolution complete. Output length: {len(response_text)}")
            await self._handle_decision(context, response_text)
        except Exception as e:
            log.error(f"Cognitive process collapsed: {e}")
            ## 필요 시 자기 치유적 재귀 호출(Re-entry) 로직 발동 가능

    async def _handle_decision(self, context: InternalContext, response: str):
        """@action: LLM의 응답을 바탕으로 다음 액션(Actuator로 전송 등)을 수행"""
        ## 예: JSON 응답 파싱, Surface Actuator로 명령 방출 등
        pass