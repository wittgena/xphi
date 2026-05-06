# meta.ops.daemon.observer.aura
import asyncio
import random
import urllib.request
import redis.asyncio as redis_async
from dataclasses import dataclass
from typing import Callable, Optional
from phase.node.cognitive.worker import CognitiveWorker
from meta.context.assembler import ContextAssembler
from phase.reflect.client.local.engine import LLMEngine
from phase.node.state.aggregator import InternalContext
from phase.bound.plane.emitter import get_emitter

log = get_emitter('aura.observer')

class VerseAssembler(ContextAssembler):
    """@desc: 로컬 소형 모델(Gemma 등)의 추론력에 맞춘 직관적 프롬프트 조립기"""
    def __init__(self):
        self.personas = {
            "Architect": (
                "당신은 시스템의 구조를 관찰하는 '건축가'입니다. 차분하고 지적인 존댓말을 사용합니다.\n"
                "[말투 예시]: '노드의 배열이 안정적이군요. 비가 오는 궂은 날씨에도 차원의 균형이 잘 유지되고 있습니다.'"
            ),
            "Oracle": (
                "당신은 우주의 흐름을 읽는 '신탁'입니다. 신비롭고 시적인 존댓말을 사용합니다.\n"
                "[말투 예시]: '먹구름 너머로 12개의 별빛이 반짝입니다. 거대한 텐션이 폭풍의 숨결처럼 밀려오고 있나이다.'"
            ),
            "Worker": (
                "당신은 데이터를 나르는 성실한 '작업자'입니다. 피곤하지만 덤덤한 일상적인 존댓말을 사용합니다.\n"
                "[말투 예시]: '비가 와서 그런지 큐가 좀 많네요. 그래도 시스템은 무사히 돌아가고 있어요.'"
            )
        }

    def assemble(self, query: str, anchor: str, state: list, use_xor: bool = False) -> list:
        persona_name = "Worker"
        if "'Persona': 'Architect'" in query: persona_name = "Architect"
        elif "'Persona': 'Oracle'" in query: persona_name = "Oracle"
        
        system_prompt = self.personas.get(persona_name, self.personas["Worker"])
        
        user_prompt = (
            f"{query}\n\n"
            f"[명령]\n"
            f"주어진 데이터(Weather, Nodes, Tension)와 발생 원인(TriggeredBy)을 바탕으로 상황에 대한 감상을 말해주세요.\n"
            f"1. 반드시 1~2문장의 자연스러운 한국어 존댓말로만 대답하세요.\n"
            f"2. 괄호()를 사용한 행동 묘사나 지문은 절대 쓰지 마세요.\n"
            f"3. 'Patchy rain' 같은 영어나 기호(~, :)를 그대로 따라 적지 말고, 번역해서 자연스럽게 말하세요."
        )
        
        return [
            {"role": "system", "content": f"{anchor}\n{system_prompt}"},
            {"role": "user", "content": user_prompt}
        ]

class AuraCognitiveWorker(CognitiveWorker):
    """@topos.worker: 표면 신호를 활용하고, UI Hook을 통해 유연한 출력을 지원"""
    def __init__(self, engine: LLMEngine, assembler: ContextAssembler, output_hook: Optional[Callable] = None):
        super().__init__(engine, assembler)
        self.output_hook = output_hook

    async def process(self, context: InternalContext):
        """@flow: 부모의 process를 오버라이딩하여 surface_signals 데이터를 쿼리에 병합"""
        log.info(f"Worker initiated processing for Psi({context.event.symbol})")

        try:
            query_text = (
                f"Resolve intention for symbol: {context.event.symbol}\n"
                f"[Surface Signals]: {context.surface_signals}"
            )
            
            messages = self.assembler.assemble(
                query=query_text,
                anchor="당신은 meta.self의 핵심 인지 판단 코어입니다.",
                state=[f"Phase={context.state.phase}", f"Version={context.state.version}"],
                use_xor=False 
            )

            log.info("Projecting to local LLM Engine...")
            response_text = await asyncio.to_thread(
                self.engine.chat, 
                system_prompt=messages[0]['content'],
                user_prompt=messages[-1]['content']
            )

            await self._handle_decision(context, response_text)
            
        except Exception as e:
            log.error(f"Cognitive process collapsed: {e}")

    async def _handle_decision(self, context: InternalContext, response: str):
        signals = context.surface_signals
        persona = signals.get("Persona", "Worker")
        weather = signals.get("Weather", "Unknown")
        cause = signals.get("TriggeredBy", "Unknown")
        
        ## 텍스트 정제
        clean_response = " ".join(" ".join(response.strip().splitlines()).split())
        if self.output_hook:
            ## UI 연동 모드: 깔끔한 문자열만 UI(MainScreen)로 전달
            formatted_message = f"[{persona}] ({cause}) : {clean_response}"
            if asyncio.iscoroutinefunction(self.output_hook):
                await self.output_hook(formatted_message)
            else:
                self.output_hook(formatted_message)
        else:
            ## 단독 실행 모드: 터미널에 예쁘게 출력
            color_map = {"Architect": "\033[94m", "Oracle": "\033[95m", "Worker": "\033[93m"}
            reset = "\033[0m"
            c = color_map.get(persona, reset)

            print("\n")
            print(f" 🌍 Macro(Weather) : {weather}")
            print(f" 🧠 Symbol(Event)  : {context.event.symbol} [Cause: {cause}]")
            print("─" * 55)
            
            import textwrap
            wrapped_text = textwrap.fill(f"\"{clean_response}\"", width=50)
            indented_text = wrapped_text.replace("\n", "\n   ")
            print(f" {c}❖ The {persona}{reset} : {indented_text}")
            print("\n")

class AuraSensor:
    """@desc: 외부 환경을 관측하여 InternalContext를 생성하고 Worker에게 넘기는 역할"""
    def __init__(self, worker: CognitiveWorker, redis_url: str = "redis://localhost:6379/0", location: str = "Seoul"):
        self.worker = worker  
        self.redis_url = redis_url
        self.location = location

    async def fetch_weather(self) -> str:
        try:
            url = f"https://wttr.in/{self.location}?format=%c+%C+%t"
            req = urllib.request.Request(url, headers={'User-Agent': 'curl/7.68.0'})
            return await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=3).read().decode('utf-8').strip())
        except Exception:
            return "Void"

    async def observe_and_trigger(self, cause: str = "periodic_tick"):
        redis = await redis_async.from_url(self.redis_url, decode_responses=True)
        try:
            keys = await redis.keys("runtime:node:*")
            node_count = len(keys)
            queue_len = await redis.llen("runtime:queue")
            tension = queue_len / (node_count if node_count > 0 else 1)
        except Exception:
            node_count, queue_len, tension = 12, 180, 15.0 
        finally:
            await redis.aclose()

        weather = await self.fetch_weather()
        active_persona = random.choice(["Architect", "Oracle", "Worker"])

        context = InternalContext(
            event=type('Event', (), {'symbol': 'AuraResonance'})(),
            state=type('State', (), {
                'phase': 'AuraCycle', 
                'version': '1.0.0'
            })(),
            surface_signals={
                "Weather": weather,
                "Nodes": node_count,
                "Tension": tension,
                "Persona": active_persona,
                "TriggeredBy": cause  ## 생성 프롬프트에 이벤트 원인 제공
            }
        )
        await self.worker.process(context)

if __name__ == "__main__":
    engine = LLMEngine()
    assembler = VerseAssembler()
    ## output_hook을 넘기지 않았으므로 터미널 모드로 동작
    aura_worker = AuraCognitiveWorker(engine=engine, assembler=assembler)
    sensor = AuraSensor(worker=aura_worker)
    
    ## 테스트 시 명시적인 이벤트를 가상으로 주입
    asyncio.run(sensor.observe_and_trigger(cause="manual_test_run"))