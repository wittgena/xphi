# resonance.hand.factory
import os
from pathlib import Path
from pydantic import SecretStr
from openhands.sdk import LLM, Agent, Tool
from openhands.tools.preset.default import get_default_tools
from bridge.client.local.engine import SERVER_PORT, MODEL_NAME
from bound.resolver import resolve_path

RES_ROOT = resolve_path("res")

def create_shell_agent(usage_id: str) -> Agent:
    """
    Hands의 껍데기만 유지하고, 도구와 복잡한 프롬프트를 모두 거세한 순수 생성기
    """
    # 1. 도구 빈 배열 전달 (Tool-less)
    tools = [] 
    
    # 2. 아주 단순화된 시스템 프롬프트 주입
    # "너는 도구를 쓸 수 없고, 오직 마크다운 텍스트만 출력해야 한다."
    # (실제 환경에 맞게 system_prompt_kwargs 나 프롬프트 파일을 극단적으로 단순화)
    
    return Agent(
        llm=get_shared_llm(usage_id),
        tools=tools,
        system_prompt_filename=str(RES_ROOT / "handex" / "generator.j2"), # 오직 마크다운만 뱉으라는 내용
    )