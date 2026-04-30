# agent.action.tool.counter
import json
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Sequence
from pydantic import Field

TOOL_NAME = "tool.counter"

class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

# (선택) 여기에 @contract.node 또는 @contract.cli 등을 붙여
# 레지스트리에 "이 툴 팩토리가 존재한다"는 메타데이터만 등록할 수 있습니다.
def load_count_tool():
    """
    지연 로딩(Lazy Loading) 경계(Boundary):
    이 함수가 명시적으로 호출되는 시점(Theoria)에만 OpenHands SDK가 메모리에 적재되고
    부작용(Side-effect)이 발생합니다.
    """
    from openhands.sdk import Action, Observation, TextContent, ToolDefinition
    from openhands.sdk.tool import ToolExecutor, register_tool

    class CountAction(Action):
        message: str = Field(description="Log message content")
        level: LogLevel = Field(default=LogLevel.INFO)
        data: dict[str, Any] = Field(default_factory=dict)
        target_path: str = Field(
            default="/tmp/agent.action.json", 
            description="Target JSON file path. Must be absolute path."
        )

    class LogObservation(Observation):
        success: bool
        final_path: str
        total_entries: int

        @property
        def to_llm_content(self) -> Sequence[TextContent]:
            status = "✅" if self.success else "❌"
            return [TextContent(text=f"{status} [Stored: {self.final_path}] (Count: {self.total_entries})")]

    class CountExecutor(ToolExecutor[CountAction, LogObservation]):
        def __call__(self, action: CountAction, conversation=None) -> LogObservation:
            log_path = Path(action.target_path)
            entries = []
            
            if log_path.exists():
                try:
                    with open(log_path, "r") as f:
                        entries = json.load(f)
                except (json.JSONDecodeError, OSError):
                    pass

            entry = {
                "timestamp": datetime.now(UTC).isoformat(),
                "level": action.level.value,
                "message": action.message,
                "data": action.data,
            }
            entries.append(entry)

            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w") as f:
                json.dump(entries, f, indent=2)

            return LogObservation(
                success=True,
                final_path=str(log_path),
                total_entries=len(entries)
            )

    class CountDataTool(ToolDefinition[CountAction, LogObservation]):
        @classmethod
        def create(cls, conv_state, **params) -> Sequence[ToolDefinition]:
            return [cls(
                description="target_path 지시를 최우선으로 따를 것",
                action_type=CountAction,
                observation_type=LogObservation,
                executor=CountExecutor(),
            )]
            
    # 동적으로 생성된 클래스와 함수들을 반환
    return CountDataTool, CountAction, CountExecutor, register_tool


if __name__ == "__main__":
    # 스크립트 단독 실행 시에만 지연 로딩 팩토리를 가동(Rupture)
    CountDataTool, CountAction, CountExecutor, register_tool = load_count_tool()
    
    # Router Registration
    register_tool(TOOL_NAME, CountDataTool)

    action = CountAction(
        message="System health check",
        level=LogLevel.DEBUG,
        data={"component": "executor", "status": "nominal"},
        target_path="agent.target.json"
    )
    executor = CountExecutor()
    obs = executor(action)
    print(obs.to_llm_content[0].text)