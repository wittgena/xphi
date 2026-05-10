# arch.model.nerve.events
from typing import Any
from arch.model.nerve.gan import Message

class AgentConfigured(Message):
    """
    하위 설정 노드(PolicyNode, SettingsNode 등)가 
    자신의 초기화 및 설정 작업을 무사히 마쳤음을 부모(App)에게 알리는 이벤트입니다.
    """
    def __init__(self):
        # 상위 오케스트레이터로 전달되어야 하므로 bubble=True 설정
        super().__init__("agent_configured", bubble=True)


class WorkspaceReady(Message):
    """
    DockerWorkspaceNode가 컨테이너를 성공적으로 띄우고 
    작업 환경(Workspace) 준비를 완료했음을 알리는 이벤트입니다.
    """
    def __init__(self, workspace_ref: str):
        super().__init__("workspace_ready", bubble=True)
        self.workspace_ref = workspace_ref


class LLMEventMessage(Message):
    """
    OpenHands 에이전트 실행 중 발생하는 실시간 텍스트 스트리밍이나 
    도구(Tool) 사용 로그를 메인 메시지 펌프로 브릿징하는 이벤트입니다.
    """
    def __init__(self, llm_message: Any):
        super().__init__("llm_event", bubble=True)
        self.llm_message = llm_message


class TaskCompletedMessage(Message):
    """
    OpenHands의 Conversation(대화/작업)이 최종적으로 종료되었으며, 
    해당 세션에서 발생한 누적 비용(Cost)을 보고하는 이벤트입니다.
    """
    def __init__(self, cost: float):
        super().__init__("task_completed", bubble=True)
        self.cost = cost