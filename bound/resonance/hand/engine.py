# bound.resonance.hand.engine
from abc import ABC, abstractmethod
from bound.resonance.schema import BridgeEvent

class BaseEngine(ABC):
    @abstractmethod
    def ask(self, prompt: str, callback: callable) -> str:
        """엔진에 질문을 던지고 스트리밍 이벤트를 콜백으로 전달"""
        pass

class HandEngine(BaseEngine):
    """Hand의 복잡성을 BridgeEvent로 단순화하는 어댑터"""
    def __init__(self, host_url: str, agent_usage: str):
        from openhands.sdk import Workspace
        from bound.resonance.hand.factory import create_shell_agent
        self.workspace = Workspace(host=host_url, working_dir=".")
        self.agent = create_shell_agent(usage_id=agent_usage)

    def ask(self, prompt, callback):
        from openhands.sdk import Conversation
        from openhands.sdk.llm import content_to_str

        def _internal_callback(oh_event):
            content = ""
            e_type = type(oh_event).__name__
            
            if e_type == "MessageAction" and hasattr(oh_event, "content"):
                content = str(oh_event.content)
            elif hasattr(oh_event, "llm_message") and oh_event.llm_message:
                content = "".join(content_to_str(oh_event.llm_message.content))
            
            if content:
                callback(BridgeEvent(content=content, source=getattr(oh_event, "source", "")))

        conv = Conversation(agent=self.agent, workspace=self.workspace, callbacks=[_internal_callback])
        try:
            conv.send_message(prompt)
            conv.run()
        finally:
            conv.close()
        return ""