# arch.topos.node.event
## @lineage: arch.topos.flow.event
## @lineage: phase.executor.flow.event
## @lineage: watcher.xe.scope.event
## @lineage: topos.scope.flow.event
from typing import Any, Optional
from xphi.arch.topos.node.gan import Message

class AgentConfigured(Message):
    def __init__(self, settings=None, is_proxy: bool = False):
        super().__init__("agent_configured", bubble=True)
        self.settings = settings
        self.is_proxy = is_proxy

class WorkspaceReady(Message):
    def __init__(self, workspace_ref: str, is_proxy: bool = False):
        super().__init__("workspace_ready", bubble=True)
        self.workspace_ref = workspace_ref
        self.is_proxy = is_proxy

class LLMEventMessage(Message):
    def __init__(self, llm_message: str):
        super().__init__("llm_event", bubble=True)
        self.llm_message = llm_message

class TaskCompletedMessage(Message):
    def __init__(self, cost: float, is_proxy: bool = False):
        super().__init__("task_completed", bubble=True)
        self.cost = cost
        self.is_proxy = is_proxy