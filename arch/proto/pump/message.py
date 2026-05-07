# arch.proto.pump.message
import asyncio
from typing import Optional, Set

# --- 1. Core Message System ---
class Message:
    """
    펌프 간 통신을 위한 기본 메시지 클래스.
    모든 커스텀 이벤트(예: LLMEvent, TaskCompleted)는 이 클래스를 상속받아야 합니다.
    """
    def __init__(self, name: str, bubble: bool = False):
        self.name = name
        self.bubble = bubble
        self.sender: Optional['MessagePump'] = None

# --- 2. Core Pump System ---
class MessagePump:
    """
    독립적인 비동기 이벤트 루프를 가지는 노드의 기본 클래스.
    Textual의 컴포넌트 모델처럼 계층 구조(Parent-Child)와 이벤트 버블링을 지원합니다.
    """
    def __init__(self, name: str):
        self.name = name
        self.parent: Optional['MessagePump'] = None
        self.children: Set['MessagePump'] = set()
        self._queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def post_message(self, message: Message):
        """메시지를 자신의 큐에 밀어넣습니다."""
        message.sender = self
        self._queue.put_nowait(message)

    async def run(self):
        """자신의 메시지 큐를 처리하는 무한 루프. (Task로 구동됨)"""
        self._running = True
        # print(f"🟢 [{self.name}] 펌프 시작됨") # 필요 시 로거로 전환
        
        while self._running:
            message = await self._queue.get()
            if message is None:  # 종료 시그널
                break
                
            await self._dispatch_message(message)
            self._queue.task_done()
            
        # print(f"🔴 [{self.name}] 펌프 종료됨")

    async def _dispatch_message(self, message: Message):
        """메시지 이름에 매칭되는 핸들러(on_*)를 찾아 실행하고, 필요시 부모로 버블링합니다."""
        handler_name = f"on_{message.name}"
        handler = getattr(self, handler_name, None)
        
        if handler and callable(handler):
            await handler(message)

        # 버블링: 나를 호출한 부모가 있고, 메시지가 버블링 속성을 가졌다면 위로 전달
        if message.bubble and self.parent:
            self.parent.post_message(message)

    def mount(self, child: 'MessagePump'):
        """자식 노드를 추가하고 즉시 독립적인 백그라운드 태스크로 루프를 실행시킵니다."""
        child.parent = self
        self.children.add(child)
        child._task = asyncio.create_task(child.run())
        # print(f" 🔗 -> {child.name} 이(가) {self.name} 에 마운트되었습니다.")

    async def unmount(self, child: 'MessagePump'):
        """자식 노드를 트리에서 떼어내고, 큐를 닫아 고아 프로세스 없이 안전하게 종료시킵니다."""
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            
            # 자식 펌프 정지 신호 전송
            child._running = False
            child._queue.put_nowait(None)
            
            # 자식의 태스크가 완전히 끝날 때까지 대기
            if child._task:
                await child._task
            # print(f" ✂️ <- {child.name} 이(가) {self.name} 에서 언마운트되었습니다.")