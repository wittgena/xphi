# topos.field.message.pump
import asyncio

class Message:
    def __init__(self, name: str, bubble: bool = False):
        self.name = name
        self.bubble = bubble
        self.sender = None

class MessagePump:
    def __init__(self, name: str):
        self.name = name
        self.parent = None
        self.children = set()           # 자식 노드들을 관리하는 집합
        self._queue = asyncio.Queue()
        self._running = False
        self._task = None               # 자신의 이벤트 루프 태스크

    def post_message(self, message: Message):
        message.sender = self
        self._queue.put_nowait(message)

    async def run(self):
        """자신의 메시지 처리 무한 루프"""
        self._running = True
        print(f"[{self.name}] 펌프 시작됨")
        
        while self._running:
            message = await self._queue.get()
            if message is None: # 종료 신호
                break
                
            await self._dispatch_message(message)
            self._queue.task_done()
            
        print(f"[{self.name}] 펌프 종료됨")

    async def _dispatch_message(self, message: Message):
        handler_name = f"on_{message.name}"
        handler = getattr(self, handler_name, None)
        
        if handler and callable(handler):
            await handler(message)

        # 버블링: 나를 호출한 부모가 있다면 위로 전달
        if message.bubble and self.parent:
            self.parent.post_message(message)

    def mount(self, child: 'DynamicMessagePump'):
        """자식을 추가하고 즉시 독립적인 이벤트 루프를 실행시킵니다."""
        child.parent = self
        self.children.add(child)
        
        # 자식의 루프를 백그라운드 태스크로 띄움
        child._task = asyncio.create_task(child.run())
        print(f"-> {child.name} 이(가) {self.name} 에 마운트되었습니다.")

    async def unmount(self, child: 'DynamicMessagePump'):
        """자식을 트리에서 떼어내고, 큐를 닫아 안전하게 종료시킵니다."""
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            
            # 자식 펌프 정지 신호 전송
            child._running = False
            child._queue.put_nowait(None)
            
            # 자식의 태스크가 완전히 끝날 때까지 대기 (고아 프로세스 방지)
            if child._task:
                await child._task
                
            print(f"<- {child.name} 이(가) {self.name} 에서 언마운트되었습니다.")