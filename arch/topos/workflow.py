# arch.topos.workflow
import asyncio
import inspect
import sys
import typing
import types
from functools import wraps

from watcher.plane.emitter import get_emitter
from arch.topos.node.gan import GanNode, Message

log = get_emitter("topos.workflow")


class WorkflowMessage(Message):
    """
    @desc: Workflow 라우팅에 사용될 메시지들의 기반 클래스.
    일반 Message와 구분하기 위해 사용되며, 이름(name)을 클래스명으로 자동 할당합니다.
    """
    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__)
        for k, v in kwargs.items():
            setattr(self, k, v)


class StopMessage(WorkflowMessage):
    def __init__(self, result=None, **kwargs):
        super().__init__(**kwargs)
        self.result = result


class ErrorMessage(WorkflowMessage):
    def __init__(self, msg: str = "", **kwargs):
        super().__init__(**kwargs)
        self.msg = msg


def step(func):
    """@desc: Decorator marking a method as an executable workflow step"""
    func.__step_config = True
    return func


class WorkflowMeta(type):
    """@desc: Message 타입 힌팅을 분석하여 라우팅 룰을 주입하는 메타클래스"""
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if name == "Workflow":
            return cls

        meta = getattr(cls, "Meta", None)
        trans_rules = getattr(meta, "trans_rules", {}) if meta else {}
        
        step_methods = {
            func_name: func for func_name, func in namespace.items()
            if callable(func) and getattr(func, "__step_config", False)
        }

        for method_name, original_method in step_methods.items():
            @wraps(original_method)
            async def wrapper(self, msg_obj, *args, _method_name=method_name, _orig=original_method, **kwargs):
                try:
                    result_msg = await _orig(self, msg_obj, *args, **kwargs)
                except Exception as e:
                    log.error(f"[Workflow] 💥 Exception in '{_method_name}': {e}", exc_info=True)
                    return ErrorMessage(msg=str(e))

                # 반환값이 WorkflowMessage 기반인지 확인
                if not isinstance(result_msg, WorkflowMessage):
                    return result_msg

                # 상태 우회 룰 (Meta.trans_rules) 적용
                status = getattr(result_msg, "status", None)
                if status and status in trans_rules:
                    target_msg_cls = trans_rules[status]
                    log.warning(f"[Workflow] Trans rule triggered for status '{status}' -> '{target_msg_cls.__name__}'")
                    return target_msg_cls(msg=getattr(result_msg, "msg", ""))

                return result_msg
            
            setattr(cls, method_name, wrapper)
        return cls


class Workflow(GanNode, metaclass=WorkflowMeta):
    """
    @desc: 시스템 제어권을 완전히 버리고, 오직 타입 기반 메시지 라우팅만 수행하는 순수 액터.
    일반 Message는 GanNode의 Name-based 라우팅을 따르고, 
    WorkflowMessage는 Type-based 라우팅을 따르도록 설계되었습니다.
    """
    
    def __init__(self, name: str = "workflow", timeout: float = 600.0, **kwargs):
        super().__init__(name=name)
        self.timeout = timeout
        self._route_map = {}
        self._build_routing_table()

    def _build_routing_table(self):
        """
        @desc: 타입 기반 라우팅 테이블 구축.
        from __future__ import annotations 로 인한 문자열 타입 힌팅을 
        실제 객체로 해석하여 매핑합니다.
        """
        # 자식 클래스가 선언된 모듈의 네임스페이스를 가져옴 (타입 힌트 해석용)
        try:
            globalns = sys.modules[self.__module__].__dict__
        except KeyError:
            globalns = {}

        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, "__step_config"):
                sig = inspect.signature(func)
                params = list(sig.parameters.values())
                
                if len(params) >= 2:
                    param_name = params[1].name
                    
                    # typing.get_type_hints를 통해 지연 평가된 문자열 힌트를 실제 클래스로 해독
                    try:
                        hints = typing.get_type_hints(func, globalns=globalns)
                        annotation = hints.get(param_name, params[1].annotation)
                    except Exception as e:
                        # 해석 실패 시 fallback
                        annotation = params[1].annotation
                    
                    origin = typing.get_origin(annotation)
                    expected_types = typing.get_args(annotation) if origin in (typing.Union, types.UnionType) else (annotation,)
                    
                    for ext_type in expected_types:
                        # 오직 WorkflowMessage 하위 타입(실제 클래스 객체)만 라우팅 테이블에 등록
                        if isinstance(ext_type, type) and issubclass(ext_type, WorkflowMessage):
                            self._route_map[ext_type] = getattr(self, name)

    def preprocess_message(self, message: WorkflowMessage) -> WorkflowMessage:
        """
        @desc: [Hook] 메시지 전처리용 확장 지점.
        """
        return message

    async def _dispatch_message(self, message: Message):
        """
        @desc: GanNode의 핵심 디스패처를 오버라이드하여 메시지 성격에 따라 라우팅 방식을 분기합니다.
        """
        if isinstance(message, WorkflowMessage):
            # Workflow 정의 메시지: Type-based 라우팅
            await self._process_workflow_message(message)
        else:
            # 일반 Actor 메시지: 기존 GanNode의 Name-based 라우팅 (on_<name>)
            await super()._dispatch_message(message)

    async def _process_workflow_message(self, message: WorkflowMessage):
        """
        @desc: WorkflowMessage 전용 처리 로직.
        전처리 -> 타입 라우팅 -> 다음 스텝 큐잉의 흐름을 가집니다.
        """
        message = self.preprocess_message(message)

        if isinstance(message, StopMessage):
            log.info(f"[Workflow] Reached StopMessage. Final Result: {getattr(message, 'result', None)}")
            self.stop()
            return

        next_step = self._find_step_for_message(message)
        if not next_step:
            log.debug(f"[Workflow] No step found for {type(message).__name__}. Ignored.")
            return

        try:
            result_msg = await asyncio.wait_for(next_step(message), timeout=self.timeout)
            if result_msg and isinstance(result_msg, Message):
                self.post_message(result_msg)
                
        except asyncio.TimeoutError:
            log.error(f"[Workflow] Step timed out after {self.timeout}s processing {type(message).__name__}.")
            self.post_message(ErrorMessage(msg="Step timeout"))

    def _find_step_for_message(self, message: WorkflowMessage):
        msg_type = type(message)
        # 1차: 정확한 타입 매칭
        if msg_type in self._route_map:
            return self._route_map[msg_type]
            
        # 2차: 서브클래스 매칭
        for expected_type, method in self._route_map.items():
            if isinstance(message, expected_type):
                return method
        return None