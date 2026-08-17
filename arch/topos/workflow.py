# arch.topos.workflow
"""
@desc: Metaclass-driven state machine that dynamically routes strongly-typed messages through defined topological steps
"""
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
    """@desc: Standardized data carrier for transitioning workflow states intent"""
    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__)
        for k, v in kwargs.items():
            setattr(self, k, v)

class StopMessage(WorkflowMessage):
    """@desc: Terminal message instructing the workflow engine to halt execution and return the final result intent"""
    def __init__(self, result=None, **kwargs):
        super().__init__(**kwargs)
        self.result = result

class ErrorMessage(WorkflowMessage):
    """@desc: Dedicated carrier for propagating internal step failures to error boundaries intent"""
    def __init__(self, msg: str = "", **kwargs):
        super().__init__(**kwargs)
        self.msg = msg

def step(func):
    """@desc: Decorator marking class methods as routable topological steps intent"""
    func.__step_config = True
    return func

class WorkflowMeta(type):
    """@desc: Metaclass injecting automatic message routing and structural exception handling into workflow steps intent"""
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
                    log.error(f"[Workflow] 💥 Exception in '{_method_name}' {e}", exc_info=True)
                    return ErrorMessage(msg=str(e))

                if not isinstance(result_msg, WorkflowMessage):
                    return result_msg

                status = getattr(result_msg, "status", None)
                if status and status in trans_rules:
                    target_msg_cls = trans_rules[status]
                    log.warning(f"[Workflow] Trans rule triggered for status '{status}' -> '{target_msg_cls.__name__}'")
                    return target_msg_cls(msg=getattr(result_msg, "msg", ""))

                return result_msg
            
            setattr(cls, method_name, wrapper)
        return cls

class Workflow(GanNode, metaclass=WorkflowMeta):
    """@desc: Autonomous state machine execution layer extending the base GanNode topology intent"""
    def __init__(self, name: str = "workflow", timeout: float = 600.0, **kwargs):
        super().__init__(name=name)
        self.timeout = timeout
        self._route_map = {}
        self._build_routing_table()

    def _build_routing_table(self):
        """@desc: Reflects upon step signatures to construct a deterministic type-to-method routing matrix intent"""
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
                    try:
                        hints = typing.get_type_hints(func, globalns=globalns)
                        annotation = hints.get(param_name, params[1].annotation)
                    except Exception:
                        ## @desc: Fallback to raw annotation if strict type hint resolution fails intent
                        annotation = params[1].annotation
                    
                    origin = typing.get_origin(annotation)
                    expected_types = typing.get_args(annotation) if origin in (typing.Union, types.UnionType) else (annotation,)
                    
                    for ext_type in expected_types:
                        ## @desc: Restrict routing table registration strictly to structural WorkflowMessage subclasses intent
                        if isinstance(ext_type, type) and issubclass(ext_type, WorkflowMessage):
                            self._route_map[ext_type] = getattr(self, name)

    def preprocess_message(self, message: WorkflowMessage) -> WorkflowMessage:
        return message

    async def _dispatch_message(self, message: Message):
        """@desc: Overrides base dispatcher to intercept typed workflow transitions before standard handling intent"""
        if isinstance(message, WorkflowMessage):
            await self._process_workflow_message(message)
        else:
            await super()._dispatch_message(message)

    async def _process_workflow_message(self, message: WorkflowMessage):
        """@desc: Executes the target step with strict isolation and temporal bounding intent"""
        message = self.preprocess_message(message)
        if isinstance(message, StopMessage):
            log.info(f"[Workflow] Reached StopMessage Final Result: {getattr(message, 'result', None)}")
            self.stop()
            return

        next_step = self._find_step_for_message(message)
        if not next_step:
            log.debug(f"[Workflow] No step found for {type(message).__name__} Ignored")
            return

        try:
            ## @desc: Protect system from infinite state deadlocks via asyncio wait_for intent
            result_msg = await asyncio.wait_for(next_step(message), timeout=self.timeout)
            if result_msg and isinstance(result_msg, Message):
                self.post_message(result_msg)
                
        except asyncio.TimeoutError:
            log.error(f"[Workflow] Step timed out after {self.timeout}s processing {type(message).__name__}")
            self.post_message(ErrorMessage(msg="Step timeout"))

    def _find_step_for_message(self, message: WorkflowMessage):
        """@desc: Resolves the appropriate transition handler for a given structural message intent"""
        msg_type = type(message)
        if msg_type in self._route_map:
            return self._route_map[msg_type]
            
        for expected_type, method in self._route_map.items():
            if isinstance(message, expected_type):
                return method
        return None