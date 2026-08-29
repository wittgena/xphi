# xphi.watcher.plane.emitter
## @lineage: watcher.plane.emitter
"""@flow: Context -> Event -> Control -> Projection"""
import logging
import os
import sys
import traceback
from typing import Any, Dict, Optional, Callable, List
from contextvars import ContextVar
from contextlib import contextmanager

from xphi.arch.event.next import LogEvent, next_id
from xphi.watcher.plane.regulator import default_plane

_flow_context: ContextVar[Dict[str, Any]] = ContextVar("flow_context", default={})
_event_interceptors: List[Callable[[LogEvent], None]] = []

# =========================================================================
# 🎚️ Log Level & State Management
# =========================================================================
_GLOBAL_MIN_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

_LEVEL_WEIGHTS = {
    "TRACE": 10,
    "DEBUG": 20,
    "INFO": 30,
    "WARN": 40,
    "WARNING": 40,
    "ERROR": 50,
    "CRIT": 60,
    "CRITICAL": 60,
    "SIGNAL": 70
}

def set_log_level(level: str):
    """
    동적으로 시스템 전체의 SurfaceEmitter 및 Native Logger의 로그 출력 임계치를 조정합니다.
    (벤치마크나 런타임 디버깅 시 코어 로그를 일시적으로 억제하거나 활성화하는 데 사용)
    """
    global _GLOBAL_MIN_LEVEL
    _GLOBAL_MIN_LEVEL = level.upper()
    
    # Native Python Logging Root Logger 호환성 동기화
    native_level = getattr(logging, _GLOBAL_MIN_LEVEL, logging.INFO)
    logging.getLogger().setLevel(native_level)

def register_interceptor(interceptor: Callable[[LogEvent], None]):
    """Registers an external interceptor to hook and extend LogEvents."""
    if interceptor not in _event_interceptors:
        _event_interceptors.append(interceptor)

@contextmanager
def flow_scope(auto_flush=False, **kwargs):
    token = _flow_context.set({**_flow_context.get(), **kwargs})
    try:
        yield _flow_context.get()
    finally:
        if auto_flush:
            default_plane.flush()
        _flow_context.reset(token)


# =========================================================================
# 📡 Surface Event Emitter
# =========================================================================
class SurfaceEmitter:
    def __init__(
        self, 
        name: str, 
        phase: Optional[str] = None, 
        boundary: Optional[str] = None,
        handler: Optional[Callable[[LogEvent], None]] = None,
        mode: str = "NORMAL"
    ):
        self.name = name
        self.phase = phase
        self.bound = boundary
        self._handler = handler or default_plane.handle
        self.mode = mode.upper()

    def set_mode(self, mode: str):
        self.mode = mode.upper()
        return self

    def _format_msg(self, msg: str, *args) -> str:
        if args:
            try:
                return str(msg) % args
            except TypeError:
                return str(msg)
        return str(msg)

    def _log(self, level: str, msg: str, *args, **kwargs):
        # [Early Return 최적화] 
        # 설정된 전역 레벨보다 낮으면 무거운 Context 추출, ID 생성, 문자열 포맷팅을 모두 생략함
        current_weight = _LEVEL_WEIGHTS.get(level.upper(), 30)
        min_weight = _LEVEL_WEIGHTS.get(_GLOBAL_MIN_LEVEL, 30)
        if current_weight < min_weight:
            return

        ctx = _flow_context.get()
        exc_info = kwargs.pop("exc_info", None)
        formatted_msg = self._format_msg(msg, *args)

        if exc_info:
            formatted_msg += "\n" + traceback.format_exc()
        
        # [flow_id 제어] 컨텍스트에 flow_id가 없다면 단독 루트 스팬으로 간주하여 자동 부여
        flow_id = ctx.get("flow_id")
        if not flow_id:
            flow_id = next_id()
        
        unified_context = {
            "flow_id": flow_id,
            "phase": self.phase or ctx.get("phase"),
            "bound": self.bound or ctx.get("bound"),
            **ctx.get("extra", {}),
            **kwargs
        }

        # event_id는 LogEvent의 default_factory(next_id)에 의해 이 시점에 자동 생성됨
        event = LogEvent(
            source_id=self.name,
            message=formatted_msg,
            level=level.upper(),
            context=unified_context,
            parent_id=ctx.get("parent_id")
        )

        for interceptor in _event_interceptors:
            try:
                interceptor(event)
            except Exception:
                pass

        self._handler(event)

    def debug(self, msg, *args, **kwargs): self._log("DEBUG", msg, *args, **kwargs)
    def trace(self, msg, *args, **kwargs): self._log("TRACE", msg, *args, **kwargs)
    def info(self, msg, *args, **kwargs): self._log("INFO", msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs): self._log("WARN", msg, *args, **kwargs)
    def warn(self, msg, *args, **kwargs): self.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs): self._log("ERROR", msg, *args, **kwargs)
    def critical(self, msg, *args, **kwargs): self._log("CRIT", msg, *args, **kwargs)
    def crit(self, msg, *args, **kwargs): self.critical(msg, *args, **kwargs)
    
    def exception(self, msg, *args, **kwargs):
        kwargs["exc_info"] = True
        self._log("ERROR", msg, *args, **kwargs)

    def signal(self, msg, *args, **kwargs): 
        self._log("SIGNAL", msg, *args, **kwargs)

    def flush(self):
        default_plane.flush()

def get_emitter(name: str, phase: Optional[str] = None, boundary: Optional[str] = None, mode: str = "NORMAL") -> SurfaceEmitter:
    return SurfaceEmitter(name, phase, boundary, mode=mode)


# =========================================================================
# 🔌 @legacy.compat: Native Python logging fallback setup
# =========================================================================
DEBUG = True

class SurfacePlaneHandler(logging.Handler):
    """
    @desc: Intercepts standard logging.Logger records and securely routes them to the new event pipeline (SurfacePlane)
    """
    def emit(self, record):
        try:
            ## @step.1: Map standard logging levels to Surface architecture topology
            level_map = {
                logging.CRITICAL: "CRIT",
                logging.ERROR: "ERROR",
                logging.WARNING: "WARN",
                logging.INFO: "INFO",
                logging.DEBUG: "DEBUG",
                logging.NOTSET: "TRACE"
            }
            mapped_level = level_map.get(record.levelno, "INFO")
            
            # 여기서도 전역 레벨 필터링을 한 번 더 수행하여 불필요한 포맷팅 방지
            current_weight = _LEVEL_WEIGHTS.get(mapped_level, 30)
            min_weight = _LEVEL_WEIGHTS.get(_GLOBAL_MIN_LEVEL, 30)
            if current_weight < min_weight:
                return
            
            ## @step.2: Format raw record payload
            formatted_msg = self.format(record)
            
            ## @step.3: Assemble standard LogEvent and dispatch to Plane
            ## @notice: record.name equals the registered logger namespace (e.g., "bound")
            event = LogEvent(
                source_id=record.name,
                message=formatted_msg,
                level=mapped_level,
                context={"phase": "LEGACY"},
                parent_id=None
            )
            default_plane.handle(event)
        except Exception as e:
            sys.stderr.write(f"[SurfacePlaneHandler Anomaly] {e}\n")

def _create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
        
    # 변경됨: _LEVEL 대신 _GLOBAL_MIN_LEVEL을 동적으로 참조하여 호환성 강화
    level_num = getattr(logging, _GLOBAL_MIN_LEVEL, logging.INFO)
    logger.setLevel(level_num)
    handler = SurfacePlaneHandler()
    
    formatter = logging.Formatter("%(message)s") 
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def get_logger(name: str = "bound") -> logging.Logger:
    return _create_logger(name)