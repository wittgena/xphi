# xphi.watcher.plane.emitter
"""@flow: Context -> Event -> Control -> Projection"""
import logging
import os
import sys
import traceback
from typing import Any, Dict, Optional, Callable, List
from contextvars import ContextVar
from contextlib import contextmanager

from xphi.arch.bound.event.next import LogEvent, next_id
from xphi.watcher.plane.regulator import default_plane

_flow_context: ContextVar[Dict[str, Any]] = ContextVar("flow_context", default={})
_event_interceptors: List[Callable[[LogEvent], None]] = []

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
    """런타임에 전역 로그 레벨을 동적으로 변경합니다."""
    global _GLOBAL_MIN_LEVEL
    _GLOBAL_MIN_LEVEL = level.upper()
    
    # Python Native Logging 레벨도 동기화
    native_level = getattr(logging, _GLOBAL_MIN_LEVEL, logging.INFO)
    logging.getLogger().setLevel(native_level)
    
    # 모든 기존 로거들의 핸들러 레벨도 강제 업데이트 (오버라이드 방지)
    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.setLevel(native_level)

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
        current_weight = _LEVEL_WEIGHTS.get(level.upper(), 30)
        min_weight = _LEVEL_WEIGHTS.get(_GLOBAL_MIN_LEVEL, 30)
        
        # [필터링] 현재 로그의 가중치가 전역 설정보다 낮으면 즉시 무시 (성능 최적화)
        if current_weight < min_weight:
            return

        ctx = _flow_context.get()
        exc_info = kwargs.pop("exc_info", None)
        formatted_msg = self._format_msg(msg, *args)

        if exc_info:
            formatted_msg += "\n" + traceback.format_exc()
        
        flow_id = ctx.get("flow_id")
        if not flow_id:
            flow_id = next_id()
        
        unified_context = {
            "flow_id": flow_id,
            "phase": self.phase or ctx.get("phase"),
            "bound": self.bound or ctx.get("bound"),
            "mode": self.mode if self.mode != "NORMAL" else ctx.get("mode", "NORMAL"),
            **ctx.get("extra", {}),
            **kwargs
        }

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


"""@legacy.compat: Native Python logging fallback setup"""
# [수정] 혼란을 야기하는 전역 DEBUG = True 하드코딩 제거 (환경변수로 통일)
DEBUG = _GLOBAL_MIN_LEVEL == "DEBUG"

class SurfacePlaneHandler(logging.Handler):
    """
    @desc: Intercepts standard logging.Logger records and securely routes them to the new event pipeline (SurfacePlane)
    """
    def emit(self, record):
        try:
            level_map = {
                logging.CRITICAL: "CRIT",
                logging.ERROR: "ERROR",
                logging.WARNING: "WARN",
                logging.INFO: "INFO",
                logging.DEBUG: "DEBUG",
                logging.NOTSET: "TRACE"
            }
            mapped_level = level_map.get(record.levelno, "INFO")
            current_weight = _LEVEL_WEIGHTS.get(mapped_level, 30)
            min_weight = _LEVEL_WEIGHTS.get(_GLOBAL_MIN_LEVEL, 30)
            
            # [필터링] 레거시 로깅에서도 전역 레벨 검사
            if current_weight < min_weight:
                return
            
            formatted_msg = self.format(record)
            
            event = LogEvent(
                source_id=record.name,
                message=formatted_msg,
                level=mapped_level,
                # [수정] NameError를 유발하던 current_mode 변수 제거
                context={"phase": "LEGACY", "mode": "NORMAL"}, 
                parent_id=None
            )
            default_plane.handle(event)
        except Exception as e:
            sys.stderr.write(f"[SurfacePlaneHandler Anomaly] {e}\n")

def _create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
        
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