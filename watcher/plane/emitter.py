# watcher.plane.emitter
"""@flow: Context -> Event -> Control -> Projection"""
import logging
import os
import sys
import traceback
from typing import Any, Dict, Optional, Callable, List
from contextvars import ContextVar
from contextlib import contextmanager
from arch.contract.event.next import LogEvent
from watcher.plane.surface import default_plane

_flow_context: ContextVar[Dict[str, Any]] = ContextVar("flow_context", default={})
_event_interceptors: List[Callable[[LogEvent], None]] = []

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
        """Dynamically override output layout (NORMAL, SLIM, MINIMAL, FULL, etc.)"""
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
        ctx = _flow_context.get()
        exc_info = kwargs.pop("exc_info", None)
        formatted_msg = self._format_msg(msg, *args)

        if exc_info:
            formatted_msg += "\n" + traceback.format_exc()
        
        unified_context = {
            "flow_id": ctx.get("flow_id"),
            "phase": self.phase or ctx.get("phase"),
            "bound": self.bound or ctx.get("bound"),
            **ctx.get("extra", {}),
            **kwargs
        }

        event = LogEvent(
            source_id=self.name,
            message=formatted_msg,
            level=level,
            context=unified_context,
            parent_id=ctx.get("parent_id")
        )

        for interceptor in _event_interceptors:
            try:
                interceptor(event)
            except Exception:
                pass

        self._handler(event)

    ## @compat: Fully compatible adapter interface for standard logging.Logger
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


## @legacy.compat: Native Python logging fallback setup
_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
DEBUG = True

class SurfacePlaneHandler(logging.Handler):
    """@desc: Intercepts standard logging.Logger records and securely routes them to the new event pipeline (SurfacePlane)"""
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
            
            ## @step.2: Format raw record payload
            formatted_msg = self.format(record)
            
            ## @step.3: Assemble standard LogEvent and dispatch to Plane
            ## @notice: record.name equals the registered logger namespace (e.g., "bound")
            event = LogEvent(
                source_id=record.name,
                message=formatted_msg,
                level=mapped_level,
                context={"phase": "LEGACY"},
                tick=None
            )
            default_plane.handle(event)
        except Exception as e:
            sys.stderr.write(f"[SurfacePlaneHandler Anomaly] {e}\n")

def _create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
        
    level_num = getattr(logging, _LEVEL, logging.INFO)
    logger.setLevel(level_num)
    handler = SurfacePlaneHandler()
    
    formatter = logging.Formatter("%(message)s") 
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def get_logger(name: str = "bound") -> logging.Logger:
    return _create_logger(name)
