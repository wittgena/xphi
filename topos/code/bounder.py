# reflect.topos.bounder 
"""
@flow:
Ψ(target)
 -> Φ(signature)
 -> ∂Φ (reflective injection)
 -> rupture
 -> traces + snapshot
@role: boundary tracer (non-execution)
@note: exposes structure via controlled failure
"""
import json
import inspect
import sys
from typing import Callable, Any, Dict, List, Optional

class TraceReflector:
    """@flow: access → ∂Φ trace"""
    def __init__(self, trace_log: List[str], path: str = "root"):
        self._trace_log = trace_log
        self._path = path

    def _log_and_rupture(self, action: str):
        current_path = f"{self._path} -> {action}"
        self._trace_log.append(current_path)
        raise RuntimeError(f"Boundary Rupture: {current_path}")

    def __getattr__(self, name): self._log_and_rupture(f"getattr(.{name})")
    def __call__(self, *args, **kwargs): self._log_and_rupture("call()")
    def __getitem__(self, key): self._log_and_rupture(f"getitem([{key}])")
    def __iter__(self): self._log_and_rupture("iter()")

class RuptureSnapshot:
    """@flow: exception → stack + locals"""
    @staticmethod
    def capture(e: Exception) -> Dict[str, Any]:
        ## extract stack frames excluding binder
        snapshot = {"error": f"{type(e).__name__}: {str(e)}", "stack": []}
        tb = e.__traceback__
        while tb:
            frame = tb.tb_frame
            if "tool/binder" not in frame.f_code.co_filename:
                snapshot["stack"].append({
                    "function": frame.f_code.co_name,
                    "line": tb.tb_lineno,
                    "locals": {k: str(v)[:60] for k, v in frame.f_locals.items() if not k.startswith("__")}
                })
            tb = tb.tb_next
        return snapshot

class ToposBounder:
    """@flow: Φ → ∂Φ → rupture → echoes"""
    @staticmethod
    def strike(target: Callable) -> Dict[str, Any]:
        echoes = {"signature": None, "traces": [], "behavioral_map": {}}
        
        ## extract callable signature (Φ)
        try:
            sig = inspect.signature(target)
            echoes["signature"] = str(sig)
        except (ValueError, TypeError) as e:
            echoes["traces"].append(f"[SigFail] {e}")
            return echoes

        ## inject reflectors to induce ∂Φ
        access_log = []
        try:
            args, kwargs = [], {}
            for name, param in sig.parameters.items():
                reflector = TraceReflector(access_log, path=f"param({name})")
                if param.kind == inspect.Parameter.KEYWORD_ONLY:
                    kwargs[name] = reflector
                else:
                    args.append(reflector)
            
            ## trigger controlled rupture
            target(*args, **kwargs)
        except Exception as e:
            echoes["traces"].append(f"[Rupture] {type(e).__name__}")
            echoes["behavioral_map"] = {
                "access_path": access_log,
                "snapshot": RuptureSnapshot.capture(e)
            }
            
        return echoes