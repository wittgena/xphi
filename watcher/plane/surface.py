# watcher.plane.surface
"""
@manifold: Surface Event Projection Plane
@desc: Manages multi-surface log event distribution (Console, File, Redis) 
       with granular independent level filtering and folding control.
       Bulletproofed against NoneType anomalies and frozen dataclasses.
       Dynamically routes file logs based on execution phases.
"""
import os
import json
import time
import asyncio
import sys
import atexit
from dataclasses import replace, asdict
from typing import Dict, List, Protocol, Optional
from collections import defaultdict, deque
from pathlib import Path

from arch.proto.event.next import LogEvent
from phase.bind.resolver import resolve_path

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...

class RedisSurface(EventObserver):
    """@desc: Streams log events in real-time via Redis Pub/Sub."""
    def __init__(self, redis_client):
        self.redis = redis_client

    def update(self, event: LogEvent):
        if not self.redis:
            return
            
        flow_id = event.context.get("flow_id") or "global"
        if flow_id == "global":
            return

        channel = f"log:{flow_id}"
        msg = json.dumps(asdict(event), ensure_ascii=False)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.redis.publish(channel, msg))
        except RuntimeError:
            pass

class ConsoleSurface(EventObserver):
    """@desc: Handles standard output with level-based filtering and rich formatting."""
    BYPASS_LEVELS = {"CRIT", "SIGNAL"}
    
    LEVEL_WEIGHTS = {
        "TRACE": 10,
        "DEBUG": 20,
        "INFO": 30,
        "WARN": 40,
        "ERROR": 50,
        "CRIT": 60,
        "SIGNAL": 70
    }

    def __init__(self, mode: str = "NORMAL", min_level: str = "INFO"):
        self.mode = mode.upper()
        self.min_level = min_level.upper()
        self.min_weight = self.LEVEL_WEIGHTS.get(self.min_level, 30)

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        
        if event_level not in self.BYPASS_LEVELS:
            current_weight = self.LEVEL_WEIGHTS.get(event_level, 30)
            if current_weight < self.min_weight:
                return

        if self.mode == "FULL":
            print(f"DEBUG_EVENT: {event}")
            return

        p_mark = "🔥" if event.kind == "summary" else ""
        gain_val = getattr(event, "gain", None)
        gain = f" [G:{gain_val:.1f}]" if gain_val is not None and gain_val < 1.0 else ""
        
        fold_val = getattr(event, "fold_count", 0)
        fold = f" (x{fold_val})" if fold_val and fold_val > 1 else ""
        
        phase_val = event.context.get("phase") if event.context else None
        phase_str = str(phase_val if phase_val is not None else "SYSTEM")
        kind_str = str(event.kind or "LOG").upper()
        source_str = str(event.source_id or "UNKNOWN")

        if self.mode == "SLIM":
            print(f"[{event_level:^5}] {source_str}: {event.message}{fold}")
        elif self.mode == "MINIMAL":
            print(f"{event.message}{fold}")
        else:
            try:
                prefix = f"T-{int(event.tick):04d}" if getattr(event, "tick", None) is not None else f"{kind_str:^5}"
            except (ValueError, TypeError):
                prefix = f"{kind_str:^5}"
                
            print(f"{prefix}{p_mark}| {phase_str:^6} | {event_level:^5} | {gain} {source_str}: {event.message}{fold}")

class FileSurface(EventObserver):
    """
    @desc: 
    - Physically records log events to the filesystem
    - Dynamically routes to different files based on the 'phase' context
    """
    def __init__(self, base_dir: str | Path, min_level: str = "DEBUG"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.min_level = min_level.upper()
        self.min_weight = ConsoleSurface.LEVEL_WEIGHTS.get(self.min_level, 20)

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        current_weight = ConsoleSurface.LEVEL_WEIGHTS.get(event_level, 30)
        
        if event_level not in ConsoleSurface.BYPASS_LEVELS and current_weight < self.min_weight:
            return

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        
        ## @step.1: Safely extract and sanitize phase for dynamic file routing
        ctx_phase = event.context.get("phase") if event.context else None
        phase_str = str(ctx_phase if ctx_phase is not None else "SYSTEM")
        
        ## Prevent path traversal and format cleanly (e.g., "SYSTEM" -> "system.log")
        safe_phase = "".join(c for c in phase_str if c.isalnum() or c in "_-").lower()
        if not safe_phase:
            safe_phase = "system"
            
        target_file = self.base_dir / f"{safe_phase}.log"
        
        source_str = str(event.source_id or "UNKNOWN")
        fold_val = getattr(event, "fold_count", 0)
        fold = f" (x{fold_val})" if fold_val and fold_val > 1 else ""
        
        log_line = f"[{timestamp}] [{event_level:^5}] [{phase_str:^8}] {source_str}: {event.message}{fold}\n"
        
        try:
            with open(target_file, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            sys.stderr.write(f"[FileSurface Error] Failed to write to {target_file.name}: {e}\n")


class PressureMeter:
    """@desc: Sliding window-based pressure and density measurement."""
    def __init__(self, window: float = 2.0):
        self.window = window
        self.history = defaultdict(deque)

    def measure(self, key: str) -> float:
        now = time.time()
        q = self.history[key]
        
        while q and now - q[0] > self.window:
            q.popleft()
            
        q.append(now)
        return len(q) / self.window

class SurfacePlane:
    """@desc: Instance-based event collector and routing orchestrator."""
    PRIORITY_LEVELS = {"CRIT", "SIGNAL"}

    def __init__(self, threshold: float = 5.0, meter_window: float = 2.0):
        self.meter = PressureMeter(window=meter_window)
        self.threshold = threshold
        self.fold_cache: Dict[str, LogEvent] = {}
        self._observers: List[EventObserver] = []

    def handle(self, event: LogEvent):
        if event.level in self.PRIORITY_LEVELS:
            self._notify(event)
            return

        phase_val = event.context.get("phase") if event.context else None
        phase_str = str(phase_val if phase_val is not None else "SYSTEM")
        source_str = str(event.source_id or "UNKNOWN")
        msg_str = str(event.message or "")
        key = f"{phase_str}:{source_str}:{msg_str}"
        
        ## @step.1: Safely calculate density
        density_val = self.meter.measure(key)

        ## @step.2: Fold repeated events securely (Bulletproofed against frozen Dataclasses)
        if density_val > self.threshold:
            gain_val = self.threshold / density_val
            
            if key in self.fold_cache:
                old_event = self.fold_cache[key]
                new_fold_count = getattr(old_event, "fold_count", 1) + 1
                self.fold_cache[key] = replace(old_event, fold_count=new_fold_count, gain=gain_val)
                return
                
            summary_event = replace(event, kind="summary", fold_count=1, density=density_val, gain=gain_val)
            self.fold_cache[key] = summary_event
            self._notify(summary_event)
        else:
            if key in self.fold_cache:
                folded_event = self.fold_cache.pop(key)
                if getattr(folded_event, "fold_count", 0) > 1:
                    self._notify(folded_event)
            
            # Forward the original event securely
            ready_event = replace(event, density=density_val)
            self._notify(ready_event)

    def _notify(self, event: LogEvent):
        for observer in self._observers:
            try:
                observer.update(event)
            except Exception as e:
                sys.stderr.write(f"[SurfacePlane-Notify Anomaly] {e}\n")
            
    def attach(self, observer: EventObserver):
        if observer not in self._observers:
            self._observers.append(observer)

    def flush(self):
        for key in list(self.fold_cache.keys()):
            folded_event = self.fold_cache.pop(key, None)
            if folded_event and getattr(folded_event, "fold_count", 0) > 1:
                self._notify(folded_event)

    def record(self, tick, phase, source, message, level="INFO"):
        event = LogEvent(
            source_id=str(source), 
            message=message, 
            level=level, 
            context={"phase": phase},
            tick=tick
        )
        self.handle(event)

"""Ecosystem Collector Network: Assembly & Deployment Region"""
## @step: Sync global log level from Environment Variables
GLOBAL_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

default_plane = SurfacePlane()
console_surface = ConsoleSurface(mode="NORMAL", min_level=GLOBAL_LOG_LEVEL)
default_plane.attach(console_surface)

## @step: Dynamic File Surface routing based on Target Directory
LOG_BASE_DIR = resolve_path("log")
file_min_level = "TRACE" if GLOBAL_LOG_LEVEL == "TRACE" else "DEBUG"

file_surface = FileSurface(base_dir=LOG_BASE_DIR, min_level=file_min_level)
default_plane.attach(file_surface)

## @step: Register forced flush on process exit to prevent fold-cache evaporation
atexit.register(default_plane.flush)
if redis_async:
    try:
        redis_streamer = RedisSurface(redis_async.from_url("redis://localhost:6379", decode_responses=True))
        default_plane.attach(redis_streamer)
    except Exception as e:
        sys.stderr.write(f"[Redis Gateway Silent Bypass] {e}\n")