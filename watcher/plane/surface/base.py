# watcher.plane.surface.base
## @lineage: watcher.plane.observer.surface
import os
import json
import time
import asyncio
import sys
import atexit
from dataclasses import replace, asdict
from typing import Dict, List, Protocol, Optional
from pathlib import Path
from arch.contract.event.next import LogEvent
from phase.bind.resolver import resolve_path
from watcher.plane.observer.event import EventObserver

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
        
        acc_val = event.context.get("acceleration") if event.context else None
        acc_str = f" [Acc:{acc_val:.1f}]" if acc_val else ""
        
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
                
            print(f"{prefix}{p_mark}| {phase_str:^6} | {event_level:^5} | {gain}{acc_str} {source_str}: {event.message}{fold}")

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
        
        ctx_phase = event.context.get("phase") if event.context else None
        phase_str = str(ctx_phase if ctx_phase is not None else "SYSTEM")
        
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