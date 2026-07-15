# watcher.plane.surface
"""
@manifold: Surface Event Projection Plane
@desc: Manages multi-surface log event distribution (Console, File, Tunnel) 
       with granular independent level filtering and dynamic telemetry folding.
       Bulletproofed against NoneType anomalies and frozen dataclasses.
"""
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
from watcher.plane.meter import default_telemetry

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...

class TunnelSurface(EventObserver):
    """@desc: Streams log events in real-time via the Universal Infrastructure Tunnel"""
    def __init__(self):
        pass

    def update(self, event: LogEvent):
        flow_id = event.context.get("flow_id") or "global"
        if flow_id == "global":
            return

        channel = f"log:{flow_id}"
        msg = json.dumps(asdict(event), ensure_ascii=False)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._async_publish(channel, msg))
        except RuntimeError:
            pass
            
    async def _async_publish(self, channel: str, msg: str):
        from arch.topos.bound.sandbox.tunnel import TunnelFactory
        try:
            tunnel = await TunnelFactory.get_default()
            await tunnel.publish(channel, msg)
        except Exception as e:
            sys.stderr.write(f"[TunnelSurface Anomaly] {e}\n")

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

class SurfacePlane:
    """@desc: Instance-based event collector and routing orchestrator."""
    PRIORITY_LEVELS = {"CRIT", "SIGNAL"}

    def __init__(self, threshold: float = 5.0, telemetry_engine=None):
        # [개선] PressureMeter를 직접 생성하지 않고 TelemetryEngine 주입
        self.telemetry = telemetry_engine or default_telemetry
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
        
        ## @step.1: Analyze multi-dimensional telemetry (Density & Kinematics)
        # [개선] 단순 밀도 조회가 아닌 다차원 위협 분석 수행
        tel_result = self.telemetry.analyze(key)
        density_val = tel_result.get("density", 0.0)
        is_bursting = tel_result.get("is_bursting", False)
        metrics = tel_result.get("metrics", {})

        ## @step.2: Fold repeated events securely
        # [개선] 임계치 초과 또는 가속도 기반 폭주(Burst) 감지 시 폴딩 발동
        if density_val > self.threshold or is_bursting:
            # 방어 강도(Gain) 설정 - 폭주 상태면 더 강하게 억제(0.1), 아니면 밀도 비례
            gain_val = 0.1 if is_bursting else self.threshold / (density_val or 1)
            
            # 이벤트 컨텍스트에 텔레메트리 흔적 남기기
            enriched_context = {**(event.context or {}), **metrics}

            if key in self.fold_cache:
                old_event = self.fold_cache[key]
                new_fold_count = getattr(old_event, "fold_count", 1) + 1
                self.fold_cache[key] = replace(
                    old_event, 
                    fold_count=new_fold_count, 
                    gain=gain_val,
                    context=enriched_context
                )
                return
                
            summary_event = replace(
                event, 
                kind="summary", 
                fold_count=1, 
                density=density_val, 
                gain=gain_val,
                context=enriched_context
            )
            self.fold_cache[key] = summary_event
            self._notify(summary_event)
        else:
            if key in self.fold_cache:
                folded_event = self.fold_cache.pop(key)
                if getattr(folded_event, "fold_count", 0) > 1:
                    self._notify(folded_event)
            
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
default_plane = SurfacePlane(telemetry_engine=default_telemetry)
console_surface = ConsoleSurface(
    mode="NORMAL", 
    min_level=os.environ.get("LOG_LEVEL", "INFO").upper()
)

def _assemble_collector_network():
    """@desc: 내부 조립 부트스트래퍼."""
    global_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    ## Console 부착
    default_plane.attach(console_surface)

    ## File Surface 동적 라우팅 및 부착 (이 변수들은 밖으로 새어나가지 않습니다)
    log_base_dir = resolve_path("log")
    file_min_level = "TRACE" if global_log_level == "TRACE" else "DEBUG"
    file_surface = FileSurface(base_dir=log_base_dir, min_level=file_min_level)
    default_plane.attach(file_surface)

    tunnel_streamer = TunnelSurface()
    default_plane.attach(tunnel_streamer)
    atexit.register(default_plane.flush)

_assemble_collector_network()