# watcher.plane.surface
"""
@manifold: Surface Event Projection Plane
@desc: Manages multi-surface log event distribution (Console, File, Redis) 
       with granular independent level filtering and folding control.
       Bulletproofed against NoneType formatting anomalies.
"""
import json
import time
import asyncio
import sys
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
    """로그 이벤트를 Redis Pub/Sub으로 실시간 스트리밍"""
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
    """콘솔 출력을 담당하며, 지정된 기본 최소 레벨 이상만 필터링하여 출력합니다."""
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
        # 💡 None 유형 방어 가드 문자열 정형화
        event_level = str(event.level or "INFO").upper()
        
        if event_level not in self.BYPASS_LEVELS:
            current_weight = self.LEVEL_WEIGHTS.get(event_level, 30)
            if current_weight < self.min_weight:
                return

        if self.mode == "FULL":
            print(f"DEBUG_EVENT: {event}")
            return

        p_mark = "🔥" if event.kind == "summary" else ""
        gain = f" [G:{event.gain:.1f}]" if event.gain is not None and event.gain < 1.0 else ""
        fold = f" (x{event.fold_count})" if event.fold_count and event.fold_count > 1 else ""
        
        # 💡 안전한 문자열 치환 가드 정렬 (None 유입 시 공백/기본값 대체)
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
                prefix = f"T-{int(event.tick):04d}" if event.tick is not None else f"{kind_str:^5}"
            except (ValueError, TypeError):
                prefix = f"{kind_str:^5}"
                
            print(f"{prefix}{p_mark}| {phase_str:^6} | {event_level:^5} | {gain} {source_str}: {event.message}{fold}")


class FileSurface(EventObserver):
    """로그 이벤트를 파일 시스템에 지속적으로 기록하는 물리 옵저버"""
    def __init__(self, file_path: str | Path, min_level: str = "DEBUG"):
        self.file_path = Path(file_path)
        self.min_level = min_level.upper()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.min_weight = ConsoleSurface.LEVEL_WEIGHTS.get(self.min_level, 20)

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        current_weight = ConsoleSurface.LEVEL_WEIGHTS.get(event_level, 30)
        
        if event_level not in ConsoleSurface.BYPASS_LEVELS and current_weight < self.min_weight:
            return

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        
        # 💡 키가 존재하되 내부가 None일 경우를 완벽하게 가드하기 위해 인라인 삼항 연산 사용
        ctx_phase = event.context.get("phase") if event.context else None
        phase_str = str(ctx_phase if ctx_phase is not None else "SYSTEM")
        source_str = str(event.source_id or "UNKNOWN")
        fold = f" (x{event.fold_count})" if event.fold_count and event.fold_count > 1 else ""
        
        log_line = f"[{timestamp}] [{event_level:^5}] [{phase_str:^8}] {source_str}: {event.message}{fold}\n"
        
        try:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            sys.stderr.write(f"[FileSurface Error] Failed to write log grid: {e}\n")


class PressureMeter:
    """슬라이딩 윈도우 기반 압력 측정"""
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
    """인스턴스 기반의 이벤트 수집 및 라우팅 총괄 본체"""
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
        
        event.density = self.meter.measure(key)

        if event.density > self.threshold:
            event.gain = self.threshold / event.density
            
            if key in self.fold_cache:
                self.fold_cache[key].fold_count += 1
                return
                
            summary_event = replace(event, kind="summary", fold_count=1)
            self.fold_cache[key] = summary_event
            self._notify(summary_event)
        else:
            if key in self.fold_cache:
                folded_event = self.fold_cache.pop(key)
                if folded_event.fold_count > 1:
                    self._notify(folded_event)
            
            self._notify(event)

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
            if folded_event and folded_event.fold_count > 1:
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


"""⚙️ 에코시스템 수집망 어댑터 초기 조립 배포 영역"""
default_plane = SurfacePlane()
console_surface = ConsoleSurface(mode="NORMAL", min_level="INFO")
default_plane.attach(console_surface)

LOG_OUTPUT_TARGET = resolve_path("res") / "log" / "brane.log"
file_surface = FileSurface(file_path=LOG_OUTPUT_TARGET, min_level="DEBUG")
default_plane.attach(file_surface)

if redis_async:
    try:
        redis_streamer = RedisSurface(redis_async.from_url("redis://localhost:6379", decode_responses=True))
        default_plane.attach(redis_streamer)
    except Exception as e:
        sys.stderr.write(f"[Redis Gateway Silent Bypass] {e}\n")