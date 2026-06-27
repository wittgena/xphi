# watcher.plane.surface
import json
import time
import asyncio
from dataclasses import replace, asdict
from typing import Dict, List, Protocol
from collections import defaultdict, deque
from arch.proto.event.next import LogEvent
import redis.asyncio as redis_async

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...

class RedisSurface(EventObserver):
    """로그 이벤트를 Redis Pub/Sub으로 실시간 스트리밍"""
    def __init__(self, redis_client):
        self.redis = redis_client

    def update(self, event: LogEvent):
        flow_id = event.context.get("flow_id") or "global"
        if flow_id == "global":
            return

        channel = f"log:{flow_id}"
        msg = json.dumps(asdict(event), ensure_ascii=False)

        if self.redis:
            try:
                ## 실행 중인 이벤트 루프가 있을 때만 안전하게 백그라운드 태스크로 발행
                loop = asyncio.get_running_loop()
                loop.create_task(self.redis.publish(channel, msg))
            except RuntimeError:
                ## 이벤트 루프가 없는 동기(Sync) 스레드에서 호출된 경우 크래시를 방지하기 위해 무시
                pass

class ConsoleSurface(EventObserver):
    """콘솔 출력을 담당하며 모드에 따라 포맷팅을 다르게 적용"""
    def __init__(self, mode: str = "NORMAL"):
        ## 모드: 'FULL' (전체 데이터), 'NORMAL' (현재 포맷), 'SLIM' (메시지 중심), 'MINIMAL'
        self.mode = mode.upper()

    def update(self, event: LogEvent):
        if self.mode == "FULL":
            print(f"DEBUG_EVENT: {event}")
            return

        p_mark = "🔥" if event.kind == "summary" else ""
        gain = f" [G:{event.gain:.1f}]" if event.gain < 1.0 else ""
        fold = f" (x{event.fold_count})" if event.fold_count > 1 else ""
        phase = event.context.get("phase")
        phase_str = phase if phase is not None else "SYSTEM"

        if self.mode == "SLIM":
            print(f"[{event.level:^5}] {event.source_id}: {event.message}{fold}")
        elif self.mode == "MINIMAL":
            print(f"{event.message}{fold}")
        else:
            prefix = f"T-{int(event.tick):04d}" if event.tick is not None else f"{event.kind.upper():^5}"
            print(f"{prefix}{p_mark}| {phase_str:^6} | {event.level:^5} | {gain} {event.source_id}: {event.message}{fold}")

class PressureMeter:
    """@control.plane: 슬라이딩 윈도우 기반 압력 측정"""
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
    """@projection: 인스턴스 기반의 이벤트 Plane"""
    PRIORITY_LEVELS = {"CRIT", "SIGNAL"}

    def __init__(self, threshold: float = 5.0, meter_window: float = 2.0):
        self.meter = PressureMeter(window=meter_window)
        self.threshold = threshold
        self.fold_cache: Dict[str, LogEvent] = {}
        self._observers: List[EventObserver] = []

    def handle(self, event: LogEvent):
        # Priority Bypass: 치명적 신호는 압력 측정 없이 즉시 방출
        if event.level in self.PRIORITY_LEVELS:
            self._notify(event)
            return

        ## Pressure Control (Folding Logic)
        phase = event.context.get("phase")
        phase_str = phase if phase is not None else "SYSTEM"
        key = f"{phase_str}:{event.source_id}:{event.message}"
        
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
            observer.update(event)
            
    def attach(self, observer: EventObserver):
        if observer not in self._observers:
            self._observers.append(observer)

    def flush(self):
        for key in list(self.fold_cache.keys()):
            folded_event = self.fold_cache.pop(key, None)
            if folded_event and folded_event.fold_count > 1:
                self._notify(folded_event)

    def record(self, tick, phase, source, message, level="INFO"):
        """Legacy entrypoint"""
        event = LogEvent(
            source_id=str(source), 
            message=message, 
            level=level, 
            context={"phase": phase},
            tick=tick
        )
        self.handle(event)

default_plane = SurfacePlane()
surface = ConsoleSurface(mode="NORMAL")
default_plane.attach(surface)

try:
    redis_streamer = RedisSurface(redis_async.from_url("redis://localhost:6379", decode_responses=True))
    default_plane.attach(redis_streamer)
except ImportError as e:
    print(f"Redis import error: {e}")
except Exception as e:
    print(f"Redis initialization error: {e}")