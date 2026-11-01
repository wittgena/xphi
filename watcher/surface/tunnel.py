# xphi.watcher.surface.tunnel
import json
import time
import sys
import socket
import threading
import queue
from typing import Optional
from xphi.arch.contract.event.next import LogEvent, EventObserver

DAEMON_PORT: int = 8356
CONSOLE_PORT: int = 8357


def _is_control_plane_active( check_interval: float = 2.0, _cache: dict = {"ts": 0.0, "active": False} ) -> bool:
    """데몬(:8356) 또는 콘솔(:8357) 포트가 열려 있는지 2초 주기로 경량 체크합니다."""
    now = time.time()
    if now - _cache["ts"] < check_interval:
        return _cache["active"]

    active = False
    for port in (DAEMON_PORT, CONSOLE_PORT):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.05)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                active = True
                break

    _cache["ts"] = now
    _cache["active"] = active
    return active


class TunnelSurface(EventObserver):
    def __init__(self, max_batch_size: int = 50, flush_interval: float = 0.2):
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self._queue = queue.Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        # [수정 1] __init__ 시점의 무조건적인 스레드 기동(self._ensure_worker) 제거

    def _ensure_worker(self):
        with self._lock:
            if self._worker_thread is None or not self._worker_thread.is_alive():
                self._worker_thread = threading.Thread(
                    target=self._publish_worker,
                    daemon=True,
                    name="TunnelSurfaceWorker"
                )
                self._worker_thread.start()

    def _publish_worker(self):
        from xphi.node.space.tunnel.factory import TunnelFactory
        tunnel = None

        while True:
            try:
                # [수정 2] 큐에 실제 보낼 데이터가 들어올 때까지 TunnelFactory.get_sync() 호출 지연
                batch = []
                try:
                    channel, msg = self._queue.get(timeout=self.flush_interval)
                    batch.append((channel, msg))
                except queue.Empty:
                    continue

                if tunnel is None:
                    tunnel = TunnelFactory.get_sync()

                start_time = time.time()
                while len(batch) < self.max_batch_size:
                    elapsed = time.time() - start_time
                    remaining = self.flush_interval - elapsed
                    if remaining <= 0:
                        break

                    try:
                        ch, m = self._queue.get(timeout=remaining)
                        batch.append((ch, m))
                    except queue.Empty:
                        break

                if batch:
                    pipeline = tunnel.pipeline()
                    for ch, m in batch:
                        pipeline.publish(ch, m)
                    pipeline.execute()

                    for _ in range(len(batch)):
                        self._queue.task_done()

            except Exception as e:
                sys.stderr.write(f"[TunnelSurface Worker Anomaly] {e}\n")
                tunnel = None
                time.sleep(1.0)

    def update(self, event: LogEvent):
        if not _is_control_plane_active():
            return

        flow_id = event.context.get("flow_id") if event.context else None
        flow_id = flow_id or "global"

        if flow_id == "global":
            return

        channel = f"log:{flow_id}"
        event_dict = {k: v for k, v in event.__dict__.items()}
        msg = json.dumps(event_dict, ensure_ascii=False, default=str)
        self._ensure_worker()
        self._queue.put((channel, msg))