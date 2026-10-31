# xphi.state.phase.network.bootstrap
import asyncio
from pathlib import Path
from typing import Optional

from xphi.arch.xor.parser.ruleset.engine import StreamTaggingParser
from xphi.watcher.plane.tracer.base import SystemBound, log_streamer
from xphi.node.space.tunnel.surface import EchoListener
from xphi.watcher.plane.emitter import get_emitter

log_flow = get_emitter("phase.network.bootstrap")

KEY_STATE_PIDS = "system:xphi:pids"
PROCESS_NAME = "xphi-dev-node"

DEFAULT_WORKER_RULESET = {
    "targets": [
        {
            "tag": "worker-ready",
            "keywords": [
                {"AND": ["Netty started", "port"]},
                {"AND": ["Started", "seconds", "JVM"]},
                {"AND": ["Ready on", "http"]},  # 범용 HTTP 워커 호환성 추가
            ],
        },
        {
            "tag": "worker-fatal",
            "keywords": [
                {"AND": ["io.netty.util.internal.OutOfDirectMemoryError", "failed to allocate"]},
                {"AND": ["reactor.blockhound.BlockingOperationError", "Blocking call!"]},
            ],
        },
    ]
}


class WorkerRuntimeBootstrap:
    """Spawns and monitors a managed local binary worker via stdout ruleset sensing."""
    def __init__(
        self,
        bin_root: Path,
        mq_surface: Optional[EchoListener] = None,
        ruleset: Optional[dict] = None,
        executable_pattern: str = "phix-*.jar",
    ):
        self.bin_root = bin_root
        self.mq_surface = mq_surface
        self.process_name = PROCESS_NAME
        self.executable_pattern = executable_pattern

        self.boundary = SystemBound()
        self.executable_path = self._resolve_executable()
        self.ready_event = asyncio.Event()

        active_ruleset = ruleset or DEFAULT_WORKER_RULESET
        self.rule_engine = StreamTaggingParser(engine_type="regex").parse_ruleset(active_ruleset)

    def _resolve_executable(self) -> str:
        bins = sorted(self.bin_root.glob(self.executable_pattern))
        if not bins:
            raise RuntimeError(
                f"Worker execution core matching {self.executable_pattern} is missing from {self.bin_root}."
            )
        return str(bins[-1])

    @log_streamer([
        "bash", "-c",
        "exec -a {process_name} java -Dreaper.tag={process_name} -jar {executable_path}",
    ])
    async def _stream_and_sense(self, line: str):
        if not line:
            return

        matched_tags = self.rule_engine.execute(line)
        if "worker-ready" in matched_tags and not self.ready_event.is_set():
            log_flow.info("[WorkerNode] 🟢 Worker Readiness condition met. Engine is active.")
            self.ready_event.set()
        elif "worker-fatal" in matched_tags:
            log_flow.crit(f"[WorkerNode] 🚨 Fatal Worker Rupture detected: {line}")
            self.boundary.collapse()

    async def ensure(self):
        log_flow.info(f"[WorkerNode] Starting managed worker process: {self.executable_path}")
        asyncio.create_task(self._stream_and_sense())
        await asyncio.sleep(0.1)
        if not self.boundary.process_pool:
            raise RuntimeError("Failed to inject Worker into SystemBound process pool.")

        pid = self.boundary.process_pool[-1].pid
        try:
            if self.mq_surface:
                self.mq_surface.register_state(KEY_STATE_PIDS, str(pid))
                log_flow.info(f"[WorkerNode] Core PID {pid} anchored to State Store.")
        except Exception as e:
            log_flow.warning(f"[WorkerNode] State anchoring failed. Proceeding in isolated mode: {e}")

        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout=15.0)
            log_flow.info("[WorkerNode] Traffic routing enabled.")
        except asyncio.TimeoutError:
            log_flow.warning("[WorkerNode] Readiness sensor timed out, proceeding anyway.")

    def shutdown(self):
        self.boundary.collapse()