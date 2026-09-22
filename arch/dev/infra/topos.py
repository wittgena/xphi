# xphi.arch.dev.infra.topos
import sys
import asyncio
from typing import List, Dict, Any, Optional, TypeVar, Callable, Generic

from xphi.arch.bound.xor.parser.ruleset.engine import CompiledEngine, StreamTaggingParser
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.dev.tracer.base import (
    BaseAuditor, 
    BaseStreamAuditor, 
    SystemBound
)

log = get_emitter("tracer.infra.topos")

T = TypeVar('T')

class ContainerStateAuditor(BaseAuditor):
    def __init__(self, target_label: str, boundary: SystemBound, namespace: str = "default"):
        super().__init__(target=target_label, namespace=namespace, boundary=boundary)
        self.pod_label = target_label
        self.is_running = True
        self.exit_code = "0"
        self.log = get_emitter(f"auditor.state.{target_label}")

    async def _observe(self) -> None:
        try:
            while True:
                cmd = [
                    "kubectl", "get", "pod", "-l", self.pod_label, "-n", self.namespace, 
                    "-o", "jsonpath={.items[0].status.phase}:{.items[0].status.containerStatuses[0].state.terminated.exitCode}"
                ]
                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                
                if code == 0 and out:
                    # e.g., "Failed:137" (OOMKilled) 또는 "Running:"
                    if "Failed" in out or "Error" in out or "Terminated" in out:
                        self.is_running = False
                        parts = out.split(":")
                        self.exit_code = parts[1].strip() if len(parts) > 1 and parts[1] else "Unknown"
                        
                        if self.exit_code not in ["", "0", "Unknown"]:
                            self.log.crit(f"  [CHAOS STATE] Pod crashed with Phase: {parts[0]}, ExitCode: {self.exit_code}")
                            break
                
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class EntropyAuditor(BaseAuditor):
    def __init__(self, target_label: str, boundary: SystemBound, namespace: str = "default"):
        super().__init__(target=target_label, namespace=namespace, boundary=boundary)
        self.pod_label = target_label
        self.log = get_emitter(f"auditor.entropy.{target_label}", phase="agent")
        self.last_cpu_usage = 0.0

    async def _observe(self) -> None:
        try:
            while True:
                # Kube Metrics Server 조회
                cmd = ["kubectl", "top", "pod", "-l", self.pod_label, "-n", self.namespace, "--no-headers"]
                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                
                if code == 0 and out:
                    out = out.strip()
                    self.log.info(f"  [METRICS] {out}")
                    try:
                        # Kube "top" 출력 형식 파싱: "pod-name   5m   12Mi"
                        parts = out.split()
                        if len(parts) >= 2:
                            cpu_m = parts[1].replace("m", "")
                            if cpu_m.isdigit():
                                self.last_cpu_usage = float(cpu_m) / 10.0  # 편의상 환산 (10m = 1.0%)
                    except Exception:
                        pass
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class UniversalLogAuditor(BaseStreamAuditor):
    """@desc: [Semantics Axis] kubectl logs -f 를 통해 스트림을 확보하고 DSL 규칙으로 파싱합니다."""
    def __init__(self, target_label: str, verify_type: str, boundary: SystemBound, namespace: str = "default", ruleset: Optional[Dict] = None):
        super().__init__(target=target_label, boundary=boundary, delay=1)
        self.pod_label = target_label
        self.namespace = namespace
        self.verify_type = verify_type
        self.log = get_emitter(f"auditor.semantic_log.{target_label}", phase="agent")
        
        _ruleset = ruleset or {"targets": []}
        self.resolver = LogResolver[CompiledEngine](ruleset=_ruleset, parser=StreamTaggingParser(engine_type="local"))
        self.rule_engine: CompiledEngine = self.resolver.resolve()
        
        self.hit_fatal_limit = False

    async def run_stream(self) -> None:
        cmd = ["kubectl", "logs", "-l", self.pod_label, "-n", self.namespace, "-f"]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        
        try:
            while True:
                line = await process.stdout.readline()
                if not line: break
                
                decoded = line.decode('utf-8', errors='replace').strip()
                if not decoded or not self.rule_engine: continue
                
                matched_tags = self.rule_engine.execute(decoded)
                for tag in matched_tags:
                    self._handle_matched_tag(tag, decoded)
                    
        except asyncio.CancelledError:
            process.terminate()

    def _handle_matched_tag(self, tag: str, line: str):
        if "fatal" in tag or "crash" in tag or "trap" in tag:
            self.hit_fatal_limit = True
            self.log.warning(f"  [CHAOS EVENT] Log boundary rupture detected: {tag}")
            self.log.info(f"    └─ Evidence: {line}")


class LogResolver(Generic[T]):
    """Ruleset을 엔진으로 파싱하는 모듈 (인터페이스 정합성 보완)"""
    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None, target_tags: Optional[List[str]] = None):
        self.ruleset = ruleset or {"targets": []}
        self.parser = parser or StreamTaggingParser(engine_type="local", target_tags=target_tags)

    def resolve(self) -> T:
        try:
            return self.parser.parse_ruleset(self.ruleset)
        except Exception as e:
            log.error(f"[LogResolver] Engine compilation failed: {e}")
            return None


# [복원됨] fiber.phase.cli.observer 가 호출하지만 누락되었던 Auditor
class LeakObserverAuditor(BaseAuditor):
    """@desc: 메모리 릭(Memory Leak) 발생 여부를 지속 관찰하는 Auditor"""
    def __init__(self, boundary: SystemBound, target_label: str = "app=fiber-worker", namespace: str = "fiber-topos"):
        super().__init__(target=target_label, namespace=namespace, boundary=boundary)
        self.pod_label = target_label
        self.log = get_emitter(f"auditor.leak.{target_label}", phase="agent")
        self.memory_growth_detected = False
        self._baseline_mem = 0.0

    async def _observe(self) -> None:
        try:
            while True:
                cmd = ["kubectl", "top", "pod", "-l", self.pod_label, "-n", self.namespace, "--no-headers"]
                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                
                if code == 0 and out:
                    try:
                        parts = out.strip().split()
                        if len(parts) >= 3:
                            # 예: "pod-name 5m 120Mi"
                            mem_str = parts[2].replace("Mi", "").replace("Gi", "")
                            current_mem = float(mem_str)
                            
                            if self._baseline_mem == 0.0:
                                self._baseline_mem = current_mem
                            elif current_mem > (self._baseline_mem * 1.5): # 50% 이상 증가 시 릭으로 판단
                                self.memory_growth_detected = True
                                self.log.warning(f"  [CHAOS EVENT] Rapid memory growth detected: {self._baseline_mem}Mi -> {current_mem}Mi")
                    except Exception:
                        pass
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            pass

HANG_VERDICT_TABLE: Dict[str, Callable[['UniversalLogAuditor'], bool]] = {
    "deadlock": lambda semantic: getattr(semantic, 'hit_fatal_limit', False),
}