# xphi.arch.dev.infra.topos
import sys
import asyncio
from typing import List, Dict, Any, Optional, TypeVar, Callable, Generic

from xphi.arch.bound.xor.parser.ruleset.stream import ElasticDSLRulesetParser, LocalStreamRulesetParser
from xphi.arch.bound.xor.parser.ruleset.engine import CompiledEngine
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.dev.tracer.base import (
    BaseAuditor, 
    BaseStreamAuditor, 
    SystemBound
)

log = get_emitter("tracer.infra.topos")

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
                        
                        # 카오스 테스트 관점: 비정상 종료 시 즉시 루프 탈출
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
        self.resolver = LogResolver[CompiledEngine](ruleset=_ruleset, parser=LocalStreamRulesetParser())
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
    """Ruleset을 엔진으로 파싱하는 모듈 (기존 동일)"""
    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None):
        self.ruleset = ruleset or {"targets": []}
        self.parser = parser or LocalStreamRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> T:
        try:
            return self.parser.parse_ruleset(self.ruleset, target_tags)
        except Exception:
            return None

HANG_VERDICT_TABLE: Dict[str, Callable[['UniversalLogAuditor'], bool]] = {
    "deadlock": lambda semantic: getattr(semantic, 'hit_fatal_limit', False),
}