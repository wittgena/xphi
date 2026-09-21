# xphi.arch.dev.infra.topos
import sys
import json
import asyncio
from functools import wraps
from typing import List, Tuple, Dict, Any, Optional, Generic, TypeVar, Union, Callable

from xphi.arch.bound.xor.parser.ruleset.stream import ElasticDSLRulesetParser, LocalStreamRulesetParser
from xphi.arch.bound.xor.parser.ruleset.engine import CompiledEngine
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.dev.tracer.base import (
    BaseAuditor, 
    BaseStreamAuditor, 
    BaseBoundary,
    SystemBound,
    log_streamer
)

log = get_emitter("tracer.infra.topos")

T = TypeVar('T')

# =====================================================================
# 1. RESOLVER & MULTI-DIMENSIONAL AUDITORS (Agnostic)
# =====================================================================

class LogResolver(Generic[T]):
    """@desc: Translates high-level domain rulesets into deeply nested queries or compiled engines."""
    
    DEFAULT_RULESET = {
        "global_config": {
            "base_query": {"environment": "production"},
            "noise_exclusions": {"tags": ["debug", "load-test"], "service": "test-runner"}
        },
        "targets": [
            {"tag": "ingestor-memory-critical", "condition": {"service": "ingest.issue", "level": "ERROR"}, "keywords": [{"OR": ["OOM", "memory leak", "heap dump"]}], "apply_exclusions": True},
            {"tag": "gateway-auth-anomaly", "condition": {"service": "api_gateway"}, "keywords": [{"AND": ["unauthorized", "token"]}, {"OR": ["expired", "invalid signature"]}], "apply_exclusions": True}
        ]
    }

    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None):
        self.ruleset = ruleset if ruleset is not None else self.DEFAULT_RULESET
        self.parser = parser if parser is not None else ElasticDSLRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> T:
        try:
            resolved_result = self.parser.parse_ruleset(self.ruleset, target_tags)
            log.info(f"✅ Successfully resolved ruleset topology via {self.parser.__class__.__name__}.")
            return resolved_result
        except Exception as e:
            log.error(f"🚨 Failed to resolve ruleset: {str(e)}")
            return None


class ToposAuditor(BaseAuditor):
    """@desc: Kubernetes/K3s 전용의 Topology(형상) 상태 수집 센서."""
    def __init__(self, target: str, namespace: str, boundary: SystemBound):
        super().__init__(target, namespace, boundary)
        self.current_replicas = 0
        self.peak_replicas = 0
        self.restart_count = 0
        self.log = get_emitter(f"auditor.topos.{target}")

    async def _observe(self) -> None:
        try:
            while True:
                # 1. Replicas 관측
                rep_cmd = ["kubectl", "get", "deployment", self.target, "-n", self.namespace, "-o", "jsonpath={.spec.replicas}:{.status.readyReplicas}"]
                code, out, _ = await self.boundary.run_command(rep_cmd, capture=True)
                if code == 0 and out:
                    parts = out.split(":")
                    spec_replicas = int(parts[0]) if parts[0] else 0
                    self.current_replicas = spec_replicas
                    if spec_replicas > self.peak_replicas:
                        self.peak_replicas = spec_replicas

                # 2. Restarts 관측
                rest_cmd = ["kubectl", "get", "pods", "-l", f"app={self.target}", "-n", self.namespace, "-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}"]
                code, out, _ = await self.boundary.run_command(rest_cmd, capture=True)
                if code == 0 and out:
                    self.restart_count = sum(int(r) for r in out.split() if r.isdigit())

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class ContainerStateAuditor(BaseAuditor):
    """@desc: [Boundary Axis] Docker Compose와 K8s/K3s를 모두 지원하는 다형성 상태 검증 센서"""
    def __init__(self, target: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target=target, namespace=namespace, boundary=boundary)
        self.infra_type = infra_type
        self.is_running = True
        self.exit_code = "0"
        self.log = get_emitter(f"auditor.state.{target}")

    async def _observe(self) -> None:
        try:
            while True:
                if self.infra_type == "compose":
                    cmd = ["docker", "inspect", self.target, "--format", "{{.State.Running}}:{{.State.ExitCode}}"]
                else:
                    cmd = ["kubectl", "get", "pod", "-l", f"app={self.target}", "-n", self.namespace, "-o", "jsonpath={.items[0].status.phase}:{.items[0].status.containerStatuses[0].state.terminated.exitCode}"]

                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                
                if code == 0 and out:
                    if self.infra_type == "compose":
                        if out.startswith("false"):
                            self.is_running = False
                            self.exit_code = out.split(":")[1].strip() if ":" in out else "Unknown"
                    else:
                        # Kube 판독기 (Phase가 Failed이거나 Terminated 정보가 있을 때)
                        if "Failed" in out or "Succeeded" in out or "Error" in out:
                            self.is_running = False
                            parts = out.split(":")
                            self.exit_code = parts[1].strip() if len(parts) > 1 and parts[1] else "Unknown"
                
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class EntropyAuditor(BaseAuditor):
    """@desc: [Energy Axis] 인프라 규격에 맞추어 물리적 에너지(CPU/Mem) 변화를 관측합니다."""
    def __init__(self, target: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target=target, namespace=namespace, boundary=boundary)
        self.infra_type = infra_type
        self.log = get_emitter(f"auditor.entropy.{target}", phase="agent")
        self.last_cpu_usage = 0.0

    async def _observe(self) -> None:
        try:
            while True:
                if self.infra_type == "compose":
                    cmd = ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} | {{.MemUsage}}", self.target]
                else:
                    cmd = ["kubectl", "top", "pod", "-l", f"app={self.target}", "-n", self.namespace, "--no-headers"]

                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                if code == 0 and out:
                    out = out.strip()
                    self.log.info(f"  [METRICS] {out}")
                    try:
                        if self.infra_type == "compose":
                            self.last_cpu_usage = float(out.split("%")[0].strip())
                        else:
                            # Kube: "pod-name   5m   12Mi" -> Millicores 파싱
                            parts = out.split()
                            if len(parts) >= 2:
                                cpu_m = parts[1].replace("m", "")
                                if cpu_m.isdigit():
                                    self.last_cpu_usage = float(cpu_m) / 10.0  # 10m = 1.0% (실용적 환산)
                    except ValueError:
                        pass
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class UniversalLogAuditor(BaseStreamAuditor):
    """@desc: [Semantics Axis] Docker/Kube 로그 스트림을 동적으로 물어서 DSL 엔진으로 파싱합니다."""
    def __init__(self, target: str, verify_type: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default", ruleset: Optional[Dict] = None):
        super().__init__(target=target, boundary=boundary, delay=1)
        self.infra_type = infra_type
        self.namespace = namespace
        self.log = get_emitter(f"auditor.universal_log.{target}", phase="agent")
        
        _ruleset = ruleset or {"targets": []}
        self.resolver = LogResolver[CompiledEngine](ruleset=_ruleset, parser=LocalStreamRulesetParser())
        self.rule_engine: CompiledEngine = self.resolver.resolve()
        
        self.max_type_depth = 0
        self.hit_fatal_limit = False

    async def run_stream(self) -> None:
        """@desc: 정적 데코레이터를 벗어나 서브프로세스 파이프라인을 직접 통제합니다."""
        if self.infra_type == "compose":
            cmd = ["docker", "logs", "-f", self.target]
        else:
            cmd = ["kubectl", "logs", "-l", f"app={self.target}", "-n", self.namespace, "-f"]

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
        if tag == "rustc-recursion-depth":
            depth = line.count("SkipWhile")
            if depth > self.max_type_depth:
                self.max_type_depth = depth
                if depth % 5 == 0:
                    self.log.info(f"  [DIVERGENCE] Type Depth reached: {depth}")
        elif tag in ["rustc-fatal-limit", "wasm-fatal-panic"]:
            self.hit_fatal_limit = True
            self.log.warning(f"  [FATAL] Boundary rupture detected: {tag}")


class LeakObserverAuditor(BaseStreamAuditor):
    def __init__(self, boundary: SystemBound):
        super().__init__(target="leak_observer", boundary=boundary, delay=0)

    @log_streamer([sys.executable, "-m", "OBSERVER_MODULE"])
    async def run_stream(self, line: str) -> None:
        if "🚨" in line or "└─" in line or "online" in line:
            print(f"  [OBSERVER] {line}")

HANG_VERDICT_TABLE: Dict[str, Callable[['UniversalLogAuditor'], bool]] = {
    "rustc_recursion": lambda semantic: getattr(semantic, 'max_type_depth', 0) > 20,
    "cranelift_loop": lambda semantic: getattr(semantic, 'optimization_loop_count', 0) > 500,
}