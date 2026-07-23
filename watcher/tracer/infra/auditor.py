# topos.audit.tracer.infra.auditor
import asyncio
import sys
from typing import Dict, Any, Optional, Union

from watcher.tracer.resolver.log import LogResolver
from arch.xor.parser.ruleset import LocalStreamRulesetParser

from watcher.tracer.bound import (
    BaseAuditor, 
    BaseStreamAuditor, 
    BaseBoundary,
    SensorOp, 
    SystemBound,
    log_streamer
)
from watcher.plane.emitter import get_emitter

class ToposAuditor(BaseAuditor):
    """@desc: Kubernetes의 Topology(형상) 상태를 주기적으로 수집하는 센서."""
    def __init__(self, target: str, namespace: str, boundary: SystemBound):
        super().__init__(target, namespace, boundary)
        self.current_replicas = 0
        self.peak_replicas = 0
        self.restart_count = 0

    @SensorOp.poll([
        "kubectl", "get", "deployment", "{target}", "-n", "{namespace}",
        "-o", "jsonpath={.spec.replicas}:{.status.readyReplicas}"
    ])
    def parse_replicas(self, out: str) -> None:
        parts = out.split(":")
        spec_replicas = int(parts[0]) if parts[0] else 0
        self.current_replicas = spec_replicas
        if spec_replicas > self.peak_replicas:
            self.peak_replicas = spec_replicas

    @SensorOp.poll([
        "kubectl", "get", "pods", "-l", "app={target}", "-n", "{namespace}",
        "-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}"
    ])
    def parse_restarts(self, out: str) -> None:
        self.restart_count = sum(int(r) for r in out.split() if r.isdigit())

class ContainerStateAuditor(BaseAuditor):
    """@desc: [Boundary Axis] Docker 컨테이너의 종료(Collapse) 상태를 확인합니다."""
    def __init__(self, container_name: str, boundary: SystemBound):
        super().__init__(target=container_name, namespace="", boundary=boundary)
        self.container_name = container_name
        self.is_running = True
        self.exit_code = "0"

    @SensorOp.poll(["docker", "inspect", "{container_name}", "--format", "{{.State.Running}}:{{.State.ExitCode}}"])
    def parse_state(self, out: str) -> None:
        if out and out.startswith("false"):
            self.is_running = False
            self.exit_code = out.split(":")[1] if ":" in out else "Unknown"

class EntropyAuditor(BaseAuditor):
    """@desc: [Energy Axis] 컨테이너의 물리적 에너지(CPU/Mem) 변화를 관측합니다."""
    def __init__(self, container_name: str, boundary: SystemBound):
        super().__init__(target=container_name, namespace="", boundary=boundary)
        self.container_name = container_name
        self.log = get_emitter(f"auditor.entropy.{container_name}", phase="agent")
        self.last_cpu_usage = 0.0

    @SensorOp.poll(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} | {{.MemUsage}}", "{container_name}"])
    def parse_stats(self, out: str) -> None:
        self.log.info(f"  [METRICS] {out}")
        try:
            self.last_cpu_usage = float(out.split("%")[0].strip())
        except ValueError:
            pass

class LeakObserverAuditor(BaseStreamAuditor):
    """@desc: 외부 모듈(Observer)을 서브프로세스로 실행하고 그 출력을 관측합니다."""
    def __init__(self, boundary: SystemBound):
        super().__init__(target="leak_observer", boundary=boundary, delay=0)

    @log_streamer([sys.executable, "-m", "OBSERVER_MODULE"]) # NOTE: OBSERVER_MODULE 상수는 상황에 맞게 주입 필요
    async def run_stream(self, line: str) -> None:
        if "🚨" in line or "└─" in line or "online" in line:
            print(f"  [OBSERVER] {line}")

class UniversalLogAuditor(BaseStreamAuditor):
    """
    @desc: [Semantic Axis] 구형 OOMLogAuditor를 대체합니다.
           외부 Resolver를 통해 컴파일된 평가기를 기반으로 로그를 선언적으로 관측합니다.
    """
    def __init__(self, container_name: str, verify_type: str, boundary: SystemBound, ruleset: Optional[Dict] = None):
        super().__init__(target=container_name, boundary=boundary, delay=1)
        self.container_name = container_name
        self.log = get_emitter(f"auditor.universal_log.{container_name}", phase="agent")
        
        # 1. 주입받은 Ruleset을 사용 (없을 경우 빈 리스트로 초기화하여 에러 방지)
        _ruleset = ruleset or {"targets": []}
        
        self.resolver = LogResolver(ruleset=_ruleset, parser=LocalStreamRulesetParser())
        self.compiled_rules = self.resolver.resolve()
        
        ## @state
        self.max_type_depth = 0
        self.hit_fatal_limit = False

    @log_streamer(["docker", "logs", "-f", "{container_name}"])
    async def run_stream(self, line: str) -> None:
        if not line: return
        
        # 2. 컴파일된 함수들로 라인을 순회 평가
        for evaluator, tag in self.compiled_rules:
            if evaluator(line):
                # 3. Tag 기반으로 상태 업데이트 라우팅
                self._handle_matched_tag(tag, line)

    def _handle_matched_tag(self, tag: str, line: str):
        """@desc: 매칭된 태그에 따른 비즈니스 상태 변화 처리"""
        if tag == "rustc-recursion-depth":
            depth = line.count("SkipWhile")
            if depth > self.max_type_depth:
                self.max_type_depth = depth
                if depth % 5 == 0:
                    self.log.info(f"  [DIVERGENCE] Type Depth reached: {depth}")
                    
        elif tag in ["rustc-fatal-limit", "wasm-fatal-panic"]:
            self.hit_fatal_limit = True
            self.log.warning(f"  [FATAL] Boundary rupture detected: {tag}")

class SemanticLogAuditor(BaseStreamAuditor):
    """@desc: 내부 WASM/VM 로그를 스트리밍하여 Livelock(진행 정지) 상태를 관측합니다."""
    def __init__(self, target: str, namespace: str, boundary: Union[BaseBoundary, Any]):
        super().__init__(target=target, boundary=boundary, delay=3)
        self.namespace = namespace
        self.log = get_emitter(f"auditor.semantic_log.{target}", phase="agent")
        self.vm_livelock_iterations = 0

    @log_streamer([
        "kubectl", "logs", "-l", "app={target}", "-n", "{namespace}", "-f", "--tail=10"
    ])
    async def run_stream(self, line: str) -> None:
        if "wasi::fd_readdir" in line or "Weight exhausted" in line:
            self.vm_livelock_iterations += 1
            if self.vm_livelock_iterations % 50 == 0:
                self.log.warning(f"  [VM_LIVELOCK] Internal progress stalled (Iteration {self.vm_livelock_iterations})")

class ResonanceSemanticAuditor(SemanticLogAuditor):
    """@desc: Parses dimensional waves based on specific target topologies."""
    def __init__(self, target_container: str, verify_type: str, boundary: Union[BaseBoundary, Any]):
        super().__init__(target=target_container, namespace="", boundary=boundary)
        self.verify_type = verify_type
        self.gas_divergence_flag = {"evm_success": False, "pallet_revert": False}
        self.is_ruptured = False
        
    @log_streamer(["docker", "logs", "-f", "{target}"])
    async def run_stream(self, line: str) -> None:
        decoded = line.lower()
        if self.verify_type == "temporal_fixation":
            if "wasi::fd_readdir" in decoded or "adapter state loop" in decoded:
                self.vm_livelock_iterations += 1
                if self.vm_livelock_iterations > 2000:
                    self.is_ruptured = True
        elif self.verify_type == "consensus_divergence":
            if "evm: execution successful" in decoded:
                self.gas_divergence_flag["evm_success"] = True
            if "weight exhausted" in decoded or "pallet_revive::revert" in decoded:
                self.gas_divergence_flag["pallet_revert"] = True
                
            if self.gas_divergence_flag["evm_success"] and self.gas_divergence_flag["pallet_revert"]:
                self.is_ruptured = True