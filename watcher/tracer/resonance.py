# topos.audit.tracer.resonance
import sys
import asyncio
from watcher.tracer.infra.auditor import ToposAuditor, SemanticLogAuditor, ResonanceSemanticAuditor
from watcher.tracer.infra.repro import ReproBaseTracer 

from watcher.tracer.bound import BaseTracer, PhaseOp

class ResonanceTracer(ReproBaseTracer):
    def __init__(self, target_name: str):
        super().__init__(target_name=target_name, timeout=35)
        self.semantic = ResonanceSemanticAuditor(
            target_container=self.config["container_name"], 
            verify_type=self.config["verify_type"], 
            boundary=self.boundary
        )

    async def execute(self) -> None:
        """
        @desc: Single linear flow mapping the resonance lifecycle.
        """
        self.log.crit(f"## @trace.init Injecting Target Topology: {self.config['desc']}")
        
        try:
            ## Phase 1: Deploy (ReproBaseTracer의 @LifecycleOp에 의해 자동 실행)
            await self.deploy_infrastructure()
            
            ## Phase 2: Attach Auditors
            self.log.info("## @phase.2: Attaching Dimensional Observers...")
            self.semantic.attach()
            
            ## Phase 3: Observe (Resonance Window)
            self.log.info(f"## @phase.3: Waiting for Convergence Collapse (ETA: {self.timeout}s)...")
            remaining = self.timeout
            
            while remaining > 0 and not self.rupture_confirmed:
                # Auditor가 파괴를 증명하면 플래그 동기화
                if self.semantic.is_ruptured:
                    if self.config["verify_type"] == "temporal_fixation":
                        self.log.crit("  [RUPTURE] Deterministic Livelock Confirmed. Time is progressing, logic is not.")
                    elif self.config["verify_type"] == "consensus_divergence":
                        self.log.crit("  [RUPTURE] Topological Divergence Confirmed! EVM vs Substrate Consensus Mismatch.")
                    self.rupture_confirmed = True
                    break
                    
                await asyncio.sleep(1)
                remaining -= 1

            ## Phase 4: Evaluate Judgment on Timeout
            if not self.rupture_confirmed:
                if self.config["verify_type"] == "temporal_fixation" and self.semantic.livelock_iterations > 100:
                    self.log.crit(f"[SUCCESS] Semantic Hang observed (Iterations: {self.semantic.livelock_iterations}).")
                else:
                    self.log.error("[FAIL] Target stabilized without expected structural rupture.")

        finally:
            ## Phase 5: Teardown (ReproBaseTracer의 자동 파괴 호출)
            self.log.info("## @phase.5: Detaching Auditors & Initiating Teardown...")
            self.semantic.detach()
            await self.teardown_infrastructure()

class DivergenceTracer(BaseTracer):
    def __init__(self, target_namespace: str = "default", target_deploy: str = "surgent-wasm-worker", timeout: int = 60):
        super().__init__(tracer_name="kube.divergence", timeout=timeout)
        self.namespace = target_namespace
        self.target = target_deploy
        self.topology = ToposAuditor(target=self.target, namespace=self.namespace, boundary=self.boundary)
        self.semantic = SemanticLogAuditor(target=self.target, namespace=self.namespace, boundary=self.boundary)

    @PhaseOp.stimulus(["kubectl", "apply", "-f", "workspace/repro/tainted-surface.yaml"], capture=True)
    async def inject_stimulus(self, exit_code=0, stdout=""):
        pass

    async def execute(self) -> None:
        try:
            self.log.info("## @phase.1: Checking base topology (Genesis skipped/assumed running)...")
            self.log.info("## @phase.2: Injecting Tainted ConfigMap (Triggering Stimulus)...")
            await self.inject_stimulus()

            self.log.info("## @phase.3: Attaching Multidimensional Auditors (Kube API + VM Logs)...")
            self.topology.attach()
            self.semantic.attach()

            self.log.info("## @phase.4: Waiting for Control Plane Oscillation and Resonance Rupture...")
            remaining = self.timeout
            while remaining > 0 and not self.rupture_confirmed:
                is_livelocked = self.semantic.vm_livelock_iterations > 100
                is_oscillating = self.topology.peak_replicas > 2
                
                if is_livelocked and is_oscillating:
                    self.log.crit("  [RESONANCE RUPTURE] Internal Livelock is causing Kubernetes Control Plane Oscillation!")
                    self.rupture_confirmed = True
                    break

                await asyncio.sleep(1)
                remaining -= 1

            self.evaluate_judgment()

        finally:
            self.log.info("## @phase.6: Detaching Auditors (Leaving Cluster in collapsed dynamic state for autopsy)...")
            self.topology.detach()
            self.semantic.detach()

    def evaluate_judgment(self) -> None:
        """@desc: Post-Mortem evaluation based on accumulated evidence from auditors"""
        if self.rupture_confirmed:
            self.log.crit("[SUCCESS] Divergence Proven. Operator failed to reach Isorhesis due to underlying VM Time-Fixation.")
        else:
            self.log.error(f"[FAIL] Target stabilized. Replicas: {self.topology.current_replicas}. System absorbed the shock.")