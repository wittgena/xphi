# watcher.tracer.topos.swarm
import asyncio
from typing import Dict, Set, Any
import json

from watcher.tracer.registry import TargetRegistry
from watcher.tracer.bound import ReproBaseTracer, LifecycleOp, PhaseOp
from watcher.tracer.infra.repro import InfrastructureMixin
from watcher.plane.emitter import get_emitter

from arch.contract.event.psi import PsiEvent
from arch.contract.event.mesh.transport import MeshP2PTransport

class SwarmFieldAuditor:
    """
    @desc: [Observer Node] Zenoh 메쉬망에 은밀히 연결되어 스웜의 열역학적 상태(Phase/Tension)를 관측합니다.
           기존의 프로세스/로그 추적이 아닌, P2P 위상 장(Topology Field) 자체를 스캐닝합니다.
    """
    def __init__(self, topic: str = "swarm:gossip:global", listen_port: int = 7447):
        self.log = get_emitter("auditor.swarm.field", phase="observer")
        self.topic = topic
        self.transport = MeshP2PTransport(listen_port=listen_port)
        
        # 상태 벡터 (노드별 위상 추적)
        self.node_phases: Dict[str, int] = {}
        self.bifurcation_detected = False
        self.syzygy_resolved = False

    async def initialize(self):
        """@step: 유령 노드로서 Zenoh Mesh 망에 수동적으로 조인"""
        await self.transport.bind_and_start(ingress_callback=self._observe_gossip)
        await self.transport.join_topic(self.topic)
        self.log.info(f"  [FIELD_AUDITOR] Ghost Node activated. Tuning to {self.topic}")

    async def _observe_gossip(self, sender_id: str, raw_bytes: bytes):
        """@flow: ψ_in -> Extract Phase -> Detect Resonance & Drift"""
        try:
            data = json.loads(raw_bytes.decode('utf-8'))
            event = PsiEvent.from_json(data)
            
            incoming_phase = event.context.get("topos_phase", 0)
            pressure = event.carrier.target_field if hasattr(event.carrier, 'target_field') else 0
            
            # 1. 텐션 공명(Resonance) 관측
            if pressure == 10:
                self.log.warning(f"  [RESONANCE] High Tension spike detected from {sender_id}. Tag: {event.tag}")

            # 2. 위상 분열(Split-Brain) 및 병합(Syzygy / EventAligner) 관측
            last_known_phase = self.node_phases.get(sender_id, incoming_phase)
            
            # 현재 관측 중인 스웜 내에 2개 이상의 서로 다른 Phase가 존재하면 망 분열(Bifurcation)로 인지
            self.node_phases[sender_id] = incoming_phase
            distinct_phases = set(self.node_phases.values())
            
            if len(distinct_phases) > 1 and not self.bifurcation_detected:
                self.bifurcation_detected = True
                self.log.crit(f"  [BIFURCATION] Topology drift verified! Swarm split into {len(distinct_phases)} multi-universes: {distinct_phases}")
                
            elif len(distinct_phases) == 1 and self.bifurcation_detected and not self.syzygy_resolved:
                self.syzygy_resolved = True
                self.log.crit(f"  [SYZYGY] Void Nexus reconciliation complete! Swarm collapsed back to Phase: {list(distinct_phases)[0]}")

        except Exception as e:
            self.log.error(f"  [FIELD_AUDITOR] Packet decoding failed: {e}")

class ChaosMeshOp:
    """@desc: 인프라 계층의 네트워크를 물리적으로 찢고(Partition) 복구(Heal)하는 혼돈 주입기"""
    def __init__(self, target_network: str, log):
        self.network = target_network
        self.log = log

    async def partition_network(self):
        """@flow: Docker 컨테이너 그룹을 A와 B로 강제 분리 (Pumba 또는 iptables 모사)"""
        self.log.warning(f"  [CHAOS] Injecting Network Partition into '{self.network}'. Slicing Mesh topology...")
        # 실제 환경에서는 pumba netem loss 또는 docker network disconnect 명령 실행
        await asyncio.to_thread(asyncio.sleep, 1) # 모사 (Mock)
        self.log.warning("  [CHAOS] Mesh is now severed. Waiting for Swarm to bifurcate.")

    async def heal_network(self):
        """@flow: 차단된 라우팅 복구 (Syzygy 유도)"""
        self.log.info(f"  [CHAOS] Healing Network '{self.network}'. Removing physical barriers...")
        # 실제 환경에서는 pumba 제어 해제
        await asyncio.to_thread(asyncio.sleep, 1) # 모사 (Mock)
        self.log.info("  [CHAOS] Physical connections restored. Awaiting EventAligner reconciliation.")


class SwarmToposTracer(ReproBaseTracer, InfrastructureMixin):
    """
    @desc: 스웜의 망 분열(Bifurcation)과 인과율 병합(Syzygy)을 관측하는 엔드투엔드 토폴로지 추적기.
    """
    def __init__(self, target_name: str = "swarm_topos_worker", timeout: int = 120):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TargetRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        
        # 스웜 위상 관측 센서 (Ghost Node)
        self.field_auditor = SwarmFieldAuditor(listen_port=7447)
        # 네트워크 카오스 엔진
        self.chaos_engine = ChaosMeshOp(self.config.get("network_name", "swarm_mesh_net"), self.log)

    async def _await_swarm_stabilization(self):
        self.log.info("## @phase.1.5: Waiting for Swarm EventDisperser to establish global baseline...")
        await asyncio.sleep(5)  # 에이전트들이 켜지고 초기 가십을 교환할 시간

    async def execute(self) -> None:
        self.log.crit("## @trace.init Commencing Thermodynamic Swarm Topology Trace")
        
        try:
            # 1. 인프라 배포 (에이전트 컨테이너 5~10대 가동)
            await self.deploy_infrastructure()
            
            # 2. Ghost Node(Auditor)로서 Mesh 망 조인
            self.log.info("## @phase.2: Attaching Swarm Field Auditor to Zenoh Mesh...")
            await self.field_auditor.initialize()
            
            # 3. 스웜 초기 안정화 대기
            await self._await_swarm_stabilization()
            
            # 4. 자극 주입: 강제 네트워크 망 분열 (Chaos)
            self.log.info("## @phase.3: Injecting Topological Stimulus (Network Cut)...")
            await self.chaos_engine.partition_network()
            
            # 5. 분열(Split-Brain) 관측 대기
            self.log.info("## @phase.4: Awaiting Bifurcation & Node0 Spontaneous Generation...")
            for _ in range(15):
                if self.field_auditor.bifurcation_detected:
                    break
                await asyncio.sleep(1)
            
            if not self.field_auditor.bifurcation_detected:
                self.log.error("[FAIL] Swarm did not bifurcate. Tension may have stalled.")
                return

            # 6. 네트워크 복구 (Healing)
            self.log.info("## @phase.5: Reconnecting Mesh to trigger EventAligner Syzygy...")
            await self.chaos_engine.heal_network()
            
            # 7. 병합(Syzygy) 관측 대기
            self.log.info("## @phase.6: Awaiting Topos Collapse (Syzygy)...")
            for _ in range(15):
                if self.field_auditor.syzygy_resolved:
                    self.log.crit("[SUCCESS] Absolute Topological Resonance achieved. The Swarm is immortal.")
                    return
                await asyncio.sleep(1)
                
            self.log.error("[FAIL] Syzygy failed. Split-Brain condition persisted permanently.")

        finally:
            self.log.info("## @phase.7: Initiating Teardown...")
            await self.teardown_infrastructure()
            # Ghost Node 소켓 정리
            await self.field_auditor.transport.close()