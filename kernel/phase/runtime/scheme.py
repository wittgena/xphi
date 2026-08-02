# kernel.phase.runtime.scheme
## @lineage: phase.runtime.scheme
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from phase.wasm.executor import WasmExecutor, TaskContext, EffectResolver
from arch.xor.parser.block.contract import Contract, CoherenceState
from watcher.dphi.broker import WasmBroker
from watcher.plane.emitter import get_emitter

log = get_emitter("runtime.scheme")

class RuntimeSchemeRunner(ABC):
    def __init__(self, broker: WasmBroker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = broker
        self.executor = WasmExecutor(resolvers=resolvers)
        self.is_running = False

    async def watch_and_react(self, initial_context: TaskContext):
        self.is_running = True
        log.info(f"[RuntimeRunner] Activated pattern for task: {initial_context.task_type}")
        
        async for contract in self.executor.execute_stream(initial_context):
            if not self.is_running:
                break
            
            await self.on_contract_emitted(contract)

    @abstractmethod
    async def on_contract_emitted(self, contract: Contract):
        """하위 Scheme 클래스들이 반드시 구현해야 하는 감응(React) 인터페이스"""
        pass

    def stop(self):
        self.is_running = False

class AutonomousRecoveryScheme(RuntimeSchemeRunner):
    def __init__(self, broker: WasmBroker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        # 서명 키 등 복구에 필요한 도구 초기화 (생략)

    async def on_contract_emitted(self, contract: Contract):
        # 1. 정상 상태일 때는 관측만 하고 개입하지 않음
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[RecoveryObserver] Node streaming normally. Topos: {contract.topos_id}")
            return
            
        # 2. 파편화(발산, 변칙, 고아) 발생 시 복구 트리거 가동
        if contract.state == CoherenceState.FRAGMENTED:
            log.warning(f"[RecoveryObserver] Anomaly detected! Phase lost at Topos: {contract.topos_id}")
            await self._trigger_parity_recovery(contract)
            
        # 3. 작업이 정상 완료되었을 때의 후처리 (단일 단어 'coherence' 적용)
        elif contract.state == CoherenceState.COHERENT and contract.kind == "coherence":
            log.info(f"[RecoveryObserver] Task finalized coherently. Nexus: {contract.nexus_id}")

    async def _trigger_parity_recovery(self, failed_contract: Contract):
        """기존의 _step2, _step3 로직이 이곳으로 통합되어 이벤트 발생 시 호출됨"""
        log.info(f"--- Initiating Parity Recovery for Nexus {failed_contract.nexus_id} ---")
        
        # 복구를 위한 새로운 TaskContext 생성 후 Executor에 재주입 (Feed-back)
        recovery_context = TaskContext(
            task_type="verify_parity",
            payload={"nexus_id": failed_contract.nexus_id, "topos_id": failed_contract.topos_id}
        )
        
        # 복구 프로세스 자체도 Executor를 통해 불변 Contract로 방출됨
        async for recovery_contract in self.executor.execute_stream(recovery_context):
            if recovery_contract.state == CoherenceState.COHERENT:
                log.info("Recovery and Rebase successful. Sealing to ledger.")
                # 원장(Ledger) 기록 로직 수행
                break

class SyzygyResonanceScheme(RuntimeSchemeRunner):
    async def on_contract_emitted(self, contract: Contract):
        if contract.kind == "divergence":
            log.warning(f"[SyzygyObserver] Drift detected at Phase: {contract.phase_id}")
            await self._seal_void_nexus(contract)

    async def _seal_void_nexus(self, contract: Contract):
        # 소수파 격리 로직 수행
        pass