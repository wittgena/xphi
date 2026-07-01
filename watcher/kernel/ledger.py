# watcher.kernel.ledger
"""
@desc: 
- Core execution state-machine and immutable ledger for topological sealing
- Transforms linear logic streams into sealed executable kernels using topological tension thresholds and Merkle-backed storage
"""
import time
import abc
from typing import Any, Dict, Optional, List
from enum import Enum
from pydantic import BaseModel, Field
from watcher.plane.emitter import get_emitter
from watcher.kernel.store import KernelStore, ToposBlob, KernelCommit

log = get_emitter("kernel.ledger", phase="KERNEL")

class ToposState(str, Enum):
    """@desc: Topological phases mapping to execution structure folding."""
    LINEAR = "logic.stream"         ## Open stream (Amino acid sequence)
    TRANSITIONAL = "logic.loop"     ## Local syntax validated, building tension (Alpha/Beta loop)
    KNOTTED = "sealed.kernel"       ## Tension threshold breached, independent executable (Knotted protein)
    NETWORKED = "composable.net"    ## External references and binding (Multi-protein complex)

class LogicStream(BaseModel):
    """@desc: Ψ_open - The initial open linear logic flow."""
    id: str
    payload: Any  
    metadata: Dict[str, Any] = Field(default_factory=dict)

class LogicState(BaseModel):
    """@desc: The state tracker during the evaluation phase."""
    stream_id: str
    topology: ToposState = ToposState.LINEAR
    tension: float = 0.0  # τ: Current topological tension
    context: Dict[str, Any] = Field(default_factory=dict)
    
    ## @state.merkle_buffer: Temporarily holds transition (Blob) hashes before sealing (Commit)
    pending_blob_hashes: List[str] = Field(default_factory=list)

class SealedKernel(BaseModel):
    """@desc: Ω_knot - The mathematically invariant, executable closed boundary."""
    kernel_id: str
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    signature: str  ## @desc: The deterministic Merkle Commit hash 

## τ_0: Critical tension threshold for sealing
THRESHOLD_TAU = 1.0  

class BaseTensionEvaluator(abc.ABC):
    """
    @desc:
    - Abstract base protocol for tension evaluation
    - Future validators (Syntax, Security, Context) will implement this
    """
    @abc.abstractmethod
    async def calculate(self, stream: LogicStream) -> float:
        """@desc: Returns the computed topological tension (τ) for the given stream"""
        pass

class FallbackTensionEvaluator(BaseTensionEvaluator):
    """
    @desc:
    - Temporary fallback evaluator ensuring backward compatibility
    - Mimics a successful validation by returning a static tension value
    @failure.mode: Graceful fallback if no strict evaluator is injected
    """
    async def calculate(self, stream: LogicStream) -> float:
        # @action: Return hardcoded mock tension (τ = 1.2)
        log.debug(f"[Evaluator: Fallback] Mocking tension for stream {stream.id}")
        return 1.2

class ToposLedger:
    """@desc: The core ledger utilizing the Merkle Tree backed KernelStore"""
    def __init__(self, store: KernelStore = None, evaluator: BaseTensionEvaluator = None):
        self.store = store or KernelStore()
        self.evaluator = evaluator or FallbackTensionEvaluator()
        self._active_states: Dict[str, LogicState] = {}

    def _record_transition(self, logic_state: LogicState, action: str, from_state: str, to_state: str, tension: float, details: str):
        """
        @desc: Records a state transition as a Blob and accumulates its hash into the logic state.
        @action: Appends to Merkle DAG Buffer
        """
        blob = ToposBlob(
            action=action,
            from_state=from_state,
            to_state=to_state,
            tension=tension,
            details=details
        )
        blob_hash = self.store.save_transition(blob)
        logic_state.pending_blob_hashes.append(blob_hash)
        
        log.info(f"[LEDGER|{action}] {logic_state.stream_id} | {from_state} -> {to_state} | τ: {tension:.2f} | {details}")

    async def system_evaluate(self, stream: LogicStream) -> LogicState:
        """
        @desc: Phase Evaluate (logic.validation). 
               Delegates tension calculation to the injected evaluator.
        @flow: logic.stream -> logic.loop
        """
        state = self._active_states.get(stream.id, LogicState(stream_id=stream.id))
        
        # @action: Calculate tension (τ) dynamically via the injected evaluator strategy
        calculated_tension = await self.evaluator.calculate(stream)
        
        state.tension = calculated_tension
        state.context = {"payload": stream.payload, "validated": calculated_tension > 0}
        
        prev_topology = state.topology.value
        state.topology = ToposState.TRANSITIONAL
        self._active_states[stream.id] = state
        
        self._record_transition(
            state, "evaluate", prev_topology, state.topology.value, state.tension,
            f"Evaluated linear stream using {self.evaluator.__class__.__name__}."
        )
        return state

    async def topos_seal(self, logic_state: LogicState) -> SealedKernel:
        """
        @desc: Phase Seal (topological.knotting). Folds into an executable kernel if tension threshold is met.
        @flow: logic.loop -> sealed.kernel
        """
        ## @action: Retrieve the parent hash (lineage) for the current stream
        parent_hash = self.store.get_head_hash(logic_state.stream_id)
        
        ## @action: Create the Merkle Commit bundling all pending transition blobs
        commit = KernelCommit(
            stream_id=logic_state.stream_id,
            executable_payload=logic_state.context.get("payload"),
            tension_at_seal=logic_state.tension,
            blob_hashes=logic_state.pending_blob_hashes,
            parent_hash=parent_hash
        )
        
        ## @action: Save commit to RocksDB and acquire immutable signature
        signature = self.store.save_kernel(commit)
        
        ## @action: Update the stream's HEAD reference
        self.store.update_head(logic_state.stream_id, signature)
        
        kernel = SealedKernel(
            kernel_id=f"ker_{logic_state.stream_id}",
            stream_id=logic_state.stream_id,
            executable_payload=commit.executable_payload,
            tension_at_seal=commit.tension_at_seal,
            signature=signature
        )
        
        prev_topology = logic_state.topology.value
        logic_state.topology = ToposState.KNOTTED
        
        self._record_transition(
            logic_state, "seal", prev_topology, logic_state.topology.value, logic_state.tension,
            f"Tension exceeded threshold. Sealed into kernel: {kernel.kernel_id} (Sig: {signature[:8]})"
        )
        
        ## @action: Clear pending hashes for the next potential evolutionary cycle
        logic_state.pending_blob_hashes.clear()
        
        return kernel

    async def monitor_revert(self, logic_state: LogicState) -> None:
        """
        @desc: Rejects sealing due to insufficient topological tension.
        @flow: logic.loop -> logic.stream
        """
        prev_topology = logic_state.topology.value
        logic_state.topology = ToposState.LINEAR
        logic_state.tension = 0.0
        
        self._record_transition(
            logic_state, "revert", prev_topology, logic_state.topology.value, logic_state.tension,
            "Insufficient tension. Rejected sealing, reverted to open stream."
        )

    async def parser_continue_reading(self, logic_state: LogicState) -> None:
        """
        @desc: Continues parsing when the state is still 1D.
        @flow: logic.stream -> logic.stream
        """
        self._record_transition(
            logic_state, "parse", logic_state.topology.value, logic_state.topology.value, logic_state.tension,
            "Continuing to read linear logic."
        )

    async def runtime_engine_deploy(self, kernel: SealedKernel) -> None:
        """
        @desc: Deploys the sealed kernel to the execution manifold.
        @flow: sealed.kernel -> composable.net
        """
        dummy_state = LogicState(stream_id=kernel.stream_id)
        
        self._record_transition(
            dummy_state, "deploy", ToposState.KNOTTED.value, ToposState.NETWORKED.value, kernel.tension_at_seal,
            f"Deployed kernel {kernel.kernel_id} to execution manifold."
        )

    async def compile_kernel(self, stream: LogicStream) -> Optional[SealedKernel]:
        """
        @desc: Continuous asynchronous compilation and sealing routine.
               Implements the mathematical logic loop spec.
        @execution.routine: Entry point
        """
        logic_state = await self.system_evaluate(stream)

        if logic_state.topology == ToposState.TRANSITIONAL:
            ## @guard.check: Verify if topological tension has breached the critical threshold (τ > τ_0)
            if logic_state.tension > THRESHOLD_TAU:
                sealed_kernel = await self.topos_seal(logic_state)
                await self.runtime_engine_deploy(sealed_kernel)
                return sealed_kernel
            else:
                await self.monitor_revert(logic_state)
                return None
        elif logic_state.topology == ToposState.LINEAR:
            await self.parser_continue_reading(logic_state)
            return None