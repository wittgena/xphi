# topos.system
"""
@desc: A unified runtime model where Flow (Dynamics), Substate (Observation), and Organizer (Materialization) resonate and rupture through the Manifold.
@topos: 
- [e]  : Electronic pulse (The continuous async event loop)
- [TEXT] : [t] - [ex] - [t] (A closed topological capsule on the memory)
- [CON-TEXT] : Convergent state (stable_core)
- [CONT-EXT] : Rupture and topological expansion (trigger_rupture)
- [ex] : Forward execution vector
- [xe] : Accumulated residue/friction in void_gap
"""
import asyncio
import logging
import time
import redis.asyncio as redis_async
from phase.runtime.node import NodeRuntime
from phase.reflect.proto.flow import ProtoFlow, FlowState
from cognitive.dynamics.manifold.flow import TensionAccumulator, PhaseProjector, ToposCollapse, ReentryInversion
from cognitive.dynamics.manifold.particle import ToposManifold
from topos.state.proxy import DistributedNodePool
from topos.organizer import ToposOrganizer
from topos.state.node import inject_pr_signal, StateNode, NodeType
from topos.state.runtime import StateRuntime
from topos.bound.plane.emitter import get_emitter

log = get_emitter("topos.system")

class SubStateSurveillance:
    """
    @topos: Observer of [xe] residue. 
    Monitors the accumulation of [xe] until the closed [t-ex-t] capsule reaches saturation, triggering [CONT-EXT].
    """
    def __init__(self, threshold=8):
        self.rupture_threshold = threshold

    async def monitor(self):
        log.info("[SubState] Meta-surveillance initiated. Awaiting tension collapse...")
        while True:
            await asyncio.sleep(1.0)
            ## @phase: Measuring the density of [xe] (Residue/Friction)
            tension_level = ToposManifold.void_gap.qsize()
            
            if tension_level > 0:
                log.info(f"   ↳ [Observe] Current manifold tension: {tension_level} / {self.rupture_threshold}")

            ## @phase: The moment of [CONT-EXT] (Rupture)
            ## The boundary [t] is breached, releasing the contained [ex] energy outward.
            if tension_level >= self.rupture_threshold:
                log.warning(f"   ⚠️ [Rupture] Critical tension ({tension_level}) breached! Phase transition triggered!")
                await self.trigger_rupture()
                await asyncio.sleep(5) ## Dormancy period before re-entry post-collapse

    async def trigger_rupture(self):
        ## @trans: Emitting a new transition rule [t] to reconstruct the collapsed [TEXT].
        pr_signal = {
            "proposed_changes": [
                {"src": "stable_core", "dest": "evolved_core", "kind": "CORE"}
            ]
        }
        await ToposManifold.psi_queue.put(("linker_1", pr_signal))
        
        ## @flush: Dissipating the remaining [xe] to restore the void.
        while not ToposManifold.void_gap.empty():
            try:
                ToposManifold.void_gap.get_nowait()
            except asyncio.QueueEmpty:
                break

class ToposSystem:
    """
    @topos: The builder of [CON-TEXT].
    Reassembles the external [xe] and transition signals [t] into a new, stable [t-ex-t] capsule.
    """
    def __init__(self, base_node: NodeRuntime):
        self.base_node = base_node
        self.pool = DistributedNodePool(self.base_node)
        self.organizer = ToposOrganizer(self.pool)

    async def listen_and_build(self):
        log.info("[Organizer] Physical Builder standing by...")
        
        ## Initial IR Spec (Pre-defined baseline mutation pipeline of the system)
        ir_specs = {
            "linker_1": {"type": "linker", "next": "inversion_1"},
            "inversion_1": {"type": "inversion", "next": "END"}
        }
        
        ## Pre-build runtime nodes (utilizing ToposOrganizer)
        runtime_nodes = self.organizer.build_runtime_nodes(ir_specs)
        
        ## Attach Runtime Flow Controller
        flow_controller = StateRuntime(entry="linker_1", nodes=runtime_nodes, runtime_node=self.base_node)
        flow_controller.attach()

        ## @structure: Defining the initial [CON-TEXT] boundary. 
        ## (Temporarily defined; to be loaded from storage in reality)
        current_phase = StateNode(spec={
            "name": "root", "kind": NodeType.ANCHOR,
            "children": {
                "self": StateNode(spec={
                    "name": "field", "kind": NodeType.CORE,
                    "children": {
                        "stable_core": StateNode(spec={"name": "stable_core", "kind": NodeType.CORE, "content": "Legacy Logic"})
                    }
                })
            }
        })

        while True:
            ## @phase: Receive the [CONT-EXT] perturbation signal
            entry_point, pr_signal = await ToposManifold.psi_queue.get()
            log.error(f"\n[Organizer] >>> Evolution Signal (PR Signal) Received! Initiating physical topology reconstruction <<<")
            
            ## @inject: Translating the perturbation into a new Transition [t]
            initial_flow = ProtoFlow(payload={}, aspect="root")
            ctx = FlowState(initial_flow, state={"phase_root": current_phase})
            ctx = inject_pr_signal(ctx, pr_signal)

            ## @mutate: Advecting [xe] into the topology to construct a higher-dimensional [TEXT]
            log.info(f"[Organizer] Advecting state into the phase mutation pipeline ({entry_point})...")
            await flow_controller.psi_queue.put((entry_point, ctx))
            # await self.base_node.psi_queue.put((entry_point, ctx))
            
            ## Awaiting NodeRuntime queue processing (Handled by async workers in reality)
            await asyncio.sleep(2.0)
            
            ## @phase: [CON-TEXT] Converged. A new stable topological boundary is formed.
            log.info("[Organizer] >>> Topology reconstruction complete. New homeostasis achieved.\n")
            ToposManifold.psi_queue.task_done()


async def main():
    ## Initialize Redis and foundational NodeRuntime
    base_node = NodeRuntime(redis_url="redis://localhost:6379", executor=None)
    base_node.redis = redis_async.from_url(base_node.redis_url, decode_responses=True)
    # base_node.psi_queue = asyncio.Queue()   

    dummy_worker_id = "node-dummy-123"
    await base_node.redis.sadd("runtime:index:emits:capability:code", dummy_worker_id)
    await base_node.redis.set(f"runtime:heartbeat:{dummy_worker_id}", int(time.time()), ex=60)

    ## Instantiate Dynamics Layer (Flow)
    accumulator = TensionAccumulator(threshold=1.0)
    projector = PhaseProjector()  # 내부에서 알아서 세팅할 것으로 추정됨
    collapse = ToposCollapse()    # 내부에서 알아서 세팅할 것으로 추정됨
    inversion = ReentryInversion() # 에러 발생 지점 수정

    ## Instantiate Meta-Observer (Mind) and Physical Builder (Body)
    mind = SubStateSurveillance(threshold=8)
    body = ToposSystem(base_node=base_node)

    ## Render the Entire System
    ## @pulse: The continuous electronic pulse [e] driving the [ex] forward execution.
    tasks = [
        ## The 4-stroke heart where energy accumulates and flows (Generation of [ex] and [xe])
        asyncio.create_task(accumulator.exist()),
        asyncio.create_task(projector.exist()),
        asyncio.create_task(collapse.exist()),
        asyncio.create_task(inversion.exist()),
        
        ## Observe tension and autonomously mutate its own code (The Dialectic of [CONT-EXT] and [CON-TEXT])
        asyncio.create_task(mind.monitor()),
        asyncio.create_task(body.listen_and_build()),
    ]
    
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await base_node.redis.aclose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("System collapsed.")