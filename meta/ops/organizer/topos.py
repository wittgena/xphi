# meta.ops.organizer.topos
import time
import asyncio
import redis.asyncio as redis_async
from typing import Dict, Any
from bound.surface.emitter import get_emitter
from phase.node.runtime import NodeRuntime
from phase.proto.col import get_proto
from phase.proto.flow import ProtoFlow, FlowState
from topos.state.rule.trans import PhaseSpec, TransRule
from topos.state.proxy import DistributedNodePool
from topos.state.node import LinkerNode, InversionNode, ToposNode, NodeType
from topos.runtime import ToposRuntime

log = get_emitter("organizer.topos")

NODE_REGISTRY = {
    "linker": LinkerNode,
    "inversion": InversionNode,
    "topos": ToposNode
}

class ToposOrganizer:
    def __init__(self, pool: DistributedNodePool):
        self.pool = pool

    def build_runtime_nodes(self, ir_specs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        runtime_nodes: Dict[str, Any] = {}
        for node_id, spec in ir_specs.items():
            node_type = spec.get("type")
            node_cls = NODE_REGISTRY.get(node_type)
            
            if node_cls is None:
                raise ValueError(
                    f"[Organizer] 알 수 없는 노드 타입입니다: '{node_type}'. "
                    f"NODE_REGISTRY에 등록되어 있는지 확인하세요."
                )
            
            p = get_proto(node_cls)
            if p is None:
                raise TypeError(
                    f"[Organizer] 조립 실패: '{node_cls.__name__}' 클래스에 @proto 정의가 없습니다. "
                    f"모든 노드는 반드시 Contract/Proto 규격을 준수해야 합니다."
                )
                
            log.debug(
                f"[Organizer] Validated: {node_cls.__name__} | proto={p.kind} | "
                f"seq={[getattr(x, '__name__', str(x)) for x in p.sequence]}"
            )
            
            node_instance = node_cls(spec, self.pool)
            setattr(node_instance, "__node_id__", node_id)
            runtime_nodes[node_id] = node_instance
        return runtime_nodes

async def main():
    ir_specs = {
        "linker_1": {"type": "linker", "next": "inversion_1"},
        "inversion_1": {"type": "inversion", "next": "END"}
    }
    entry_point = "linker_1"

    base_node = NodeRuntime(redis_url="redis://localhost:6379", executor=None)
    base_node.redis = redis_async.from_url(base_node.redis_url, decode_responses=True)

    try:
        dummy_worker_id = "node-dummy-123"
        await base_node.redis.sadd("runtime:index:emits:capability:code", dummy_worker_id)
        await base_node.redis.sadd("runtime:index:emits:capability:logic", dummy_worker_id)
        await base_node.redis.set(f"runtime:heartbeat:{dummy_worker_id}", int(time.time()), ex=60)

        ## Pool 및 Organizer를 통한 Graph 물리화
        pool = DistributedNodePool(base_node)
        organizer = ToposOrganizer(pool)
        runtime_nodes = organizer.build_runtime_nodes(ir_specs)
        log.info(f"[Organizer] 성공적으로 {len(runtime_nodes)}개의 런타임 노드를 빌드했습니다.")

        ## Runtime Engine 부착 및 실행
        flow_controller = ToposRuntime(
            entry=entry_point,
            nodes=runtime_nodes,
            runtime_node=base_node
        )
        flow_controller.attach()

        topos = ToposNode(spec={
            "name": "root",
            "kind": NodeType.ANCHOR,
            "children": {
                "self": ToposNode(spec={
                    "name": "field",
                    "kind": NodeType.CORE,
                    "children": {
                        "legacy_symlink": ToposNode(
                            spec={ "name": "legacy_symlink", "kind": NodeType.SYMLINK, "ref_target": "ext_src" }
                        ),
                        "stable_core": ToposNode(
                            spec={ "name": "stable_core", "kind": NodeType.CORE, "content": "existing logic" }
                        )
                    }
                })
            }
        })
        
        ## 외부 PR 신호 - 'stable_core'를 'evolved_core'로 바꾸라는 외부 제안을 주입
        external_pr_rules = [
            TransRule("stable_core", "evolved_core", NodeType.CORE)
        ]

        ## 자가 유도 활성화 - payload에 'target_spec'이 없으므로 LinkerNode가 'legacy_symlink'를 스스로 서치
        initial_flow = ProtoFlow(payload={}, aspect="root")
        initial_ctx = FlowState(initial_flow, state={
            "topos_root": topos,
            "external_rules": external_pr_rules  # 외부 신호 주입
        })

        log.info(f">>> Injecting Evolutionary Flow into ({entry_point})...")
        await base_node.psi_queue.put((entry_point, initial_ctx))
        await base_node.psi_queue.join()
        log.info(">>> Evolution Cycle Finished.")
        
    except Exception as e:
        log.error(f"Execution Error: {e}", exc_info=True)
    finally:
        base_node.running = False
        for task in base_node.loop_tasks:
            task.cancel()
        await base_node.redis.aclose()

if __name__ == "__main__":
    asyncio.run(main())