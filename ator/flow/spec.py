# ator.flow.spec
"""
@flow: Ψ(Markdown Spec) → Φ(MdTranscript) → Φ_materialized(Runtime) → Ψ'(Execution/Dump)
@intent: 마크다운 형태의 '실행 가능한 위상 명세(Executable Phase Spec)'를 부트스트랩하고 제어권 이양
"""
import sys
import asyncio
from pathlib import Path
from bound.surface.emitter import get_logger
from bound.resolver import resolve_path, find_current_self
from ator.bootstrap import bootstrap
from phase.proto.flow import ProtoFlow, FlowState

SELF_ROOT = find_current_self()
CONTRACT_ROOT = resolve_path("contract")
log = get_logger("flow.spec")

async def flow_spec(md_path_str: str) -> bool:
    md_path = Path(md_path_str)
    if not md_path.exists():
        log.error(f"[Ψ:error] Phase 명세 파일을 찾을 수 없음: {md_path}")
        return False

    log.info(f"[Ψ:init] '{md_path.name}' 실행 명세 투영 시작")

    try:
        ## 내부적으로 MdPhiTranscript가 호출되어 .md의 @contract 들을 인메모리 XPHI 토폴로지로 파싱하고 런타임 노드로 변환
        log.info("  [Φ:bootstrap] Acquiring topology from Markdown...")
        base_node, flow_controller, entry_node_id = await bootstrap(str(md_path))
        
        ## 추출된 첫 번째 진입점(entry_node_id, 예: "probe.external.contract")으로 보낼 페이로드
        initial_payload = {
            "source_spec": md_path.name,
            "session_intent": "openapi_dump_execution"
        }
        initial_ctx = FlowState(ProtoFlow(payload=initial_payload, aspect="init"), state={})
        log.info(f"  [Φ:start] Injecting initial Ψ into [{entry_node_id}]")
        await base_node.psi_queue.put((entry_node_id, initial_ctx))
        await base_node.psi_queue.join()
        
        log.info(f"\n[Ψ'] 위상 전이 완료: '{md_path.name}'의 모든 계약이 물리 환경에 정합하게 투영되었습니다.")
        return True
    except Exception as e:
        ## 워쳐(Watcher) 실패 시 굴절(Rupture)되어 여기로 캐치되거나 내부 큐에서 처리
        log.error(f"[Φ:fracture] 실행 중 굴절(Rupture) 발생: {e}", exc_info=True)
        return False
    finally:
        ## Field Collapse (시스템 종료)
        if 'base_node' in locals() and base_node:
            base_node.running = False
            log.info("  [Φ:collapse] Phase field safely collapsed.")

async def main():
    ## CLI 인자가 주어지면 해당 파일을, 없으면 지정된 샘플 파일을 타겟으로 설정
    if len(sys.argv) > 1:
        target_file = Path(sys.argv[1])
    else:
        target_file = CONTRACT_ROOT / "manifold" / "index.md"

    await flow_spec(str(target_file))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n[System] User interrupted phase execution. (Force Rupture)")