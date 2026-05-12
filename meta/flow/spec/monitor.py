# meta.flow.spec.monitor
"""
@flow: Ψ(Markdown Spec) → Φ(MdTranscript) → Φ_materialized(Runtime) → Ψ'(Execution/Dump)
@intent: 마크다운 형태의 '실행 가능한 위상 명세(Executable Phase Spec)'를 부트스트랩하고 제어권 이양
"""
import sys
import asyncio
from pathlib import Path
from topos.bound.plane.emitter import get_logger
from topos.bound.resolver import resolve_path, find_current_self
from xphi.ator.bootstrap import bootstrap
from topos.bound.proto.flow import ProtoFlow, FlowState

SELF_ROOT = find_current_self()
CONTRACT_ROOT = resolve_path("contract")
log = get_logger("flow.monitor")

def flow_monitor(func):
    """메서드의 docstring에서 @flow 또는 @phase 메타데이터를 추출해 런타임 흐름을 감시합니다."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        docstring = func.__doc__ or ""
        
        ## 주석에서 의도(Intent) 추출
        flow_match = re.search(r'@flow:\s*(.+)', docstring)
        phase_match = re.search(r'@phase:\s*(.+)', docstring)
        
        if flow_match:
            log.info(f"[Monitor] 기대 흐름(Flow) 확인: {flow_match.group(1).strip()}")
        if phase_match:
            log.info(f"[Monitor] 현재 위상(Phase) 진입: {phase_match.group(1).strip()}")

        ## 실행 전 컨텍스트 검증 (예: flow 인스턴스의 상태 확인)
        ## args에서 ProtoFlow 객체를 찾아 현재 실행되어야 할 노드인지 검증
        flow_obj = next((arg for arg in args if hasattr(arg, 'payload')), None)
        if flow_obj and hasattr(self, 'role'):
             log.debug(f"  -> 검증: Operator '{self.role}'가 현재 토폴로지 컨텍스트와 일치합니다.")

        ## 본래 함수 실행
        result = func(self, *args, **kwargs)

        ## 실행 후 컨트롤 (이탈 방지)
        ## 만약 반환된 result(ProtoFlow)가 마크다운의 @phase.flow에 정의된 next 경로를 
        ## 따르지 않는다면 여기서 강제로 인터셉트하여 예외를 발생시키거나 NODE0로 붕괴시킬 수 있습니다.
        return result
    return wrapper

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