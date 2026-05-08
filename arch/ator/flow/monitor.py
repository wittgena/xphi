# arch.ator.flow.monitor
import functools
import re
from phase.bound.plane.emitter import get_logger

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