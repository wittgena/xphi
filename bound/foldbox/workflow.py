# bound.foldbox.workflow
import asyncio
import inspect
import re
import logging
from functools import wraps
from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent
from flow.surface.emitter import get_emitter

log = get_emitter("foldbox.router")

class ProcessEvent(Event):
    status: str

class FinalizeEvent(Event):
    pass

class ErrorEvent(Event):
    msg: str

def apply_topos_rules(cls):
    """클래스의 docstring을 파싱하여 3단계 폴백 라우팅을 적용하는 데코레이터"""
    docstring = cls.__doc__ or ""
    
    ## 룰 추출
    trans_rule_match = re.search(r'@trans\.rule:\s*(.+)', docstring)
    flow_match = re.search(r'@flow:\s*(.+)', docstring)
    
    trans_rule = trans_rule_match.group(1).strip() if trans_rule_match else None
    flow_rule = flow_match.group(1).strip() if flow_match else None

    ## 클래스 내의 모든 @step 메서드를 물리적 순서대로 추출
    step_methods = [
        name for name, func in inspect.getmembers(cls, predicate=inspect.isfunction)
        if hasattr(func, "__step_config") # LlamaIndex의 step 데코레이터 확인
    ]

    ## 원래의 step 메서드들을 가로채기(Intercept) 위해 래핑
    for method_name in step_methods:
        original_method = getattr(cls, method_name)
        
        @wraps(original_method)
        async def wrapper(self, ev, *args, **kwargs):
            ## 실제 스텝 실행
            result_event = await original_method(self, ev, *args, **kwargs)
            log.info(f"[Router] Step '{method_name}' 완료. 다음 경로 탐색 중...")

            ## @trans.rule 검사 (동적 전이)
            if trans_rule:
                if "error" in trans_rule and getattr(result_event, "status", "") == "error":
                    log.warning(f"[Router] @trans.rule 발동: 조건 충족, 강제 전이 발생")
                    return ErrorEvent(msg="Transduction Rule Activated")

            ## @flow 검사 (정적 위상)
            elif flow_rule:
                log.info(f"[Router] @flow 규칙에 따라 다음 이벤트 진행: {flow_rule}")
                return result_event
            
            ## 순차 실행 (Default)
            else:
                log.info(f"[Router] 명시적 룰 없음. 물리적 코드 순서에 따라 순차 실행")
                return result_event
                
        ## 래핑된 메서드로 교체
        setattr(cls, method_name, wrapper)
    return cls


@apply_topos_rules
class FoldboxWorkflow(Workflow):
    """
    @trans.rule: if event.status == 'error' -> trigger_recovery
    @flow: analyze -> process -> finalize
    """
    @step
    async def analyze(self, ev: StartEvent) -> ProcessEvent | ErrorEvent:
        log.info(">>> [Workflow] 'analyze' 스텝 실행 중...")
        return ProcessEvent(status="success")

    @step
    async def process(self, ev: ProcessEvent) -> FinalizeEvent | ErrorEvent:
        log.info(">>> [Workflow] 'process' 스텝 실행 중...")
        return FinalizeEvent()
        
    @step
    async def finalize(self, ev: FinalizeEvent) -> StopEvent:
        log.info(">>> [Workflow] 'finalize' 스텝 실행 중...")
        return StopEvent(result="모든 처리가 성공적으로 완료되었습니다!")
    
    @step
    async def handle_error(self, ev: ErrorEvent) -> StopEvent:
        log.error(f">>> [Workflow] 'handle_error' 스텝 실행: 에러 감지됨! ({ev.msg})")
        return StopEvent(result=f"실패: {ev.msg}")

async def main():
    print("## workflow.start")
    workflow = FoldboxWorkflow(timeout=10.0)
    try:
        result = await workflow.run()
        print(f"## Result: {result}")
    except Exception as e:
        print(f"## Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())