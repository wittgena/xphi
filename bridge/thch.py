# bridge.thch
import sys
import inspect
from contextlib import contextmanager
from typing import Any, Type
import dspy
import dspy.predict.chain_of_thought

@contextmanager
def thch_scope(lm: dspy.LM = None):
    """[Inversion & Folding] dspy의 모든 Fractal 구조와 LM 의존성을 ThCh 내부로 접어 넣습니다."""
    ## dspy의 원본 유전자(DNA) 추출
    original_cot = dspy.predict.chain_of_thought.ChainOfThought

    ## ThCh에 dspy의 엔진 기능을 일시적으로 위임(Delegation)
    ThCh._folded_engine = original_cot

    try:
        ## 전역 반전 (Monkey Patching) - dspy 내부의 모든 모듈이 ChainOfThought를 찾을 때 ThCh을 반환하게 함
        dspy.predict.chain_of_thought.ChainOfThought = ThCh
        dspy.predict.ChainOfThought = ThCh
        dspy.ChainOfThought = ThCh

        ## 프랙탈 하이재킹 실행
        inject_spirals()
        
        ## dspy.context()를 사용하여 스코프(공간)가 유지되는 동안에만 LM 의존성을 허용
        if lm:
            with dspy.context(lm=lm):
                yield 
        else:
            yield
            
    finally:
        ## Unfolding - 원상 복구
        dspy.predict.chain_of_thought.ChainOfThought = original_cot
        dspy.predict.ChainOfThought = original_cot
        dspy.ChainOfThought = original_cot
        ThCh._folded_engine = None

class ThCh(dspy.Module):
    ## scope에 의해 동적으로 주입될 엔진 클래스
    _folded_engine: Type = None

    def __init__(self, signature, **kwargs):
        super().__init__()
        ## dspy를 직접 호출하지 않고, 위임된 엔진이 있다면 그것을 사용
        if self._folded_engine:
            self.engine = self._folded_engine(signature, **kwargs)
        else:
            self.engine = None
            ## [파열 지점] 나중에 dspy 의존성이 제거되면 이곳에 자체 엔진이 들어옵니다.

    def forward(self, **kwargs):
        if self.engine:
            return self.engine(**kwargs)
        return None

    async def aforward(self, **kwargs):
        if self.engine and hasattr(self.engine, 'acall'):
            return await self.engine.acall(**kwargs)
        return None

def inject_spirals():
    """메모리에 적재된 dspy 하위 모듈들의 연결 고리를 ThCh으로 교체"""
    for module_name, module in sys.modules.items():
        if module_name.startswith("dspy") and "thch" not in module_name:
            for attr_name in dir(module):
                attr_value = getattr(module, attr_name, None)
                if inspect.isclass(attr_value) and attr_value.__name__ == "ChainOfThought":
                    if attr_value is not ThCh:
                        setattr(module, attr_name, ThCh)