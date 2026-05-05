# bound.code.schema
import json
from typing import Any, Callable, Dict, Optional, Union, List, Any
from pydantic import BaseModel, Field, ConfigDict

class HypoSchema(BaseModel):
    """Φ_canonical: 경계($\partial$)에서 수집된 파편을 실체(Bound)로 응집한 표준 위상 스키마"""
    model_config = ConfigDict(arbitrary_types_allowed=True) # Callable 허용
    name: str = Field(..., description="객체 또는 도구의 식별자")
    module_origin: Optional[str] = Field(None, description="원본 모듈 경로 ($\partial$의 위치)")
    
    ## [Boundary Echoes] 경계면 현상 데이터
    status: str = Field("Unresolved", description="발현 상태: Signature_Captured | Deep_Boundary_Mapped")
    traces: Dict[str, Any] = Field(default_factory=dict, description="Tracer가 수집한 원시 반향 데이터")
    
    ## [Bound Structure] 실체적 결속 데이터
    description: Optional[str] = Field(None, description="추론된 도구의 목적")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="구조적 요구사항 (JSON Schema)")
    executable: Optional[Callable] = Field(None, exclude=True, description="실제 실행 가능한 결속체")

class HypoRegistry:
    """Bound Registry: 파편화된 가설들을 하나의 위상 지도(Map)로 결속하는 저장소"""
    def __init__(self):
        self._hypotheses: Dict[str, HypoSchema] = {}

    def assimilate(self, module_name: str, target_name: str, echoes: Dict[str, Any]):
        """Binder에서 전달된 Echoes를 ExtSchema로 변환하여 결속(Bound)"""
        key = f"{module_name}::{target_name}"
        
        ## Echoes의 깊이에 따른 상태 결정
        state = "Signature_Captured"
        if echoes.get("behavioral_map"):
            state = "Deep_Boundary_Mapped"

        ## 파편을 표준 스키마로 응집
        schema = HypoSchema(
            name=target_name,
            module_origin=module_name,
            status=state,
            traces=echoes,
            parameters={"raw_sig": echoes.get("signature")} 
        )
        self._hypotheses[key] = schema

    def get_hypothesis(self, key: str) -> Optional[HypoSchema]:
        return self._hypotheses.get(key)

    def dump(self) -> str:
        """가설의 전질(Whole)을 JSON으로 출력 (실행체 제외)"""
        return json.dumps(
            {k: v.model_dump() for k, v in self._hypotheses.items()}, 
            indent=2, ensure_ascii=False
        )