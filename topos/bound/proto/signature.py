# topos.bound.proto.signature
## @lineage: bridge.context.proto.signature
from pydantic import BaseModel, Field
from typing import Any

def In(desc: str = "", **kwargs) -> Any:
    """순수 Pydantic Field를 이용한 입력 필드 마커"""
    return Field(json_schema_extra={"__dspy_field_type": "input", "desc": desc, **kwargs})

def Out(desc: str = "", **kwargs) -> Any:
    """순수 Pydantic Field를 이용한 출력 필드 마커"""
    return Field(json_schema_extra={"__dspy_field_type": "output", "desc": desc, **kwargs})

class ProtoSignature(BaseModel):
    """런타임에 ThCh 스코프 내부에서 실제 DSPy Signature로 컴파일"""
    pass