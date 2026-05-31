# nexus.hub.closure.molt
## @lineage: nexus.manager.residue.closure.molt
## @lineage: iso.domain.closure.molt
## @lineage: agent.domain.closure.molt
## @lineage: domain.closure.molt
## @lineage: scripts.abcd.closure.molt
## @lineage: extrans.field.molt
from pydantic import BaseModel, model_validator
from typing import Any

def _melt_alien_objects(obj: Any) -> Any:
    """
    재귀적으로 데이터를 순회하며, Pydantic 객체나 Dataclass 등
    '외부 네임스페이스'에서 온 객체들을 순수 딕셔너리로 강제 융해(Melt)시킵니다.
    """
    if isinstance(obj, dict):
        return {k: _melt_alien_objects(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return type(obj)(_melt_alien_objects(v) for v in obj)
    # Pydantic v2 호환
    elif hasattr(obj, 'model_dump') and callable(obj.model_dump):
        return _melt_alien_objects(obj.model_dump())
    # Pydantic v1 호환
    elif hasattr(obj, 'dict') and callable(obj.dict):
        return _melt_alien_objects(obj.dict())
    return obj

class MoltBaseModel(BaseModel):
    @model_validator(mode='before')
    @classmethod
    def sanitize_namespaces(cls, data: Any) -> Any:
        # 데이터가 딕셔너리(kwargs)로 들어온 경우에만 융해를 수행
        if isinstance(data, dict):
            return _melt_alien_objects(data)
        return data