# arch.topos.bound.surge.model
from pydantic import BaseModel, model_validator, ConfigDict
from typing import Any, Type, TypeVar
import dataclasses

T = TypeVar("T", bound="SurgeBaseModel")

def _melt_alien_objects(obj: Any) -> Any:
    """이종 네임스페이스에서 온 모든 실체(Instance)를 순수 데이터 질료(Primitive)로 해체"""
    if isinstance(obj, type):
        return obj

    if isinstance(obj, dict):
        return {k: _melt_alien_objects(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return type(obj)(_melt_alien_objects(v) for v in obj)

    if hasattr(obj, 'model_dump') and callable(getattr(obj, 'model_dump')):
        return _melt_alien_objects(obj.model_dump())
    if hasattr(obj, 'dict') and callable(getattr(obj, 'dict')):
        return _melt_alien_objects(obj.dict())

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _melt_alien_objects(dataclasses.asdict(obj))

    if hasattr(obj, "__dict__") and str(type(obj).__module__).startswith(("agent", "closure", "ext")):
        return _melt_alien_objects(vars(obj))

    return obj

class SurgeBaseModel(BaseModel):
    """Topological Crucible"""
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        extra='ignore'
    )

    @model_validator(mode='before')
    @classmethod
    def sanitize_namespaces(cls, data: Any) -> Any:
        """입력된 kwargs 딕셔너리 내부의 모든 외래 객체를 융해"""
        if isinstance(data, dict):
            return _melt_alien_objects(data)
        return data

    @classmethod
    def suture(cls: Type[T], **kwargs: Any) -> T:
        """@desc: model_construct를 캡슐화한 봉합"""
        melted_data = _melt_alien_objects(kwargs)
        instance = cls.model_construct(**melted_data)
        if hasattr(instance, "model_post_init"):
            instance.model_post_init(None)
        return instance

class DynamicSurgeModel(SurgeBaseModel):
    """
    - 동적 스키마(extra='allow')를 지원
    - Dict-like 접근(obj['key'])을 Pydantic V2에 맞게 안전하게 제공하는 확장 모델
    """
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        extra='allow',
        protected_namespaces=()
    )

    def __getitem__(self, key: str) -> Any:
        if hasattr(self, key):
            return getattr(self, key)
        
        if self.__pydantic_extra__ is not None and key in self.__pydantic_extra__:
            return self.__pydantic_extra__[key]
            
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        if hasattr(self, key):
            return True
        return bool(self.__pydantic_extra__ is not None and key in self.__pydantic_extra__)

    def items(self):
        return self.model_dump().items()

    def __setitem__(self, key: str, value: Any):
        setattr(self, key, value)