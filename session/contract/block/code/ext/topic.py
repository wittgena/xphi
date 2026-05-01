# session.contract.block.code.ext.topic
from typing import List, Dict, Any
from pydantic import BaseModel, Field

class CoreModuleInfo(BaseModel):
    """Phase Space 내의 핵심 모듈 정보"""
    path: str = Field(..., description="모듈의 파일 시스템 경로")
    density: float = Field(..., description="해당 Phase 내의 위상적 밀도(확률)")

class PhaseSpace(BaseModel):
    """위상적으로 클러스터링된 하위 시스템(Subsystem) 정보"""
    topological_markers: List[str] = Field(..., description="해당 공간을 식별하는 주요 심볼들 (Boundary 후보)")
    core_modules: List[CoreModuleInfo] = Field(..., description="해당 공간의 중추 역할을 하는 모듈들")

class TopicMetadata(BaseModel):
    """저장소 전역 위상 메타데이터"""
    repository: str
    analyzed_modules: int
    global_interfaces: List[str] = Field(..., description="시스템 버스(Bus) 역할을 하는 전역 인터페이스 리스트")
    local_variants: Dict[str, List[str]] = Field(..., description="각 Phase 고유의 변이 심볼들")

class TopicMap(BaseModel):
    """
    [Phase Space Map] 
    topos.code.topic의 최종 출력물이자 topos.code.binder의 입력 규격
    """
    metadata: TopicMetadata
    phase_spaces: Dict[str, PhaseSpace]
    module_alignment: Dict[str, Dict[str, Any]] = Field(..., description="모듈별 Phase 소속 정보")

    @classmethod
    def load_from_json(cls, file_path: str) -> "TopicMap":
        """JSON 파일을 읽어 TopicMap 객체로 결속(Bound)합니다."""
        with open(file_path, "r", encoding="utf-8") as f:
            import json
            return cls.model_validate(json.load(f))