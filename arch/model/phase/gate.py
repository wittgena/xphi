# arch.model.phase.gate
## @lineage: arch.contract.gov.gate
import uuid as _std_uuid
import random
from arch.contract.event.next import generator

uuid = _std_uuid

def uuid4() -> _std_uuid.UUID:
    """
    - ToposId(64bit)를 상위 비트에, 랜덤 값(64bit)을 하위 비트에 결합하여 128비트 UUID 객체(8-4-4-4-12 포맷)로 변환하여 반환
    - ToposGenerator 실패 시 표준 UUIDv4로 안전하게 폴백(Fallback)
    """
    try:
        ## 기존 체계의 ToposId (64비트 정수) 추출
        topos_int = generator.generate()
        
        ## 상위 64비트에 ToposId를, 하위 64비트에 난수를 패킹 - 고유성을 보장, 시간순 정렬(Sortable) 특성을 유지
        uuid_int = (topos_int << 64) | random.getrandbits(64)
        
        ## 표준 UUID 객체로 변환하여 반환 (호출부에서 str() 처리 호환)
        return _std_uuid.UUID(int=uuid_int)
        
    except Exception:
        ## Clock backwards 등 ToposGenerator 예외 발생 시 표준 UUIDv4로 폴백
        return _std_uuid.uuid4()