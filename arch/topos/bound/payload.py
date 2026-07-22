# arch.topos.bound.payload
import json
from typing import Any, Dict

class StreamPayloadAdapter:
    """
    @desc: Redis Stream의 1차원 데이터 제약을 극복하기 위해,
           복잡한 파이썬 객체(dict, list)를 'data' 필드로 직렬화/역직렬화하는 통신 어댑터.
    """
    DATA_KEY = "data"
    DATA_KEY_BYTES = b"data"

    @classmethod
    def encode(cls, payload: Any) -> Dict[str, str]:
        """복잡한 페이로드를 JSON 문자열로 압축하여 Redis Stream 포맷으로 변환"""
        return {cls.DATA_KEY: json.dumps(payload)}

    @classmethod
    def decode(cls, stream_message: Dict[Any, Any]) -> Any:
        """Redis Stream에서 읽어온 메시지에서 JSON 문자열을 추출하여 파이썬 객체로 복원"""
        raw_data = stream_message.get(cls.DATA_KEY) or stream_message.get(cls.DATA_KEY_BYTES)
        
        if not raw_data:
            return stream_message  # Fallback: 일반 딕셔너리 포맷인 경우 그대로 반환
            
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
            
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return raw_data  # Fallback: 단순 문자열인 경우