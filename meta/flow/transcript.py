# meta.flow.transcript
## @lineage: meta.debug.fragment.transcript
## @lineage: meta.logtail.spec.fragment.transcript
## @lineage: surface.logtail.spec.fragment.transcript
## @lineage: surface.logtail.spec.digest.transcript
## @lineage: surface.logtail.digest.transcript
import json
import uuid
import time
from pathlib import Path

def transcript(payload: dict, expected_yield: int) -> str:
    """@desc: LLM이 도출한 타겟 매핑 데이터(의도)를 결합하여, 하위 노드가 실행할 스펙을 생성"""
    spec_id = f"trk_{uuid.uuid4().hex[:8]}"
    
    ## LLM이 생성한 Payload는 단순한 데이터로 캡슐화
    context = {
        "target_node": "https://rpc.gateway.local/v1",
        "headers": {"Content-Type": "application/json"},
        "payload_template": payload,
        "expected_delta": expected_yield
    }

    ## Logic - 순수 네트워크 동기화 스크립트
    logic = """def catalyze(context):
    import urllib.request
    import json
    
    req = urllib.request.Request(
        context['target_node'], 
        data=json.dumps(context['payload_template']).encode('utf-8'), 
        headers=context['headers']
    )
    
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            # 200 OK가 떨어지면 delta 반환
            if resp.getcode() == 200:
                return context['expected_delta']
    except Exception:
        pass
    
    return 0
"""

    ## JSON 스펙 조립
    spec = {
        "topology_id": spec_id,
        "ttl_ms": 3600000, # 1시간 후 세포사멸
        "created_at": int(time.time()),
        "membrane_context": context,
        "synthetic_logic": logic
    }
    
    ## 파일로 Materialize
    out_path = Path(f"spec/theoria/{spec_id}.json")
    with open(out_path, "w") as f:
        json.dump(spec, f, indent=2)
    return str(out_path)