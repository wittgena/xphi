# model.signature.inspector
import json
from rocksdict import Rdict, Options
from bound.emitter import get_emitter
from bound.resolver import resolve_path

log = get_emitter("signature.inspector")
ROCKS_PATH = resolve_path("io") / "signature.rocks"

class SignatureInspector:
    def __init__(self, path=ROCKS_PATH):
        # 읽기 전용 옵션 (실행 중인 엔진과 충돌 방지)
        self.db = Rdict(str(path))

    def inspect(self, module_id: str, limit: int = 3):
        """저장된 기저(Basis) 데이터를 위상적 관점에서 출력"""
        log.info(f"--- [Inspecting RocksDB: {module_id}] ---")
        
        # 1. 모든 키를 가져와 해당 모듈의 기저(basis) 키만 필터링 및 정렬
        keys = [k for k in self.db.keys() if k.decode().startswith(f"basis::{module_id}::") 
                and not k.decode().endswith("latest")]
        keys.sort(reverse=True) # 최신순

        if not keys:
            log.warning(f"No basis data found for module: {module_id}")
            return

        for k in keys[:limit]:
            raw_val = self.db[k]
            data = json.loads(raw_val)
            
            print(f"\n[Key: {k.decode()}]")
            print(f"├─ Timestamp: {data.get('ts')}")
            # Lineage: 위상적 주소 확인
            print(f"├─ Lineage (Identity): {data.get('lineage')}")
            
            # Context: 외부 마찰(Tension) 확인
            ctx = data.get('context', {})
            print(f"├─ Context (Δ):")
            print(f"│  └─ Tension: {ctx.get('tension')}")
            print(f"│  └─ Seed: {ctx.get('seed')}")
            print(f"│  └─ Enriched Docs: {len(ctx.get('enriched_docs', []))} units")

            # Traces: 내부 궤적(Ψ) 확인
            traces = data.get('traces', [])
            print(f"└─ Traces (Ψ): {len(traces)} steps captured")
            for i, t in enumerate(traces[:2]): # 지면상 2개만 출력
                print(f"   └─ Trace[{i}] Score: {t.get('score')} | Payload: {t.get('steps')[0].get('inputs', {}).get('payload')}")
        
        print("\n" + "="*50)

    def close(self):
        self.db.close()

if __name__ == "__main__":
    inspector = SignatureInspector()
    try:
        # agent_alpha 모듈의 물리적 흔적 조사
        inspector.inspect("agent_alpha", limit=5)
    finally:
        inspector.close()