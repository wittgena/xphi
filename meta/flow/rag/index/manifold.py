# meta.flow.rag.index.manifold
import sys
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Dict, Any
from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent, Context
from llama_index.core.schema import TextNode
from llama_index.core import VectorStoreIndex
from bound.surface.emitter import get_logger
from meta.flow.xphi.xor import Xor 
from meta.flow.xphi.ktory import EmissionRunner, KotlinPSITool, RipgrepTool
from bound.resolver import find_current_self, resolve_path
from bound.folding import folding
from topos.project.block.extractor import extract_block_from_file, Block

log = get_logger("index.manifold")

try:
    SELF_ROOT = find_current_self()
    XOR_ROOT = resolve_path("xor")
    METADATA_ROOT = resolve_path("io") / "metadata"
    BLOCKS_ROOT = XOR_ROOT / "blocks"
except Exception as e:
    log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)

class RepoAnalyzedEvent(Event):
    target_files: List[Path]
    kt_contracts_map: Dict[str, list]

class ExtractionDoneEvent(Event):
    """추출이 완료되어 양방향 인덱싱 준비가 끝남을 알리는 이벤트"""
    json_path: Path
    llama_nodes: List[TextNode]

class XorIndexedEvent(Event):
    """Xor 분산 인덱싱 완료"""
    status: str

class LlamaIndexedEvent(Event):
    """LlamaIndex 로컬/벡터 인덱싱 완료"""
    index: VectorStoreIndex

class DualIngestionWorkflow(Workflow):
    """
    ### @project.regime("indexing")
    @flow: analyze -> prepare -> build -> finalize
    
    ### @phase.flow
    ```yaml
    analyze_repository:
      retry: 5           # bounding이 이를 읽어 5회 재시도를 적용함
    trigger_xor_indexing:
      retry: 2           # 분산 환경에 따른 개별 장력 조절
    ```
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xor_client = Xor()
        self.persist_dir = XOR_ROOT / "llama"

    @step
    async def analyze_repository(self, ev: StartEvent) -> RepoAnalyzedEvent:
        """Step 1: 대상 파일 목록 및 정적 분석 (이전과 동일)"""
        target_dir = ev.get("target_dir")
        if not target_dir:
            raise ValueError("target_dir가 StartEvent에 필요합니다.")

        root = Path(target_dir)
        log.info(f"[Step 1] '{root.name}' 프로젝트 분석 시작...")

        kt_runner = EmissionRunner([KotlinPSITool(), RipgrepTool()])
        all_kt_contracts = await asyncio.to_thread(kt_runner.run, str(root))
        
        kt_contracts_map = {}
        for fact in all_kt_contracts:
            if fact.source:
                abs_file_path = str(Path(fact.source).absolute())
                kt_contracts_map.setdefault(abs_file_path, []).append(fact)

        target_files = list(root.rglob("*.md")) + list(root.rglob("*.py")) + list(root.rglob("*.kt"))
        log.info(f"[Step 1] 분석 완료. 대상 파일: {len(target_files)}개")
        
        return RepoAnalyzedEvent(target_files=target_files, kt_contracts_map=kt_contracts_map)

    @step
    async def extract_and_prepare(self, ev: RepoAnalyzedEvent) -> ExtractionDoneEvent:
        """Step 2: 파일을 파싱하여 JSON을 저장하고 LlamaIndex Node를 생성합니다."""
        all_llama_nodes = []
        total_blocks = 0
        
        def _process_files():
            nonlocal total_blocks
            for path in ev.target_files:
                try:
                    blocks = extract_block_from_file(path, ev.kt_contracts_map)
                    if not blocks:
                        continue
                    
                    total_blocks += len(blocks)
                    blocks_dict = []
                    
                    # 1. LlamaIndex TextNode 매핑 & JSON Dict 준비
                    for b in blocks:
                        b_dict = b.to_dict()
                        blocks_dict.append(b_dict)
                        
                        # LlamaIndex 메타데이터 제약(Flat structure, No None)을 위한 정제
                        meta = {k: (v if v is not None else "") for k, v in b_dict.items()}
                        if isinstance(meta.get("symbols"), list):
                            meta["symbols"] = ", ".join(meta["symbols"])
                            
                        node = TextNode(
                            id_=b.block_id,
                            text=b.content or "",
                            metadata=meta
                        )
                        all_llama_nodes.append(node)

                    # 2. Xor 처리를 위한 JSON 파일 저장
                    try:
                        rel_path = path.relative_to(SELF_ROOT)
                    except ValueError:
                        rel_path = path.name

                    out_path = BLOCKS_ROOT / Path(rel_path).with_suffix(".json")
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text(json.dumps(blocks_dict, indent=2, ensure_ascii=False), encoding="utf-8")
                
                except Exception as e:
                    log.error(f"[error] {path} 파싱 실패: {e}")

        await asyncio.to_thread(_process_files)
        log.info(f"[Step 2] 추출 완료. 총 {total_blocks}개 블록 준비됨.")
        
        return ExtractionDoneEvent(json_path=BLOCKS_ROOT, llama_nodes=all_llama_nodes)

    ## [병렬 분기 1] Xor 인덱싱 트리거
    @step
    async def trigger_xor_indexing(self, ev: ExtractionDoneEvent) -> XorIndexedEvent:
        """Step 3-A: 기존 Xor(Kotlin/Lucene) 시스템에 분산 인덱싱을 요청합니다."""
        target_path = str(ev.json_path)
        log.info(f"[Step 3-A] Xor 분산 인덱싱 트리거: {target_path}")
        
        await asyncio.to_thread(self.xor_client.index_dist, target_path)
        return XorIndexedEvent(status="Xor Indexing Dispatched")

    ## [병렬 분기 2] LlamaIndex 인덱싱
    @step
    async def build_llama_index(self, ev: ExtractionDoneEvent) -> LlamaIndexedEvent:
        """Step 3-B: LlamaIndex를 생성, 저장하고 메타데이터(Manifest)를 기록합니다."""
        log.info(f"[Step 3-B] LlamaIndex 네이티브 인덱싱 시작 (노드 {len(ev.llama_nodes)}개)")
        
        def _build_and_record_metadata():
            ## 인덱스 생성 및 저장
            index = VectorStoreIndex(ev.llama_nodes)
            persist_path = Path(self.persist_dir)
            persist_path.mkdir(parents=True, exist_ok=True)
            index.storage_context.persist(persist_dir=str(persist_path))
            
            ## 메타데이터(Manifest) 구성
            metadata = {
                "engine": "llama-index",
                "indexed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "persist_dir": str(persist_path.absolute()),
                "total_nodes": len(ev.llama_nodes),
                "source_directories": list(set([str(p.parent.name) for p in ev.target_files])) # 추출된 폴더 목록 등
            }
            
            ## METADATA_ROOT에 상태 파일 기록
            metadata_file = METADATA_ROOT / "llama_manifest.json"
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
                
            return f"Index persisted and metadata written to {metadata_file.name}"

        status_msg = await asyncio.to_thread(_build_and_record_metadata)
        log.info(f"[Step 3-B] 완료: {status_msg}")
        return LlamaIndexedEvent(status=status_msg)

    @step
    async def finalize(self, ctx: Context, ev: Event) -> StopEvent:
        """Step 4: 양쪽 인덱싱이 모두 끝날 때까지 기다린 후 최종 결과를 반환합니다."""
        ## ctx.collect_events는 지정된 두 이벤트가 모두 도착해야만 리스트를 반환
        events = ctx.collect_events(ev, [XorIndexedEvent, LlamaIndexedEvent])
        
        if events is None:
            ## 아직 하나의 이벤트만 도착한 상태이므로 대기
            return None 

        ## 이벤트 언패킹
        xor_ev, llama_ev = events
        return StopEvent(result={
            "xor_status": xor_ev.status,
            "llama_status": llama_ev.status,
            "message": "이중 인덱싱(Dual Indexing)이 성공적으로 완료되었습니다."
        })

async def run_pipeline(repo_name: str):
    """위상적 보호막(Membrane) 내에서 워크플로우를 실행"""
    root = SELF_ROOT / Path(repo_name)
    if not root.exists():
        log.error(f"Directory not found: {root}")
        sys.exit(1)

    workflow = DualIngestionWorkflow(timeout=600.0)

    ## 지능형 접합 (Smart Bounding)
    ## - bounding 내부에서 MdPhiTranscript가 workflow.__doc__을 파싱합니다.
    ## - 주석의 @phase.flow(YAML)에 정의된 analyze_repository(retry:5) 등이 실체화됩니다.
    ## - xor_client 역시 동시에 접합하여 분산 인덱싱의 불안정성을 제어합니다.
    with folding(workflow, workflow.xor_client, re_entry_limit=3) as (b_workflow, b_xor):
        log.info(f"[System] 위상 전사 완료. '{repo_name}' 파이프라인 가동.")
        result = await b_workflow.run(target_dir=str(root))
    
    ## 결과 출력
    print("\n" + "="*30)
    print("[Workflow Result Summary]")
    print(f"- Xor status:   {result.get('xor_status')}")
    print(f"- Llama status: {result.get('llama_status')}")
    print(f"- Final Msg:    {result.get('message')}")
    print("="*30)

def main():
    parser = argparse.ArgumentParser(description="Dual Indexing Pipeline with Topological Bound")
    parser.add_argument("--repo", required=True, help="분석할 레포지토리 이름")
    args = parser.parse_args()
    
    try:
        asyncio.run(run_pipeline(args.repo))
    except Exception as e:
        log.critical(f"[System Failure] 위상 붕괴: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()