# meta.flow.ops.transcript
"""@flow: Φ(config surface) → Ψ(transcription) → Ψ′(k8s projection)"""
import subprocess
import yaml
import sys
import shutil
import os
import re
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from session.bound.resolver import find_current_self, resolve_path
from phase.sphere.container.validator.compose import FlowValidator, SelfExtractor 
from session.contract.block.parser.md import MdAstParser
from session.contract.block.extractor import BlockExtractor
from meta.flow.surface.emitter import get_emitter

log = get_emitter("kube.loop")

def sanitize_name(name: str) -> str:
    """한글 및 특수문자를 제거하고 시스템 안전한 이름으로 변환"""
    # 1. 영문/숫자 외 제거 및 소문자화
    name = name.lower()
    name = re.sub(r'[^a-z0-9\-]', '-', name)
    # 2. 연속된 하이픈 제거 및 양끝 제거
    name = re.sub(r'-+', '-', name).strip('-')
    return name or "unnamed-service"

class TranscriptEngine:
    def __init__(self):
        self.self_root = find_current_self()
        self.outlet_root = resolve_path("k8s")
        self.ensure_env()

    def ensure_env(self):
        """환경 검사: 도구 설치 및 클러스터 연결 확인"""
        for tool in ["kompose", "kubectl"]:
            if shutil.which(tool) is None:
                log.error(f"[Ψ:critical] '{tool}'이 설치되어 있지 않습니다.")
                sys.exit(1)
        
        try:
            subprocess.run(["kubectl", "cluster-info"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            log.error("[Ψ:critical] K8s 클러스터에 연결할 수 없습니다. context를 확인하세요.")
            # 실패하더라도 변환(transcribe)까지는 수행할 수 있도록 종료는 선택사항

    def transcribe(self, service_id: str, compose_content: str) -> Optional[Path]:
        target_dir = self.outlet_root / service_id
        target_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"[Ψ:transcribe] '{service_id}' 변환 시작")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            tmp.write(compose_content)
            tmp_path = tmp.name

        try:
            # Kompose 실행 (기존 생성물 덮어쓰기 위해 --force-overwrite 권장)
            subprocess.run(
                ["kompose", "convert", "-f", tmp_path, "--out", str(target_dir)],
                check=True, capture_output=True, text=True
            )
            log.info(f"  [success] 매니페스트 생성 완료 -> {target_dir}")
            return target_dir
        except subprocess.CalledProcessError as e:
            log.error(f"  [fail] Kompose 변환 오류: {e.stderr}")
            return None
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)

class Projector:
    @staticmethod
    def apply(manifest_dir: Path):
        log.info(f"[Ψ':project] 클러스터 반영 시작: {manifest_dir.name}")
        try:
            # apply 전 전송용 로그
            result = subprocess.run(
                ["kubectl", "apply", "-f", str(manifest_dir)],
                check=True, capture_output=True, text=True
            )
            log.info(f"  [success] 클러스터 투영 완료.")
        except subprocess.CalledProcessError as e:
            # 에러 메시지 가독성 확보
            log.error(f"  [fail] kubectl apply 오류:\n{e.stderr.strip()}")

def loop(md_path: str):
    # 1. 검증
    validator = FlowValidator()
    if not validator.validate(md_path):
        log.error("[Ψ:abort] 검증 실패")
        return

    # 2. 데이터 추출
    doc = MdAstParser(Path(md_path)).parse()
    blocks = BlockExtractor().extract(doc)
    surface = SelfExtractor.extract(blocks)
    
    # 3. 식별자 결정 (id > name > filename)
    meta = surface["metadata"]
    raw_id = meta.get("id") or meta.get("name") or Path(md_path).stem
    service_id = sanitize_name(raw_id)
    compose_yaml = surface["raw_yaml"]
    engine = TranscriptEngine()
    output_path = engine.transcribe(service_id, compose_yaml)
    
    if output_path:
        Projector.apply(output_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log.info("Usage: python -m flow.res.transcript <md_path>")
    else:
        loop(sys.argv[1])