# phase.wasm.builder
import os
import shutil
import re
import json
import time
from pathlib import Path
from typing import Tuple

from phase.bind.resolver import resolve_path
from watcher.tracer.bound import BaseTracer

THEORIA_ROOT = resolve_path("theoria")
SANDBOX_ROOT = resolve_path("sandbox")
WASM_DIR = THEORIA_ROOT / "dphi"
WASM_TARGET_DIR = WASM_DIR / "target" / "wasm32-unknown-unknown" / "release"
WASM_BUILD_FILE = WASM_TARGET_DIR / "dphi.wasm"
DEST_WASM_FILE = SANDBOX_ROOT / "dphi.wasm"
REGISTRY_FILE = SANDBOX_ROOT / "wasm_registry.json"

class WasmBuilder(BaseTracer):
    """WASM 컴파일과 API 레지스트리 생성을 전담하는 빌더 페이즈"""
    def __init__(self, timeout: int = 120):
        super().__init__(tracer_name="wasm.builder", timeout=timeout)
        self.build_error = "" # 에이전트에게 전달할 에러 컨텍스트
        
        cargo_path = str(Path.home() / ".cargo" / "bin")
        if cargo_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{cargo_path}:{os.environ.get('PATH', '')}"

    def generate_registry(self) -> bool:
        space_rs_path = WASM_DIR / "src" / "space.rs"
        if not space_rs_path.exists():
            self.build_error = f"Source file not found: {space_rs_path}"
            self.log.error(f"[ERROR] {self.build_error}")
            return False

        with open(space_rs_path, "r", encoding="utf-8") as f:
            content = f.read()

        match = re.search(r"enum\s+Method\s*\{([^}]+)\}", content)
        if not match:
            self.log.warning("[WARN] 'enum Method' not found. Registry not generated.")
            return True # 치명적 에러는 아님

        methods = []
        for line in match.group(1).split("\n"):
            line = line.split(",")[0].strip()
            if line and not line.startswith("//"):
                snake = re.sub(r'(?<!^)(?=[A-Z])', '_', line).lower()
                methods.append(snake)

        os.makedirs(SANDBOX_ROOT, exist_ok=True)
        
        reg_data = {}
        if REGISTRY_FILE.exists():
            with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                reg_data = json.load(f)
                
        reg_data["generated_at"] = time.time()
        reg_data["methods"] = methods

        with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(reg_data, f, indent=4)
            
        self.log.info(f"[Builder] Registry updated ({len(methods)} methods).")
        return True

    async def build_and_deploy(self) -> bool:
        # [개선] PhaseOp.sequence(strict=True) 대신 직접 제어하여 stderr를 캡처합니다.
        await self.boundary.run_command(
            ["rustup", "target", "add", "wasm32-unknown-unknown"], 
            cwd=str(WASM_DIR)
        )
        
        code, out, err = await self.boundary.run_command(
            ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
            cwd=str(WASM_DIR), capture=True
        )
        
        if code != 0:
            self.build_error = err
            self.log.error(f"[Builder] Compilation Failed:\n{err[:500]}...")
            return False

        os.makedirs(SANDBOX_ROOT, exist_ok=True)
        shutil.copy2(WASM_BUILD_FILE, DEST_WASM_FILE)
        self.log.info(f"[Builder] Copied artifact -> {DEST_WASM_FILE.name}")
        return True

    async def execute(self) -> None:
        self.log.info("\n--- [START] Compiling WASM Artifact ---")
        if not self.generate_registry():
            self.rupture_confirmed = True
            return
            
        if not await self.build_and_deploy():
            self.rupture_confirmed = True