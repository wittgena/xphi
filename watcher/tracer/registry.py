# watcher.tracer.registry
## @lineage: topos.audit.tracer.registry
from kernel.bind.resolver import resolve_path

class TargetRegistry:
    _RULESET_RUSTC_OOM = {
        "targets": [
            {
                "tag": "rustc-recursion-depth",
                "keywords": [{"AND": ["SkipWhile"]}],
                "action": "count_max"
            },
            {
                "tag": "rustc-fatal-limit",
                "keywords": [{"AND": ["reached the recursion limit"]}],
                "action": "trigger_rupture"
            }
        ]
    }
    _RULESET_WASM_OOM = {
         "targets": [
            {
                "tag": "wasm-fatal-panic",
                "keywords": [{"OR": ["stack overflow", "panic", "unreachable"]}],
                "action": "trigger_rupture"
            }
        ]
    }
    CONFIGS = {
        "oom": {
            "infra_type": "docker",
            "workspace_suffix": "oom",
            "image_name": "surgent-rust-target",
            "container_name": "surgent_rustc_target",
            "env_vars": ["-e", "RUSTC_LOG=rustc_trait_selection=info"],
            "mem_limit": "64m",
            "verify_type": "rustc_recursion",
            "desc": "Rustc trait selection infinite recursion OOM",
            "ruleset": _RULESET_RUSTC_OOM
        },
        "wasm_e0": {
            "infra_type": "docker",
            "workspace_suffix": "wasm_e0",
            "image_name": "surgent-wasm-target",
            "container_name": "surgent_wasm_target",
            "env_vars": ["-e", "CRANELIFT_LOG=debug", "-e", "RUST_BACKTRACE=1"],
            "mem_limit": "128m",
            "verify_type": "cranelift_loop",
            "desc": "Cranelift e-graph optimization loop OOM",
            "ruleset": _RULESET_WASM_OOM
        },
        "wasm_livelock": {
            "infra_type": "compose",
            "workspace_suffix": "resonance",
            "compose_file": "docker-compose.wasm.yml",
            "container_name": "wasm_runner",
            "verify_type": "temporal_fixation",
            "desc": "Wasmtime #11701 (WASI Adapter Deterministic Livelock)"
        },
        "polkadot_gas": {
            "infra_type": "compose",
            "workspace_suffix": "resonance",
            "compose_file": "docker-compose.polkadot.yml",
            "container_name": "polkadot_node",
            "verify_type": "consensus_divergence",
            "desc": "Polkadot-SDK #11525 (Validator vs Execution Gas Mismatch)"
        },
        "repro_worker": {
            "infra_type": "compose",
            "workspace_suffix": "resonance",
            "compose_file": "docker-compose.yml", # Default repro compose
            "verify_type": "leak_detection",
            "desc": "Worker delayed message stabilization & leak detection"
        },
        "wasm_autonomous": {
            "infra_type": "wasm_native",           # 서브프로세스(Docker) 대신 파이썬 프로세스 내부에 Wasmtime 구동
            "workspace_suffix": "wasm_sandbox",    # 샌드박스 워크스페이스 마운트 경로
            "target_wasm": "dphi.wasm",      # 구동할 타겟 바이너리
            "limits": {
                "fuel": 10_000_000,                # 기본 STANDARD 티어 제약 (1천만 틱)
                "mem_limit": "64m"                 # 선형 메모리 한계 (64MB)
            },
            "verify_type": "semantic_hang",        # 텔레메트리를 통한 Livelock/Trap 집중 관측
            "desc": "Autonomous Self-Healing WASM Agent Loop (In-Process)",
            "ruleset": _RULESET_WASM_OOM           # FFI 통신 중 떨어지는 로그 파싱용 (Fallback)
        }
    }

    @classmethod
    def get(cls, target_name: str) -> dict:
        """@desc: 지정된 위상의 설정값을 반환하며, 런타임에 필요한 절대 경로들을 동적으로 주입합니다."""
        if target_name not in cls.CONFIGS:
            raise ValueError(f"Unknown target topology: '{target_name}'. Available: {list(cls.CONFIGS.keys())}")
        
        config = cls.CONFIGS[target_name].copy()
        suffix = config.get("workspace_suffix", "default")
        config["workspace_path"] = str(resolve_path("workspace") / "repro" / suffix)
        if config["infra_type"] == "wasm_native":
            sandbox_root = resolve_path("sandbox")
            config["sandbox_root"] = str(sandbox_root)
            config["target_wasm_path"] = str(sandbox_root / config["target_wasm"])
            
        return config