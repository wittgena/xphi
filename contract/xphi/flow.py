# contract.xphi.flow
## @phase: Φ_declared
XPHI = {
    ## 1. Ψ_probe: 계약 관측
    "contract_prober": {
        "type": "aligner",
        "spec": {
            "role": "surface_sensor",
            "next": "execution_trigger",
            "context": {
                "endpoint": "http://0.0.0.0:8000/openapi.json"
            },
            "operator": "http.probe.aligner"
        }
    },
    ## 2. Ψ_act: 실행 (genesis + inject + run 통합)
    "execution_trigger": {
        "type": "ator",
        "spec": {
            "role": "execution_driver",
            "next": "observation_trace",
            "context": {
                "instruction": "create conversation → inject message → run"
            },
            "operator": "http.post.transductor"
        }
    },

    ## 3. Ψ_obs: 결과 관측
    "observation_trace": {
        "type": "ator",
        "spec": {
            "role": "world_line_tracer",
            "next": "mismatch_resonance",
            "context": {
                "instruction": "GET events"
            },
            "operator": "http.get.transductor"
        }
    },
    ## 4. ∂Φ: mismatch detection (contract + execution 통합)
    "mismatch_resonance": {
        "type": "resonance",
        "spec": {
            "next": "topos_judgment",
            "operator": "event.closure.resonance"
        }
    },
    ## 5. Φ′ + δ: 판단 및 재진입
    "topos_judgment": {
        "type": "judgment",
        "spec": {
            "rules": {
                "stable": "UGA",              # Φ⁺
                "fracture": "contract_prober" # δ (re-entry)
            },
            "operator": "topology.conclusion.router"
        }
    }
}