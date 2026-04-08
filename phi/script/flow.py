# phi.script.flow
## @phase: Φ_declared
PHI = {
    ## 1. Ψ_probe: 계약 관측
    "contract_prober": {
        "type": "aligner",
        "spec": {
            "role": "surface_sensor",
            "next": "execution_trigger",
            "context": {
                "endpoint": "http://0.0.0.0:8000/openapi.json"
            },
            "operator": "http_probe_aligner"
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
            "operator": "http_post_transductor"
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
            "operator": "http_get_transductor"
        }
    },
    ## 4. ∂Φ: mismatch detection (contract + execution 통합)
    "mismatch_resonance": {
        "type": "resonance",
        "spec": {
            "next": "topos_judgment",
            "operator": "event_closure_resonance"
            ## 의미:
            ## - openapi vs response
            ## - expected vs observed
            ## 둘 다 ∂Φ로 통합
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
            "operator": "topology_conclusion_router"
        }
    }
}