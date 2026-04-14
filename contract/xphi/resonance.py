# contract.xphi.resonance
XPHI = {
    ## 0. entry (control plane, not HTTP)
    "contract_judgment": {
        "type": "judgment",
        "spec": {
            "role": "control_plane",
            "next": "gen_ator",
            "rules": {
                "retry": "gen_ator",
                "ok": "align"
            },
            "context": {
                "surface": "redis",
                "channel": "loop:decision"
            },
            "operator": "contract.judgment"
        }
    },
    ## 1. Ψ_gen (worker)
    "gen_ator": {
        "type": "ator",
        "spec": {
            "role": "generator",
            "next": "critic_ator",
            "context": {
                "instruction": "generate solution",
                "temperature": 0.9,
                "surface": "redis",
                "emit_channel": "loop:gen"
            },
            "operator": "genai.transductor"
        }
    },
    ## 2. Ψ_crit (worker)
    "critic_ator": {
        "type": "ator",
        "spec": {
            "role": "critic",
            "next": "resonance_contract",
            "context": {
                "instruction": "critique solution",
                "temperature": 0.2,
                "surface": "redis",
                "emit_channel": "loop:crit"
            },
            "operator": "llm_transductor"
        }
    },
    ## 3. ∂Φ + Φ′ (evaluation service)
    "resonance_contract": {
        "type": "resonance",
        "spec": {
            "next": "contract_judgment",
            "context": {
                "surface": "redis",
                "consume": ["loop:gen", "loop:crit"],
                "emit_channel": "loop:decision"
            },
            "operator": "resonance_feedback"
        }
    },
    ## 4. Φ (materialization boundary)
    "align": {
        "type": "aligner",
        "spec": {
            "role": "materializer",
            "next": "contract_judgment",
            "context": {
                "surface": "redis",
                "emit_channel": "loop:state",
                "delay": 0.5  # burst 방지 (backpressure)
            },
            "operator": "file_writer"
        }
    }
}