# model.surface.xphi.judgment
XPHI={
    "gen_ator": {
        "type": "ator",
        "spec": {
            "role": "generator",
            "next": "critic_ator",
            "context": { "instruction": "generate solution", "temperature": 0.9 }
        }
    },
    "critic_ator": {
        "type": "ator",
        "spec": {
            "role": "critic",
            "next": "resonance_contract",
            "context": { "instruction": "critique solution", "temperature": 0.2 }
        }
    },
    "resonance_contract": {
        "type": "resonance",
        "spec": {
            "next": "contract_judgment",
            "operator": "resonance.feedback"
        }
    },
    "contract_judgment": {
        "type": "judgment",
        "spec": {
            "rules": { "retry": "gen_ator", "ok": "align"},
            "operator": "contract.judgment"
        }
    },
    "align": {
        "type": "aligner",
        "spec": { "next": "UGA" }
    },
}
