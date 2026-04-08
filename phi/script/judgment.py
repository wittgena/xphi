# phi.script.judgment
"""
# @flow:
# Ψ₀
#  → gen_ator        # Ψ → Ψ_gen
#  → critic_ator     # Ψ → Ψ_crit
#  → resonance        # Ψ_gen ⊕ Ψ_crit → Δ → decision
#  → judgment           # decision judgment
#       ↙        ↘
#    retry       ok
#     ↓           ↓
#  gen_ator    align → Φ
"""
PHI={
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
            "operator": "resonance_feedback"
        }
    },
    "contract_judgment": {
        "type": "judgment",
        "spec": {
            "rules": { "retry": "gen_ator", "ok": "align"},
            "operator": "contract_judgment"
        }
    },
    "align": {
        "type": "aligner",
        "spec": { "next": "UGA" }
    },
}
