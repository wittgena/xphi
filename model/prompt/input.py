# ops.input
# @desc: MetaFlow-aligned Context Assembly with Budget Resolution
from dataclasses import dataclass
from typing import Dict, List
import argparse
import json

## @data.contracts
@dataclass
class InputBundle:
    anchor: str
    query: str
    state: List[str]
    evidence: List[str]
    max_tokens: int = 4000

@dataclass
class AssembledContext:
    anchor: str
    query: str
    state: List[str]
    evidence: List[str]

## @bound_definition
def define_bound(bundle: InputBundle) -> Dict:
    """
    Anchor & Query are structural invariants.
    """
    return {
        "anchor": bundle.anchor.strip(),
        "query": bundle.query.strip()
    }

## @state_consolidation
def consolidate_state(bundle: InputBundle, max_items: int = 8) -> List[str]:
    """
    State compression with order preservation.
    """
    unique = []
    seen = set()

    for s in bundle.state:
        s_clean = s.strip()
        if s_clean and s_clean not in seen:
            unique.append(s_clean)
            seen.add(s_clean)

    return unique[:max_items]

## @evidence_selection
def score_evidence(text: str) -> int:
    """
    Simple structural scoring heuristic.
    """
    score = 0
    if "def " in text:
        score += 3
    if "class " in text:
        score += 3
    if "IR" in text or "Anchoring" in text:
        score += 2
    if len(text) > 500:
        score += 1
    return score

def select_evidence(bundle: InputBundle, max_items: int = 6) -> List[str]:
    """
    Priority-based evidence selection.
    """
    scored = sorted(
        bundle.evidence,
        key=score_evidence,
        reverse=True
    )
    return scored[:max_items]

## @budget_resolution
def estimate_size(anchor: str, state: List[str], evidence: List[str], query: str) -> int:
    return sum(len(x) for x in ([anchor, query] + state + evidence))

def resolve_budget(bound: Dict,
                   state: List[str],
                   evidence: List[str],
                   max_tokens: int) -> AssembledContext:
    """
    Budget logic:
    1. Anchor NEVER removed
    2. Query NEVER removed
    3. Evidence reduced first
    4. State reduced second
    """
    anchor = bound["anchor"]
    query = bound["query"]

    current_size = estimate_size(anchor, state, evidence, query)

    # Reduce evidence first
    while evidence and current_size > max_tokens:
        evidence.pop()
        current_size = estimate_size(anchor, state, evidence, query)

    # Then reduce state
    while state and current_size > max_tokens:
        state.pop()
        current_size = estimate_size(anchor, state, evidence, query)

    return AssembledContext(
        anchor=anchor,
        query=query,
        state=state,
        evidence=evidence
    )

## @ordered_emission
def emit_ordered(context: AssembledContext) -> List[Dict]:
    """
    Strict hierarchical emission.
    """
    messages = []

    # 1. Anchor
    if context.anchor:
        messages.append({
            "role": "system",
            "content": f"@anchor: {context.anchor}"
        })

    # 2. State
    if context.state:
        messages.append({
            "role": "system",
            "content": "@state.context:\n" + "\n".join(context.state)
        })

    # 3. Evidence
    if context.evidence:
        messages.append({
            "role": "system",
            "content": "primary.target:\n" + "\n\n".join(context.evidence)
        })

    # 4. Query (always last, always user)
    messages.append({
        "role": "user",
        "content": context.query
    })
    return messages

## @entry
def entry(bundle: InputBundle) -> List[Dict]:
    bound = define_bound(bundle)
    state = consolidate_state(bundle)
    evidence = select_evidence(bundle)

    resolved = resolve_budget(
        bound,
        state,
        evidence,
        bundle.max_tokens
    )

    return emit_ordered(resolved)

## @main
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--anchor", default="")
    parser.add_argument("--query", required=True)
    parser.add_argument("--max_tokens", type=int, default=4000)

    args = parser.parse_args()

    bundle = InputBundle(
        anchor=args.anchor,
        query=args.query,
        state=[],
        evidence=[],
        max_tokens=args.max_tokens
    )

    messages = entry(bundle)
    print(json.dumps(messages, indent=2, ensure_ascii=False))

## @runtime
if __name__ == "__main__":
    main()