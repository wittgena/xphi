# phase.wasm.resolver.adapter
import canonicaljson

class StateAdapter:
    """
    @module: StateAdapter
    @desc: Strict schema adapter ensuring Python dictionary compliance with Rust WASM FFI struct memory layouts.
    """

    @staticmethod
    def to_canonical_bytes(data: dict) -> bytes:
        """Enforce RFC 8785 Canonical JSON (JCS) for deterministic hashing."""
        return canonicaljson.encode_canonical_json(data)

    @staticmethod
    def _assert_uint32(val: int, name: str):
        """Rust u32(0 ~ 4,294,967,295) 경계값 검증"""
        if val is not None and not (0 <= val <= 4294967295):
            raise ValueError(f"[FFI Type Error] '{name}' must be a uint32, got {val}")

    @staticmethod
    def build_topos_context(timestamp: int, injected_anchor: int = None, injected_tick: int = None) -> dict:
        """@target: Rust `ToposContext` (Influx Context)"""
        StateAdapter._assert_uint32(injected_tick, "injected_tick")
        return {
            "timestamp": timestamp,
            "injected_anchor": injected_anchor,
            "injected_tick": injected_tick
        }

    @staticmethod
    def build_parity_triplet(topos_id: str, phase_id: int, nexus_id: int) -> dict:
        """@target: Rust `ParityTriplet`"""
        StateAdapter._assert_uint32(phase_id, "phase_id")
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        return {
            "topos_id": topos_id,
            "phase_id": phase_id,
            "nexus_id": nexus_id
        }

    @staticmethod
    def build_core_node(name: str, content: str, children: dict = None) -> dict:
        return {"name": name, "kind": "CORE", "content": content, "ref_target": None, "children": children or {}}

    @staticmethod
    def build_symlink_node(name: str, ref_target: str, children: dict = None) -> dict:
        return {"name": name, "kind": "SYMLINK", "content": None, "ref_target": ref_target, "children": children or {}}

    @staticmethod
    def build_repo_commit(nexus_id: int, parent_nexus_id: int, parent_commit_id: str) -> dict:
        """
        @target: Rust `RepoCommit` struct
        @usage: Used to generate canonical bytes for `inscribe_actor` signatures.
        """
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        
        return {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id or 0,
            "parent_commit_id": parent_commit_id
        }

    @staticmethod
    def build_anchor_commit(parity: dict, parent_nexus_id: int, parent_commit_id: str, repos: dict, cached_states: dict = None) -> dict:
        """
        @target: Rust `AnchorCommit` struct
        @usage: Used to generate canonical bytes for `seal_epoch` signatures.
        """
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        
        # Rust의 BTreeMap<String, String> 호환을 위해 값들을 문자열로 강제 변환
        safe_repos = {str(k): str(v) for k, v in (repos or {}).items()}
        safe_cached_states = {str(k): str(v) for k, v in (cached_states or {}).items()}

        return {
            "parity": parity,
            "parent_nexus_id": parent_nexus_id or 0,
            "parent_commit_id": parent_commit_id,
            "repos": safe_repos,
            "cached_states": safe_cached_states
        }

    @staticmethod
    def build_inscribe_payload(nexus_id: int, parent_nexus_id: int, parent_commit_id: str, pubkey: str, signature: str) -> dict:
        """@target: Rust `InscribePayload` (Membrane Entrypoint)"""
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        
        return {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id,
            "parent_commit_id": parent_commit_id,
            "pubkey": pubkey,
            "signature": signature
        }

    @staticmethod
    def build_seal_epoch_payload(parity: dict, parent_nexus_id: int, self_parent_state: str, repos: dict, cached_states: dict, timestamp: float, pubkey: str, signature: str) -> dict:
        """@target: Rust `SealEpochPayload` (Membrane Entrypoint)"""
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        
        # Rust Map 직렬화 에러 방지
        safe_repos = {str(k): str(v) for k, v in (repos or {}).items()}
        safe_cached_states = {str(k): str(v) for k, v in (cached_states or {}).items()}

        return {
            "parity": parity,
            "parent_nexus_id": parent_nexus_id,
            "self_parent_state": self_parent_state,
            "repos": safe_repos,
            "cached_states": safe_cached_states,
            "timestamp": timestamp,
            "pubkey": pubkey,
            "signature": signature
        }

    @staticmethod
    def build_trans_rule(src: str, dest: str, kind: str) -> dict:
        """
        @target: Rust `TransRule` struct
        @constraint: `kind` MUST strictly be one of ["CORE", "SYMLINK", "ANCHOR"]
        """
        valid_kinds = {"CORE", "SYMLINK", "ANCHOR"}
        if kind not in valid_kinds:
            raise ValueError(f"[Schema Error] Invalid NodeType '{kind}'. Must be one of {valid_kinds}")
            
        return {
            "src": src,
            "dest": dest,
            "kind": kind
        }

    @staticmethod
    def build_evolution_context(phase_root: dict, external_rules: list = None) -> dict:
        """@target: Rust `EvolutionContext` struct"""
        return {
            "phase_root": phase_root,
            "external_rules": external_rules or []
        }

    @staticmethod
    def build_transition_payload(intent_action: str, intent_payload: dict, evolution_ctx: dict) -> dict:
        """@target: Rust `TransitionPayload` struct (Top-level FFI)"""
        return {
            "intent_action": intent_action,
            "intent_payload": intent_payload,
            "evolution_ctx": evolution_ctx
        }

    @staticmethod
    def adapt_swarm_to_phase_root(commit_hash: str, agents_dict: dict) -> dict:
        children = {
            agent_name: StateAdapter.build_core_node(agent_name, str(agent_hash))
            for agent_name, agent_hash in agents_dict.items()
        }
        return StateAdapter.build_core_node("swarm_root", commit_hash, children)

    @staticmethod
    def adapt_ecosystem_to_phase_root(epoch_state: str, proposal_ipfs_hash: str) -> dict:
        children = {
            "pending_proposal": StateAdapter.build_symlink_node("pending_proposal", proposal_ipfs_hash)
        }
        return StateAdapter.build_core_node("ecosystem_root", epoch_state, children)

    @staticmethod
    def adapt_provenance_to_phase_root(commit_hash: str, repos_dict: dict) -> dict:
        children = {
            repo_name: StateAdapter.build_core_node(repo_name, str(repo_hash))
            for repo_name, repo_hash in repos_dict.items()
        }
        return StateAdapter.build_core_node("provenance_root", commit_hash, children)