# xphi.arch.bound.adapter.fsm
import json
from typing import Dict, Any, List, Optional, Union

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.fsm")

class FsmTarget:
    COMPILE = "FSM_COMPILE"
    STEP = "FSM_STEP"
    SANDBOX = "FSM_SANDBOX"
    HOST_BRIDGE = "HOST_BRIDGE"

class FsmOp:
    STEP = "STEP"
    FORK = "FORK"
    MERGE = "MERGE"
    HITL_PARK = "HITL_PARK"

class FsmAdapter:
    """표준 Calldata / State Snapshot / Unified Payload 빌더 어댑터"""

    @staticmethod
    def build_step_calldata(
        curr_node: str,
        step_cost: int = 0,
        meta_ctx: Optional[Dict[str, Any]] = None,
        call_stack: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """단일 1:1 상태 전이(CORE / SYMLINK / RETURN)를 위한 Calldata 생성"""
        return {
            "op": FsmOp.STEP,
            "curr_node": curr_node,
            "step_cost": int(step_cost),
            "meta_ctx": dict(meta_ctx) if meta_ctx else {},
            "call_stack": list(call_stack) if call_stack else [],
        }

    @staticmethod
    def build_fork_calldata(
        curr_node: str,
        step_cost: int = 0,
        fuel_ratios: Optional[List[float]] = None,
        meta_ctx: Optional[Dict[str, Any]] = None,
        call_stack: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """1:N 멀티버스 가설 분기 및 원자적 연료 분할(_slice_fuel)을 위한 Calldata 생성"""
        payload: Dict[str, Any] = {
            "op": FsmOp.FORK,
            "curr_node": curr_node,
            "step_cost": int(step_cost),
            "meta_ctx": dict(meta_ctx) if meta_ctx else {},
            "call_stack": list(call_stack) if call_stack else [],
        }
        if fuel_ratios is not None:
            payload["fuel_ratios"] = [float(r) for r in fuel_ratios]
        return payload

    @staticmethod
    def build_hitl_park_calldata(
        curr_node: str,
        meta_ctx: Optional[Dict[str, Any]] = None,
        call_stack: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """HTTP 202 Pre-Flight 인터럽트 시 Zero-Spend 파킹을 위한 Calldata 생성"""
        return {
            "op": FsmOp.HITL_PARK,
            "curr_node": curr_node,
            "step_cost": 0,  # 파킹 시 연료 소모는 무조건 0 보장
            "meta_ctx": dict(meta_ctx) if meta_ctx else {},
            "call_stack": list(call_stack) if call_stack else [],
        }

    @staticmethod
    def build_merge_calldata(
        join_node: str,
        arriving_cursors: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        DagJoinBarrier에서 N개 브랜치를 합류(N:1 Collapse)하고
        승자(_score 기준) 선택 및 잔여 연료를 100% 회수하기 위한 Calldata 생성
        """
        return {
            "op": FsmOp.MERGE,
            "curr_node": join_node,
            "step_cost": 0,
            "arriving_cursors": arriving_cursors,
            "meta_ctx": {},
            "call_stack": [],
        }

    # State Snapshot Builders (기존 build_evm_account_data 대응)
    @staticmethod
    def build_runtime_snapshot(
        branches: Dict[str, Dict[str, Any]],
        barriers: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        return {
            "branches": branches,
            "barriers": barriers or {},
        }

    @staticmethod
    def build_catalog_snapshot(
        valid_mcp_uris: Optional[List[str]] = None,
        verified_subgraphs: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """컴파일(FSM_COMPILE) 시 정적 Linter에 주입할 사내 카탈로그 스냅샷 포맷팅"""
        return {
            "valid_mcp_uris": valid_mcp_uris or [],
            "verified_subgraphs": verified_subgraphs or {},
        }

    @staticmethod
    def build_fsm_payload(
        target_address: str,
        calldata: Union[Dict[str, Any], str],
        state_snapshot: Optional[Dict[str, Any]] = None,
        vm_target: str = FsmTarget.STEP,
    ) -> Dict[str, Any]:
        return {
            "vm_target": vm_target,
            "target_address": target_address,
            "calldata": calldata,
            "state_snapshot": state_snapshot or {},
        }

    @classmethod
    def build_compile_payload(
        cls,
        trace_id: str,
        compact_ir: Dict[str, Any],
        catalog_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """ExFSMCompiler가 토폴로지 2KB 분할 및 데드락 검증을 요청할 때 사용하는 원샷 빌더"""
        return cls.build_fsm_payload(
            target_address=trace_id,
            calldata=compact_ir,
            state_snapshot=catalog_snapshot or {},
            vm_target=FsmTarget.COMPILE,
        )

    @staticmethod
    def parse_broker_output(exec_result_output: str) -> Dict[str, Any]:
        """FSMInterpreter가 반환한 중첩 JSON 문자열(response_payload["output"])을 파이썬 Dict로 한 번에 풀어서 반환"""
        if not exec_result_output:
            return {}
        try:
            outer = json.loads(exec_result_output)
            if isinstance(outer, dict) and isinstance(outer.get("output"), str):
                with_inner = dict(outer)
                try:
                    with_inner["parsed_output"] = json.loads(outer["output"])
                except Exception:
                    with_inner["parsed_output"] = {}
                return with_inner
            return outer
        except Exception as e:
            log.warning(f"Failed to parse FSM output payload: {e}")
            return {}