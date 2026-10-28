# xphi.node.runtime.wasm.method
from enum import Enum

class DphiMethod(str, Enum):
    EXECUTE_CODE = "execute_code"
    EXECUTE_FSM = "execute_fsm"
    COMPUTE_FINGERPRINT = "compute_fingerprint"
    GENERATE_TOPOS_ID = "generate_topos_id"
    GENERATE_PHASE_ID = "generate_phase_id"
    INIT_EPOCH = "init_epoch"
    INSCRIBE_ACTOR = "inscribe_actor"
    SEAL_EPOCH = "seal_epoch"
    VERIFY_PARITY = "verify_parity"
    EXECUTE_TRANSITION = "execute_transition"
    PROCESS_FIELD_DYNAMICS = "process_field_dynamics"