# phase.runtime.inter.protocol
## @lineage: arch.xor.proto.code
import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

PRIMITIVE_TYPES = (str, int, float, bool, list, dict, tuple, type(None))

class SandboxError(RuntimeError):
    """샌드박스 환경 예외의 기저 클래스"""
    pass

class ExecutionError(SandboxError):
    """
    실행 위상 오류 (Execution Error)
    - 주입된 코드의 런타임 오류 (NameError, TypeError 등)
    - 주입된 Callable 객체의 실행 실패
    """
    pass

class ProtocolError(SandboxError):
    """
    인프라/통신 위상 오류 (Protocol Error)
    - 호스트와 샌드박스 간 IPC 붕괴, 잘못된 JSON-RPC 포맷
    - 프로세스 크래시 등 샌드박스 재생성이 필요한 상태
    """
    pass


# ==========================================
# Data Flow: 불변의 결과 컨테이너
# ==========================================
@dataclass(frozen=True)
class ExecutionResult:
    """
    코드 실행의 순수한 변환 결과.
    이 데이터의 재사용/폐기 여부는 현재 위상에서 결정되지 않음.
    frozen=True를 통해 반환 이후의 사이드 이펙트를 원천 차단.
    """
    success: bool
    output: Any | None = None
    error: ExecutionError | None = None


# ==========================================
# Interface: 상태 없는(Stateless) 실행기 규약
# ==========================================
@runtime_checkable
class CodeInterpreter(Protocol):
    """
    사이드 이펙트가 없는 순수 실행 환경 프로토콜.
    상태(callables, variables)는 인스턴스에 저장되지 않고, 오직 실행 시점에만 주입됨.
    """

    def start(self) -> None:
        """실행 환경의 인프라 초기화"""
        ...

    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
    ) -> ExecutionResult:
        """
        주입된 데이터(variables)와 함수(callables)를 컨텍스트로 삼아 코드를 실행.
        
        - Mutable한 dict 대신 Mapping을 사용하여 인터페이스 레벨에서 조작(Side-effect) 방지.
        - 결과는 구조화된 ExecutionResult로 반환.
        """
        ...

    def shutdown(self) -> None:
        """실행 환경의 자원 해제"""
        ...

import json
from typing import Any, Mapping

class JsonRpcErrorCode:
    """
    JSON-RPC 2.0 표준 및 애플리케이션 에러 코드.
    매직 넘버를 배제하고 위상적으로 오류 타입을 분류합니다.
    """
    # Standard JSON-RPC 2.0 Errors
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    # Application Specific Errors (Sandbox)
    SYNTAX_ERROR = -32000
    NAME_ERROR = -32001
    TYPE_ERROR = -32002
    VALUE_ERROR = -32003
    ATTRIBUTE_ERROR = -32004
    INDEX_ERROR = -32005
    KEY_ERROR = -32006
    RUNTIME_ERROR = -32007
    UNKNOWN = -32099

    @classmethod
    def from_exception_type(cls, error_type: str) -> int:
        """예외 타입 문자열을 기반으로 적절한 RPC 에러 코드를 매핑합니다."""
        attr_name = error_type.replace("Error", "_ERROR").upper()
        return getattr(cls, attr_name, cls.UNKNOWN)


class JsonRpcMessage:
    """
    JSON-RPC 2.0 페이로드를 생성하는 순수(Stateless) 빌더 클래스.
    상태를 가지지 않으며(Side-effect free), 오직 규격에 맞는 직렬화된 문자열만 반환합니다.
    """
    VERSION = "2.0"

    @classmethod
    def request(cls, method: str, params: Mapping[str, Any], msg_id: int | str) -> str:
        """응답을 기대하는 요청(Request) 메시지를 생성합니다."""
        return json.dumps({
            "jsonrpc": cls.VERSION,
            "method": method,
            "params": params,
            "id": msg_id
        })

    @classmethod
    def notification(cls, method: str, params: Mapping[str, Any] | None = None) -> str:
        """응답을 기대하지 않는 단방향 알림(Notification) 메시지를 생성합니다."""
        msg: dict[str, Any] = {
            "jsonrpc": cls.VERSION, 
            "method": method
        }
        if params:
            msg["params"] = params
        return json.dumps(msg)

    @classmethod
    def result(cls, result: Any, msg_id: int | str) -> str:
        """성공적인 실행 결과(Result) 메시지를 생성합니다."""
        return json.dumps({
            "jsonrpc": cls.VERSION,
            "result": result,
            "id": msg_id
        })

    @classmethod
    def error(
        cls, 
        code: int, 
        message: str, 
        msg_id: int | str | None, 
        data: Mapping[str, Any] | None = None
    ) -> str:
        """실패한 실행의 에러(Error) 메시지를 생성합니다."""
        err: dict[str, Any] = {
            "code": code, 
            "message": message
        }
        if data:
            err["data"] = data
        return json.dumps({
            "jsonrpc": cls.VERSION,
            "error": err,
            "id": msg_id
        })