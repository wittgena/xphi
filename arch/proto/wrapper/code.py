# arch.proto.wrapper.code
## @lineage: arch.proto.interpreter.code
## @lineage: gov.sandbox.debugger.interpreter.code
from typing import Any, Callable, Protocol, runtime_checkable

SIMPLE_TYPES = (str, int, float, bool, list, dict, type(None))

class CodeInterpreterError(RuntimeError):
    """Error raised during code interpretation.

    This exception covers two distinct failure modes:

    1. **Execution errors**: The sandbox ran user code that failed.
       - NameError, TypeError, ValueError, etc.
       - Tool call failures (unknown tool, tool raised exception)
       - These are normal user code errors.

    2. **Protocol errors**: Communication between host and sandbox failed.
       - Malformed JSON from sandbox
       - Sandbox process crashed or became unresponsive
       - Invalid JSON-RPC message structure
       - These may indicate a corrupted sandbox needing restart.

    The error message typically includes the original error type (e.g., "NameError: ...")
    which can help distinguish the failure mode.

    Note: SyntaxError is raised separately (not wrapped) for invalid Python syntax.
    """


class FinalOutput:
    def __init__(self, output: Any):
        self.output = output

    def __repr__(self) -> str:
        return f"FinalOutput({self.output!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FinalOutput):
            return NotImplemented
        return self.output == other.output


@runtime_checkable
class CodeInterpreter(Protocol):
    """Protocol for code execution environments (interpreters).

    Implementations must provide:
    - start(): Initialize the interpreter (optional, can be lazy)
    - execute(): Run code and return results
    - shutdown(): Clean up resources

    Lifecycle:
        1. Create instance (config only, no resources allocated)
        2. start() - Initialize interpreter (explicit) or let execute() do it (lazy)
        3. execute() - Run code (can be called many times)
        4. shutdown() - Release resources
    """

    @property
    def tools(self) -> dict[str, Callable[..., str]]:
        ...

    def start(self) -> None:
        ...

    def execute(
        self,
        code: str,
        variables: dict[str, Any] | None = None,
    ) -> Any:
        ...

    def shutdown(self) -> None:
        ...
