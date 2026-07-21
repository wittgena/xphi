# sandbox.executor.command.terminal
from enum import Enum

class TerminalCommandStatus(Enum):
    """Status of a terminal command execution."""
    CONTINUE = "continue"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    NO_CHANGE_TIMEOUT = "no_change_timeout"
    HARD_TIMEOUT = "hard_timeout"
