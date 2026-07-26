# arch.xor.driver.error
class DriverError(Exception):
    message: str
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return self.message

class LLMNoResponseError(DriverError):
    def __init__(
        self,
        message: str = (
            "LLM did not return a response. This is only seen in Gemini models so far."
        ),
    ) -> None:
        super().__init__(message)