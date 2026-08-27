# xphi.arch.model.message
## @lineage: arch.model.message
import json
import logging
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, ClassVar, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator

log = logging.getLogger(__name__)

DEFAULT_TEXT_CONTENT_LIMIT = 50000

class MessageToolCall(BaseModel):
    id: str = Field(..., description="Canonical tool call id")
    name: str = Field(..., description="Tool/function name")
    arguments: str = Field(..., description="JSON string of arguments")
    origin: Literal["completion", "responses"] = Field(..., description="Originating API family")

    def to_chat_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": "function",
            "function": {
                "name": self.name,
                "arguments": self.arguments,
            },
        }

    def to_responses_dict(self) -> dict[str, Any]:
        resp_id = self.id if str(self.id).startswith("fc") else f"fc_{self.id}"
        args_str = (
            self.arguments
            if isinstance(self.arguments, str)
            else json.dumps(self.arguments)
        )
        return {
            "type": "function_call",
            "id": resp_id,
            "call_id": resp_id,
            "name": self.name,
            "arguments": args_str,
        }

class ThinkingBlock(BaseModel):
    type: Literal["thinking"] = "thinking"
    thinking: str = Field(..., description="The thinking content")
    signature: str | None = Field(default=None, description="Cryptographic signature")

class RedactedThinkingBlock(BaseModel):
    type: Literal["redacted_thinking"] = "redacted_thinking"
    data: str = Field(..., description="The redacted thinking content")

class ReasoningItemModel(BaseModel):
    id: str | None = Field(default=None)
    summary: list[str] = Field(default_factory=list)
    content: list[str] | None = Field(default=None)
    encrypted_content: str | None = Field(default=None)
    status: str | None = Field(default=None)

class BaseContent(BaseModel):
    cache_prompt: bool = False

    @abstractmethod
    def to_llm_dict(self) -> list[dict[str, Any]]:
        """Convert to LLM API format."""

class TextContent(BaseContent):
    type: Literal["text"] = "text"
    text: str
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", populate_by_name=True)

    def to_llm_dict(self) -> list[dict[str, Any]]:
        data: dict[str, Any] = {
            "type": self.type,
            "text": self.text,
        }
        if self.cache_prompt:
            data["cache_control"] = {"type": "ephemeral"}
        return [data]

class ImageContent(BaseContent):
    type: Literal["image"] = "image"
    image_urls: list[str]

    def to_llm_dict(self) -> list[dict[str, Any]]:
        images: list[dict[str, Any]] = []
        for url in self.image_urls:
            images.append({"type": "image_url", "image_url": {"url": url}})
        if self.cache_prompt and images:
            images[-1]["cache_control"] = {"type": "ephemeral"}
        return images

class Message(BaseModel):
    role: Literal["user", "system", "assistant", "tool", "environment", "watcher"]
    content: Sequence[TextContent | ImageContent] = Field(default_factory=list)
    tool_calls: list[MessageToolCall] | None = None
    tool_call_id: str | None = None
    name: str | None = None
    reasoning_content: str | None = Field(default=None)
    thinking_blocks: Sequence[ThinkingBlock | RedactedThinkingBlock] = Field(default_factory=list)
    responses_reasoning_item: ReasoningItemModel | None = Field(default=None)

    model_config = ConfigDict(extra="ignore")

    @property
    def contains_image(self) -> bool:
        return any(isinstance(content, ImageContent) for content in self.content)

    @field_validator("content", mode="before")
    @classmethod
    def _coerce_content(cls, v: Any) -> Sequence[TextContent | ImageContent] | Any:
        if v is None:
            return []
        if isinstance(v, str):
            return [TextContent(text=v)]
        return v

    def _maybe_truncate_tool_text(self, text: str) -> str:
        if not text or len(text) <= DEFAULT_TEXT_CONTENT_LIMIT:
            return text
        log.warning(
            "Tool TextContent length (%s) exceeds limit (%s).",
            len(text), DEFAULT_TEXT_CONTENT_LIMIT
        )
        return text[:DEFAULT_TEXT_CONTENT_LIMIT] + "..."

def content_to_str(contents: Sequence[TextContent | ImageContent]) -> list[str]:
    text_parts = []
    for content_item in contents:
        if isinstance(content_item, TextContent):
            text_parts.append(content_item.text)
        elif isinstance(content_item, ImageContent):
            text_parts.append(f"[Image: {len(content_item.image_urls)} URLs]")
    return text_parts