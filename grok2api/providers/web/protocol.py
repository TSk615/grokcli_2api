"""OpenAI Chat Completions to the evidenced Grok Web REST chat shape.

The reference project's pre-Gateway implementation posted this payload to
``/rest/app-chat/conversations/new``. Authentication deliberately remains in
HTTP headers; this module accepts no credential and copies no unknown fields.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .models import WebModel, get_web_model


class WebProtocolError(ValueError):
    """A bounded validation error that never includes request contents."""


@dataclass(frozen=True, slots=True, repr=False)
class ConvertedWebChat:
    """REST payload plus client response mode kept outside the upstream JSON."""

    public_model: str
    upstream_mode: str
    stream: bool
    payload: Mapping[str, Any] = field(repr=False)

    def __repr__(self) -> str:
        return (
            "ConvertedWebChat("
            f"public_model={self.public_model!r}, "
            f"upstream_mode={self.upstream_mode!r}, stream={self.stream!r}"
            ")"
        )


def _resolve_model(raw_model: Any) -> WebModel:
    model_id = str(raw_model or "").strip()
    if model_id.lower().startswith("web/"):
        model_id = model_id.split("/", 1)[1].strip()
    model = get_web_model(model_id)
    if model is None or not model.supports("chat"):
        raise WebProtocolError("unsupported Grok Web chat model")
    return model


def _content_text(content: Any, *, index: int) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if not isinstance(content, Sequence) or isinstance(content, (bytes, bytearray)):
        raise WebProtocolError(f"messages[{index}].content must be text or a text-part list")

    values: list[str] = []
    for part_index, part in enumerate(content):
        if not isinstance(part, Mapping):
            raise WebProtocolError(
                f"messages[{index}].content[{part_index}] must be an object"
            )
        part_type = str(part.get("type") or "").strip().lower()
        if part_type not in ("text", "input_text", "output_text"):
            raise WebProtocolError(
                f"messages[{index}].content[{part_index}] is not a supported text part"
            )
        text = part.get("text")
        if not isinstance(text, str):
            raise WebProtocolError(
                f"messages[{index}].content[{part_index}].text must be a string"
            )
        if text:
            values.append(text)
    return "\n".join(values)


def messages_to_prompt(messages: Any) -> str:
    """Serialize textual OpenAI history in the same role-tagged format as the reference."""

    if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes, bytearray)):
        raise WebProtocolError("messages must be a non-empty list")
    if not messages:
        raise WebProtocolError("messages must be a non-empty list")

    sections: list[str] = []
    allowed_roles = {"system", "developer", "user", "assistant", "tool"}
    for index, message in enumerate(messages):
        if not isinstance(message, Mapping):
            raise WebProtocolError(f"messages[{index}] must be an object")
        role = str(message.get("role") or "").strip().lower()
        if role not in allowed_roles:
            raise WebProtocolError(f"messages[{index}].role is unsupported")
        text = _content_text(message.get("content"), index=index)
        if role == "tool" and message.get("tool_call_id"):
            text = f"Tool result ({str(message['tool_call_id']).strip()}): {text}"
        if text.strip():
            sections.append(f"[{role}]\n{text}")
    if not sections:
        raise WebProtocolError("messages contain no supported text")
    return "\n\n".join(sections).strip()


def build_rest_chat_payload(message: str, mode: str) -> dict[str, Any]:
    """Build only fields present in the reference REST implementation."""

    return {
        "collectionIds": [],
        "disabledConnectorIds": [],
        "deviceEnvInfo": {
            "darkModeEnabled": False,
            "devicePixelRatio": 2,
            "screenHeight": 1328,
            "screenWidth": 2056,
            "viewportHeight": 1083,
            "viewportWidth": 2056,
        },
        "disableMemory": True,
        "disableSearch": False,
        "disableSelfHarmShortCircuit": False,
        "disableTextFollowUps": False,
        "enableImageGeneration": True,
        "enableImageStreaming": True,
        "enableSideBySide": True,
        "fileAttachments": [],
        "forceConcise": False,
        "forceSideBySide": False,
        "imageAttachments": [],
        "imageGenerationCount": 2,
        "isAsyncChat": False,
        "message": message,
        "modeId": mode,
        "responseMetadata": {},
        "returnImageBytes": False,
        "returnRawGrokInXaiRequest": False,
        "sendFinalMetadata": True,
        "temporary": True,
    }


def convert_chat_completion(request: Mapping[str, Any]) -> ConvertedWebChat:
    """Convert ``model/messages/stream`` without forwarding unrelated fields."""

    if not isinstance(request, Mapping):
        raise WebProtocolError("chat completion request must be an object")
    model = _resolve_model(request.get("model"))
    stream = request.get("stream", False)
    if not isinstance(stream, bool):
        raise WebProtocolError("stream must be a boolean")
    prompt = messages_to_prompt(request.get("messages"))
    return ConvertedWebChat(
        public_model=model.id,
        upstream_mode=model.upstream_mode,
        stream=stream,
        payload=build_rest_chat_payload(prompt, model.upstream_mode),
    )
