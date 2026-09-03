"""Controlled CloudEvents 1.0 structured JSONL adapter.

CloudEvents ``time`` is optional in the general specification.  This adapter
requires it because event-time alignment cannot invent occurrence time.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from stream_quilt.errors import ValidationError
from stream_quilt.io import _reject_duplicate_keys, _reject_json_constant, _text
from stream_quilt.limits import (
    MAX_BASE64_DECODED_BYTES,
    MAX_EVENT_FILE_BYTES,
    MAX_EVENTS,
)
from stream_quilt.models import Event

_MAX_BASE64_DECODED_BYTES = MAX_BASE64_DECODED_BYTES

_CORE_ATTRIBUTES = {
    "specversion",
    "id",
    "source",
    "type",
    "datacontenttype",
    "dataschema",
    "subject",
    "time",
}
_DATA_FIELDS = {"data", "data_base64"}
_ADAPTER_EXTENSIONS = {"stream", "modality", "durationms"}
_ATTRIBUTE_NAME = re.compile(r"^[a-z][a-z0-9]{0,19}$")
_RFC3339_MILLISECONDS = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?P<fraction>\.\d{1,3})?(?P<offset>Z|[+-]\d{2}:\d{2})$"
)
_URI_REFERENCE = re.compile(r"^[A-Za-z0-9:/?#\[\]@!$&'()*+,;=._~%-]+$")
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*$")
_TOKEN = r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
_QUOTED = r'"(?:[\t !#-\[\]-~]|\\[\t -~])*"'
_MEDIA_TYPE = re.compile(
    rf"^{_TOKEN}/{_TOKEN}(?:[ \t]*;[ \t]*{_TOKEN}=({_TOKEN}|{_QUOTED}))*[ \t]*$"
)


def load_cloudevents(path: str | Path) -> tuple[Event, ...]:
    """Load one structured CloudEvent JSON object per nonblank line."""

    source = Path(path)
    try:
        lines = _read_text_limited(source).splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"cannot read CloudEvents {source}: {exc}") from exc
    events: list[Event] = []
    seen: set[str] = set()
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        if len(events) == MAX_EVENTS:
            raise ValidationError(f"CloudEvents file exceeds the {MAX_EVENTS}-record limit")
        try:
            payload = json.loads(
                line,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise ValidationError(f"invalid CloudEvent JSON at line {line_number}: {exc}") from exc
        event = cloudevent_from_dict(payload, path=f"line {line_number}")
        if event.id in seen:
            raise ValidationError(f"duplicate CloudEvent source/id identity at line {line_number}")
        seen.add(event.id)
        events.append(event)
    return tuple(events)


def cloudevent_from_dict(payload: Any, *, path: str = "$cloudevent") -> Event:
    """Map one CloudEvents 1.0 JSON envelope to a Stream Quilt event.

    ``stream``, ``modality``, and ``durationms`` are optional CloudEvents
    extension attributes understood by this adapter.  Other extensions are
    retained as provenance but do not influence alignment.
    """

    if not isinstance(payload, Mapping) or not all(isinstance(key, str) for key in payload):
        raise ValidationError(f"{path} must be an object with string keys")
    required = {"specversion", "id", "source", "type", "time"}
    present = {key for key, value in payload.items() if value is not None}
    missing = sorted(required - present)
    if missing:
        raise ValidationError(
            f"{path} is missing required alignment field(s): {', '.join(missing)}"
        )
    if payload["specversion"] != "1.0":
        raise ValidationError(f"{path}.specversion must be '1.0'")
    event_id = _text(payload["id"], f"{path}.id")
    source = _uri_reference(payload["source"], f"{path}.source")
    event_type = _text(payload["type"], f"{path}.type")
    timestamp = _rfc3339_ms(payload["time"], f"{path}.time")
    stream_value = payload.get("stream")
    modality_value = payload.get("modality")
    stream = _text(source if stream_value is None else stream_value, f"{path}.stream")
    modality = _text(event_type if modality_value is None else modality_value, f"{path}.modality")
    duration_value = payload.get("durationms")
    duration = _duration(0 if duration_value is None else duration_value, f"{path}.durationms")
    has_data = "data" in payload
    has_base64 = payload.get("data_base64") is not None
    if has_data and has_base64:
        raise ValidationError(f"{path} cannot contain both data and data_base64")
    _validate_context_attributes(payload, path)
    if has_base64:
        encoded = payload["data_base64"]
        if not isinstance(encoded, str):
            raise ValidationError(f"{path}.data_base64 must be a base64 string")
        maximum_encoded = ((_MAX_BASE64_DECODED_BYTES + 2) // 3) * 4
        if len(encoded) > maximum_encoded:
            raise ValidationError(
                f"{path}.data_base64 exceeds the {_MAX_BASE64_DECODED_BYTES}-byte decoded limit"
            )
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValidationError(f"{path}.data_base64 must be valid base64") from exc
        if len(decoded) > _MAX_BASE64_DECODED_BYTES:
            raise ValidationError(
                f"{path}.data_base64 exceeds the {_MAX_BASE64_DECODED_BYTES}-byte decoded limit"
            )

    attributes = {
        key: value
        for key, value in payload.items()
        if key in _CORE_ATTRIBUTES and value is not None
    }
    extensions = {
        key: value
        for key, value in payload.items()
        if key not in _CORE_ATTRIBUTES | _DATA_FIELDS and value is not None
    }
    data: dict[str, Any] = {"cloudevent": attributes}
    if extensions:
        data["extensions"] = extensions
    if has_data:
        data["payload"] = payload["data"]
    if has_base64:
        data["payload_base64"] = payload["data_base64"]
    return Event(
        id=_identity(source, event_id),
        stream=stream,
        modality=modality,
        timestamp_ms=timestamp,
        duration_ms=duration,
        data=data,
    )


def _rfc3339_ms(value: Any, path: str) -> float:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{path} must be an RFC 3339 timestamp")
    text = value.strip()
    match = _RFC3339_MILLISECONDS.fullmatch(text)
    if match is None:
        raise ValidationError(
            f"{path} must use the supported RFC 3339 subset with uppercase T/Z and at most "
            "millisecond precision"
        )
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValidationError(f"{path} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{path} must include a UTC offset")
    try:
        utc = parsed.astimezone(UTC)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = utc - epoch
        timestamp_ms = delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000
    except (OSError, OverflowError, ValueError) as exc:
        raise ValidationError(f"{path} is outside the supported timestamp range") from exc
    return float(timestamp_ms)


def _duration(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**53:
        raise ValidationError(f"{path} must be an integer from 0 to 9007199254740991")
    return float(value)


def _identity(source: str, event_id: str) -> str:
    """Encode the CloudEvents source/id identity injectively as one safe label."""

    return f"ce:{quote(source, safe='')}:{quote(event_id, safe='')}"


def _validate_context_attributes(payload: Mapping[str, Any], path: str) -> None:
    for key in payload:
        if key in _DATA_FIELDS:
            continue
        if _ATTRIBUTE_NAME.fullmatch(key) is None:
            raise ValidationError(
                f"{path} attribute name {key!r} must be lowercase alphanumeric, start with a "
                "letter, and contain at most 20 characters"
            )
    if payload.get("datacontenttype") is not None:
        content_type = _text(payload["datacontenttype"], f"{path}.datacontenttype")
        if _MEDIA_TYPE.fullmatch(content_type) is None:
            raise ValidationError(f"{path}.datacontenttype must be a valid media type")
    if payload.get("subject") is not None:
        _text(payload["subject"], f"{path}.subject")
    if payload.get("dataschema") is not None:
        schema = _uri_reference(payload["dataschema"], f"{path}.dataschema")
        try:
            scheme = urlsplit(schema).scheme
        except ValueError as exc:
            raise ValidationError(f"{path}.dataschema must be an absolute URI") from exc
        if _URI_SCHEME.fullmatch(scheme) is None:
            raise ValidationError(f"{path}.dataschema must be an absolute URI")
    for key, value in payload.items():
        if key in _CORE_ATTRIBUTES | _DATA_FIELDS | _ADAPTER_EXTENSIONS or value is None:
            continue
        if isinstance(value, float) or not isinstance(value, (str, int, bool)):
            raise ValidationError(
                f"{path}.{key} must use a CloudEvents String, Integer, or Boolean value"
            )
        if isinstance(value, str):
            _text(value, f"{path}.{key}")
        if isinstance(value, int) and not isinstance(value, bool) and not -(2**53) < value < 2**53:
            raise ValidationError(f"{path}.{key} integer is outside the interoperable JSON range")


def _uri_reference(value: Any, path: str) -> str:
    text = _text(value, path)
    if _URI_REFERENCE.fullmatch(text) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    for index, character in enumerate(text):
        if character == "%" and (
            index + 2 >= len(text)
            or any(item not in "0123456789abcdefABCDEF" for item in text[index + 1 : index + 3])
        ):
            raise ValidationError(f"{path} contains an invalid percent escape")
    return text


def _read_text_limited(source: Path) -> str:
    with source.open("rb") as handle:
        raw = handle.read(MAX_EVENT_FILE_BYTES + 1)
    if len(raw) > MAX_EVENT_FILE_BYTES:
        raise ValidationError(f"CloudEvents file exceeds the {MAX_EVENT_FILE_BYTES}-byte limit")
    return raw.decode("utf-8")
