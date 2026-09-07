"""Controlled CloudEvents 1.0 structured JSONL adapter.

CloudEvents ``time`` is optional in the general specification.  This adapter
requires it because event-time alignment cannot invent occurrence time.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from stream_quilt.errors import ValidationError
from stream_quilt.io import _reject_duplicate_keys, _reject_json_constant
from stream_quilt.limits import (
    MAX_BASE64_DECODED_BYTES,
    MAX_EVENT_FILE_BYTES,
    MAX_EVENTS,
    MAX_MAPPING_ENTRIES,
    MAX_TEXT_LENGTH,
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
_ATTRIBUTE_NAME = re.compile(r"^[a-z0-9]{1,20}$")
_RFC3339_MILLISECONDS = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?P<fraction>\.\d{1,3})?(?P<offset>Z|[+-]\d{2}:\d{2})$"
)
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*$")
_PCT_ENCODED = r"%[0-9A-Fa-f]{2}"
_UNRESERVED = r"A-Za-z0-9._~\-"
_SUB_DELIMS = r"!$&'()*+,;="
_PCHAR = rf"(?:[{_UNRESERVED}{_SUB_DELIMS}:@]|{_PCT_ENCODED})"
_PATH = re.compile(rf"^(?:{_PCHAR}|/)*$")
_PATH_NOSCHEME_SEGMENT = re.compile(rf"^(?:[{_UNRESERVED}{_SUB_DELIMS}@]|{_PCT_ENCODED})*$")
_QUERY_OR_FRAGMENT = re.compile(rf"^(?:{_PCHAR}|[/?])*$")
_USERINFO = re.compile(rf"^(?:[{_UNRESERVED}{_SUB_DELIMS}:]|{_PCT_ENCODED})*$")
_REG_NAME = re.compile(rf"^(?:[{_UNRESERVED}{_SUB_DELIMS}]|{_PCT_ENCODED})*$")
_IPV_FUTURE = re.compile(rf"^[vV][0-9A-Fa-f]+\.(?:[{_UNRESERVED}{_SUB_DELIMS}:])+$")
_CE_INTEGER_MIN = -(2**31)
_CE_INTEGER_MAX = 2**31 - 1
_TCHAR_RUN = r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
_QUOTED = r'"(?:[\t !#-\[\]-~]|\\[\t -~])*"'
_MEDIA_TYPE = re.compile(
    rf"^{_TCHAR_RUN}/{_TCHAR_RUN}"
    rf"(?:[ \t]*;[ \t]*{_TCHAR_RUN}=({_TCHAR_RUN}|{_QUOTED}))*[ \t]*$"
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

    if not isinstance(payload, Mapping):
        raise ValidationError(f"{path} must be an object with string keys")
    if len(payload) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-attribute limit")
    item: dict[str, Any] = {}
    for index, (key, value) in enumerate(payload.items()):
        if index == MAX_MAPPING_ENTRIES:
            raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-attribute limit")
        if not isinstance(key, str):
            raise ValidationError(f"{path} must be an object with string keys")
        item[key] = value
    payload = item
    required = {"specversion", "id", "source", "type", "time"}
    present = {key for key, value in payload.items() if value is not None}
    missing = sorted(required - present)
    if missing:
        raise ValidationError(
            f"{path} is missing required alignment field(s): {', '.join(missing)}"
        )
    if payload["specversion"] != "1.0":
        raise ValidationError(f"{path}.specversion must be '1.0'")
    event_id = _ce_string(payload["id"], f"{path}.id", nonempty=True)
    source = _uri_reference(payload["source"], f"{path}.source")
    event_type = _ce_string(payload["type"], f"{path}.type", nonempty=True)
    timestamp = _rfc3339_ms(payload["time"], f"{path}.time")
    stream_value = payload.get("stream")
    modality_value = payload.get("modality")
    stream = _adapter_label(source if stream_value is None else stream_value, f"{path}.stream")
    modality = _adapter_label(
        event_type if modality_value is None else modality_value, f"{path}.modality"
    )
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
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{path} must be an RFC 3339 timestamp")
    text = value
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
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _CE_INTEGER_MAX:
        raise ValidationError(f"{path} must be a CloudEvents Integer from 0 to {_CE_INTEGER_MAX}")
    return float(value)


def _identity(source: str, event_id: str) -> str:
    """Encode a CloudEvents source/id pair as one bounded collision-resistant label."""

    readable = f"ce:{quote(source, safe='')}:{quote(event_id, safe='')}"
    if len(readable) <= MAX_TEXT_LENGTH:
        return readable
    canonical = json.dumps([source, event_id], ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return f"ce-sha256:{hashlib.sha256(canonical).hexdigest()}"


def _validate_context_attributes(payload: Mapping[str, Any], path: str) -> None:
    for key in payload:
        if key in _DATA_FIELDS:
            continue
        if _ATTRIBUTE_NAME.fullmatch(key) is None:
            raise ValidationError(
                f"{path} attribute name {key!r} must be lowercase alphanumeric and contain "
                "1 to 20 characters"
            )
    if payload.get("datacontenttype") is not None:
        content_type = _ce_string(
            payload["datacontenttype"], f"{path}.datacontenttype", nonempty=True
        )
        if _MEDIA_TYPE.fullmatch(content_type) is None:
            raise ValidationError(f"{path}.datacontenttype must be a valid media type")
    if payload.get("subject") is not None:
        _ce_string(payload["subject"], f"{path}.subject", nonempty=True)
    if payload.get("dataschema") is not None:
        try:
            _, scheme, has_fragment = _parse_uri_reference(
                payload["dataschema"], f"{path}.dataschema"
            )
        except ValidationError as exc:
            raise ValidationError(f"{path}.dataschema must be an absolute URI") from exc
        if scheme is None or has_fragment:
            raise ValidationError(f"{path}.dataschema must be an absolute URI")
    for key, value in payload.items():
        if key in _CORE_ATTRIBUTES | _DATA_FIELDS | _ADAPTER_EXTENSIONS or value is None:
            continue
        if isinstance(value, float) or not isinstance(value, (str, int, bool)):
            raise ValidationError(
                f"{path}.{key} must use a CloudEvents String, Integer, or Boolean value"
            )
        if isinstance(value, str):
            _ce_string(value, f"{path}.{key}")
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and not _CE_INTEGER_MIN <= value <= _CE_INTEGER_MAX
        ):
            raise ValidationError(f"{path}.{key} integer is outside the CloudEvents Integer range")


def _uri_reference(value: Any, path: str) -> str:
    text, _, _ = _parse_uri_reference(value, path)
    return text


def _parse_uri_reference(value: Any, path: str) -> tuple[str, str | None, bool]:
    """Validate the RFC 3986 URI-reference grammar used by CloudEvents.

    Parsing is intentionally local rather than delegated to ``urllib.parse``:
    that module splits convenient URL components but explicitly does not claim
    to validate RFC 3986. In particular, it accepts a colon in the first segment
    of a relative path and extra fragment separators.
    """

    text = _ce_string(value, path, nonempty=True)
    if not text.isascii() or text.count("#") > 1:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    before_fragment, separator, fragment = text.partition("#")
    before_query, query_separator, query = before_fragment.partition("?")
    if separator and _QUERY_OR_FRAGMENT.fullmatch(fragment) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    if query_separator and _QUERY_OR_FRAGMENT.fullmatch(query) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")

    scheme: str | None = None
    hierarchical = before_query
    colon = before_query.find(":")
    slash = before_query.find("/")
    if colon >= 0 and (slash < 0 or colon < slash):
        candidate = before_query[:colon]
        if _URI_SCHEME.fullmatch(candidate) is None:
            raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
        scheme = candidate
        hierarchical = before_query[colon + 1 :]

    if hierarchical.startswith("//"):
        authority_and_path = hierarchical[2:]
        authority, slash_separator, suffix = authority_and_path.partition("/")
        _validate_authority(authority, path)
        path_text = f"/{suffix}" if slash_separator else ""
    else:
        path_text = hierarchical
        first_segment = path_text.split("/", 1)[0]
        if (
            scheme is None
            and path_text
            and not path_text.startswith("/")
            and _PATH_NOSCHEME_SEGMENT.fullmatch(first_segment) is None
        ):
            raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    if _PATH.fullmatch(path_text) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    return text, scheme, bool(separator)


def _validate_authority(authority: str, path: str) -> None:
    if authority.count("@") > 1:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    userinfo, at, host_port = authority.rpartition("@")
    if at and _USERINFO.fullmatch(userinfo) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    if not at:
        host_port = authority

    if host_port.startswith("["):
        closing = host_port.find("]")
        if closing < 0 or "]" in host_port[closing + 1 :]:
            raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
        literal = host_port[1:closing]
        remainder = host_port[closing + 1 :]
        if not literal or not (_valid_ipv6(literal) or _IPV_FUTURE.fullmatch(literal)):
            raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
        if remainder and (
            not remainder.startswith(":") or (remainder[1:] and not remainder[1:].isdigit())
        ):
            raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
        return

    if "[" in host_port or "]" in host_port or host_port.count(":") > 1:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    host, colon, port = host_port.rpartition(":")
    if not colon:
        host = host_port
    elif port and not port.isdigit():
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")
    if _REG_NAME.fullmatch(host) is None:
        raise ValidationError(f"{path} must be an ASCII RFC 3986 URI-reference")


def _valid_ipv6(value: str) -> bool:
    # Importing lazily keeps normal relative-source ingestion on the lightweight path.
    import ipaddress

    if "%" in value:
        return False
    try:
        ipaddress.IPv6Address(value)
    except ValueError:
        return False
    return True


def _ce_string(value: Any, path: str, *, nonempty: bool = False) -> str:
    """Validate a CloudEvents String without changing its value."""

    if not isinstance(value, str) or (nonempty and not value):
        qualifier = "non-empty " if nonempty else ""
        raise ValidationError(f"{path} must be a {qualifier}CloudEvents String")
    if len(value) > MAX_TEXT_LENGTH:
        raise ValidationError(f"{path} exceeds the {MAX_TEXT_LENGTH}-character limit")
    if any(_forbidden_ce_code_point(ord(character)) for character in value):
        raise ValidationError(f"{path} contains a character forbidden by CloudEvents String")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValidationError(f"{path} must contain valid Unicode scalar values") from exc
    return value


def _adapter_label(value: Any, path: str) -> str:
    text = _ce_string(value, path, nonempty=True)
    if text != text.strip():
        raise ValidationError(f"{path} cannot have leading or trailing whitespace")
    return text


def _forbidden_ce_code_point(code_point: int) -> bool:
    return (
        code_point <= 0x1F
        or 0x7F <= code_point <= 0x9F
        or 0xFDD0 <= code_point <= 0xFDEF
        or code_point & 0xFFFF in {0xFFFE, 0xFFFF}
    )


def _read_text_limited(source: Path) -> str:
    with source.open("rb") as handle:
        raw = handle.read(MAX_EVENT_FILE_BYTES + 1)
    if len(raw) > MAX_EVENT_FILE_BYTES:
        raise ValidationError(f"CloudEvents file exceeds the {MAX_EVENT_FILE_BYTES}-byte limit")
    return raw.decode("utf-8")
