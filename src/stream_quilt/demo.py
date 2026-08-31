"""Deterministic example data used by the CLI, docs, and smoke tests."""

from __future__ import annotations

from typing import Any


def demo_config_payload() -> dict[str, Any]:
    return {
        "window_ms": 1_000,
        "hop_ms": 500,
        "allowed_lateness_ms": 150,
        "origin_ms": 0,
        "required_streams": ["camera", "microphone", "transcript"],
        "offsets_ms": {"camera": -40, "microphone": 15, "transcript": 0},
        "expected_cadence_ms": {"camera": 500, "microphone": 500, "transcript": 750},
        "gap_factor": 1.6,
        "late_policy": "reject",
        "max_events_per_window": 100,
        "max_output_windows": 100,
    }


def demo_event_payloads() -> list[dict[str, Any]]:
    """Return intentionally out-of-order arrivals on three clocks."""

    return [
        {
            "id": "v0",
            "stream": "camera",
            "modality": "video",
            "timestamp_ms": 40,
            "duration_ms": 420,
            "data": {"label": "entrance"},
        },
        {
            "id": "a0",
            "stream": "microphone",
            "modality": "audio",
            "timestamp_ms": -15,
            "duration_ms": 480,
            "data": {"rms": 0.14},
        },
        {
            "id": "t0",
            "stream": "transcript",
            "modality": "text",
            "timestamp_ms": 120,
            "duration_ms": 240,
            "data": {"text": "vehicle entering"},
        },
        {
            "id": "v1",
            "stream": "camera",
            "modality": "video",
            "timestamp_ms": 540,
            "duration_ms": 420,
            "data": {"label": "merge"},
        },
        {
            "id": "t1",
            "stream": "transcript",
            "modality": "text",
            "timestamp_ms": 880,
            "duration_ms": 260,
            "data": {"text": "braking"},
        },
        {
            "id": "a2",
            "stream": "microphone",
            "modality": "audio",
            "timestamp_ms": 985,
            "duration_ms": 480,
            "data": {"rms": 0.52},
        },
        {
            "id": "a1",
            "stream": "microphone",
            "modality": "audio",
            "timestamp_ms": 485,
            "duration_ms": 480,
            "data": {"rms": 0.18},
        },
        {
            "id": "v2",
            "stream": "camera",
            "modality": "video",
            "timestamp_ms": 1_040,
            "duration_ms": 420,
            "data": {"label": "stop"},
        },
        {
            "id": "v3",
            "stream": "camera",
            "modality": "video",
            "timestamp_ms": 2_040,
            "duration_ms": 420,
            "data": {"label": "clear"},
        },
        {
            "id": "a3",
            "stream": "microphone",
            "modality": "audio",
            "timestamp_ms": 1_485,
            "duration_ms": 480,
            "data": {"rms": 0.20},
        },
        {
            "id": "t2",
            "stream": "transcript",
            "modality": "text",
            "timestamp_ms": 1_650,
            "duration_ms": 300,
            "data": {"text": "lane clear"},
        },
    ]
