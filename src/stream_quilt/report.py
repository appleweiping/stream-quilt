"""Machine-readable output and a dependency-free HTML timeline."""

from __future__ import annotations

import html
import json
from pathlib import Path

from stream_quilt.errors import OutputError
from stream_quilt.models import AlignmentConfig, AlignmentResult, Event

_COLORS = {
    "video": "#38bdf8",
    "audio": "#a78bfa",
    "text": "#34d399",
    "sensor": "#fbbf24",
}


def write_report_bundle(
    result: AlignmentResult,
    config: AlignmentConfig,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write `alignment.json` and a standalone `timeline.html`."""

    destination = Path(output_dir)
    paths = {
        "alignment": destination / "alignment.json",
        "report": destination / "timeline.html",
    }
    try:
        destination.mkdir(parents=True, exist_ok=True)
        paths["alignment"].write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        paths["report"].write_text(render_html(result, config), encoding="utf-8", newline="\n")
    except (OSError, ValueError) as exc:
        raise OutputError(f"cannot write report bundle to {destination}: {exc}") from exc
    return paths


def render_html(result: AlignmentResult, config: AlignmentConfig) -> str:
    """Render windows and events with all user-controlled text escaped."""

    streams = sorted({event.stream for window in result.windows for event in window.events})
    events = _unique_events(result)
    if events:
        start = min(event.timestamp_ms for event in events)
        end = max(event.end_ms if event.duration_ms else event.timestamp_ms for event in events)
    else:
        start = config.origin_ms
        end = start + config.window_ms
    span = max(end - start, 1.0)
    width = 980
    label_width = 145
    chart_width = width - label_width - 20
    row_height = 62
    svg_height = 35 + max(len(streams), 1) * row_height
    svg = [f'<svg viewBox="0 0 {width} {svg_height}" role="img" aria-label="Aligned streams">']
    for row, stream in enumerate(streams):
        y = 20 + row * row_height
        svg.append(f'<text x="8" y="{y + 25}" class="stream-label">{html.escape(stream)}</text>')
        svg.append(
            f'<line x1="{label_width}" y1="{y + 31}" x2="{width - 10}" '
            f'y2="{y + 31}" stroke="#253753" />'
        )
        for event in events:
            if event.stream != stream:
                continue
            x = label_width + (event.timestamp_ms - start) / span * chart_width
            duration = max(event.duration_ms / span * chart_width, 5.0)
            color = _COLORS.get(event.modality, "#fb7185")
            title = html.escape(f"{event.id} | {event.modality} | {event.timestamp_ms:g} ms")
            svg.append(
                f'<rect x="{x:.2f}" y="{y + 9}" width="{duration:.2f}" height="34" '
                f'rx="6" fill="{color}"><title>{title}</title></rect>'
            )
            if duration > 55:
                svg.append(
                    f'<text x="{x + 7:.2f}" y="{y + 31}" class="event-label">'
                    f"{html.escape(event.id)}</text>"
                )
    if not streams:
        svg.append('<text x="20" y="45" class="stream-label">No events</text>')
    svg.append("</svg>")

    window_rows = "\n".join(
        "<tr>"
        f"<td>{window.index}</td><td>{window.start_ms:.1f}-{window.end_ms:.1f}</td>"
        f"<td>{len(window.events)}</td>"
        f"<td>{html.escape(', '.join(window.modalities) or '-')}</td>"
        f"<td>{'yes' if window.complete else 'no'}</td>"
        f"<td>{html.escape(', '.join(window.missing_streams) or '-')}</td>"
        "</tr>"
        for window in result.windows
    )
    gap_rows = (
        "\n".join(
            "<tr>"
            f"<td>{html.escape(gap.stream)}</td><td>{gap.start_ms:.1f}</td>"
            f"<td>{gap.end_ms:.1f}</td><td>{gap.observed_ms:.1f}</td>"
            f"<td>{gap.expected_ms:.1f}</td></tr>"
            for gap in result.gaps
        )
        or '<tr><td colspan="5">No cadence gaps detected</td></tr>'
    )
    incomplete = sum(not window.complete for window in result.windows)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stream Quilt timeline</title>
<style>
:root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }}
body {{ margin: 0; background: #07111f; color: #e5edf8; }}
main {{ max-width: 1100px; margin: auto; padding: 42px 24px 64px; }}
.eyebrow {{ color: #38bdf8; font-weight: 800; letter-spacing: .15em; text-transform: uppercase; }}
h1 {{ font-size: clamp(2.2rem, 5vw, 4.5rem); margin: .15em 0; }}
.lede {{ color: #9fb0c8; max-width: 740px; }}
.metrics {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr));
  gap: 12px; margin: 28px 0; }}
.metric, section {{ background: #0d1b2e; border: 1px solid #203451; border-radius: 14px; }}
.metric {{ padding: 16px; display: grid; gap: 7px; }}
.metric span {{ color: #8da2bd; font-size: .82rem; }}
section {{ padding: 20px; margin-top: 18px; overflow-x: auto; }}
svg {{ width: 100%; min-width: 720px; background: #091526; border-radius: 10px; }}
.stream-label {{ fill: #dbeafe; font: 600 13px system-ui; }}
.event-label {{ fill: #06111f; font: 700 11px system-ui; pointer-events: none; }}
table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }}
th, td {{ padding: 10px; border-bottom: 1px solid #1b2b44; text-align: left; }}
th {{ color: #7dd3fc; font-size: .78rem; text-transform: uppercase; }}
</style></head><body><main>
<div class="eyebrow">Stream Quilt report</div><h1>Aligned timeline</h1>
<p class="lede">A deterministic view of normalized event time, window completeness,
  and cadence gaps.</p>
<div class="metrics">
  <div class="metric"><span>Windows</span><strong>{len(result.windows)}</strong></div>
  <div class="metric"><span>Unique events</span><strong>{len(events)}</strong></div>
  <div class="metric"><span>Incomplete windows</span><strong>{incomplete}</strong></div>
  <div class="metric"><span>Cadence gaps</span><strong>{len(result.gaps)}</strong></div>
  <div class="metric"><span>Dropped late events</span>
    <strong>{len(result.dropped_event_ids)}</strong></div>
  <div class="metric"><span>Accepted late events</span>
    <strong>{len(result.accepted_late_event_ids)}</strong></div>
  <div class="metric"><span>Unassigned events</span>
    <strong>{len(result.unassigned_event_ids)}</strong></div>
</div>
<section><h2>Streams</h2>{"".join(svg)}</section>
<section><h2>Windows</h2><table><thead><tr>
  <th>#</th><th>Range ms</th><th>Events</th><th>Modalities</th>
  <th>Complete</th><th>Missing</th>
</tr></thead><tbody>{window_rows}</tbody></table></section>
<section><h2>Cadence gaps</h2><table><thead><tr>
  <th>Stream</th><th>Gap start</th><th>Next event</th>
  <th>Observed ms</th><th>Expected ms</th>
</tr></thead><tbody>{gap_rows}</tbody></table></section>
</main></body></html>
"""


def _unique_events(result: AlignmentResult) -> tuple[Event, ...]:
    by_id: dict[str, Event] = {}
    for window in result.windows:
        for event in window.events:
            by_id[event.id] = event
    return tuple(
        sorted(by_id.values(), key=lambda event: (event.timestamp_ms, event.stream, event.id))
    )
