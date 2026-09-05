"""Grid-aligned interval index for half-open window membership queries."""

from __future__ import annotations

import heapq
import math
from collections.abc import Iterator
from typing import NamedTuple

from stream_quilt.models import Event


def event_horizon(event: Event) -> float:
    """Return the exclusive end of the timeline an event can still occupy."""

    if event.duration_ms == 0:
        return math.nextafter(event.timestamp_ms, math.inf)
    return event.end_ms


def overlaps(event: Event, start: float, end: float) -> bool:
    """Report whether an event intersects the half-open interval ``[start, end)``."""

    if event.duration_ms == 0:
        return start <= event.timestamp_ms < end
    return event.timestamp_ms < end and event.end_ms > start


class _Entry(NamedTuple):
    """One live event and the window index range it covers."""

    event: Event
    first_index: int
    last_index: int


def _decompose(first: int, last: int) -> Iterator[tuple[int, int]]:
    """Yield the canonical dyadic nodes that exactly cover ``[first, last]``.

    Node ``(level, block)`` covers window indexes ``[block << level, (block + 1) << level)``.
    The nodes are disjoint and their union is exactly the requested range, so a stabbing
    query finds any covered event exactly once. An empty range yields nothing.
    """

    low, high = first, last + 1
    level = 0
    while low < high:
        if low & 1:
            yield level, low
            low += 1
        if high & 1:
            high -= 1
            yield level, high
        low >>= 1
        high >>= 1
        level += 1


class WindowIntervalIndex:
    """Answer "which events overlap window ``i``" without scanning the buffer.

    Windows form the regular half-open grid ``[origin + i*hop, origin + i*hop + window)``,
    so every event overlaps one *contiguous* range of window indexes. Each event is stored
    once per node of the canonical dyadic decomposition of that range, and a query for
    window ``i`` reads one node per level on the path from that leaf to the root. Insert
    and expiry cost ``O(log span)``; a query costs ``O(log span)`` lookups plus the events
    it actually returns.

    The shape is chosen for this access pattern:

    * Arrivals are append-mostly behind a monotonically advancing watermark. A dyadic
      decomposition is implicit, so near-frontier inserts and expiry from behind need no
      rebalancing (as a balanced interval tree would) and no memmove (as a sorted array
      would).
    * A single very long event covers many windows. One bucket per covered window would
      cost ``O(span)`` per event; canonical decomposition costs ``O(log span)``.
    * Ready windows are previewed as one transaction that may be rejected, so queries must
      be pure. A sweep-line active set advanced per window would have to be rolled back
      after a rejected preview; this index never mutates while answering a query.

    Window-index boundaries are found by binary search over the exact overlap predicate
    rather than by dividing timestamps by ``hop_ms``, so no floating-point error analysis
    stands between the index and the window builder.
    """

    __slots__ = (
        "_ceiling",
        "_events_examined",
        "_expiry",
        "_hop",
        "_live",
        "_nodes",
        "_origin",
        "_top_level",
        "_window",
    )

    def __init__(
        self, *, origin_ms: float, window_ms: float, hop_ms: float, max_windows: int
    ) -> None:
        self._origin = origin_ms
        self._window = window_ms
        self._hop = hop_ms
        # Windows above ``max_windows - 1`` are never built, so searches stop there.
        # ``max_windows`` itself is the "expires no earlier than the end of the buildable
        # grid" sentinel, which keeps far-future events out of the expiry path without
        # pretending to know their true last window.
        self._ceiling = max_windows
        self._live: dict[str, _Entry] = {}
        self._nodes: dict[tuple[int, int], dict[str, Event]] = {}
        self._expiry: list[tuple[int, str]] = []
        self._top_level = 0
        self._events_examined = 0

    def __len__(self) -> int:
        return len(self._live)

    @property
    def events_examined(self) -> int:
        """Count of buffered events inspected while answering window queries.

        The linear builder inspected the whole buffer for every window. This counter makes
        the difference assertable without timing anything.
        """

        return self._events_examined

    def insert(self, event: Event) -> None:
        """Index one normalized event under every window it can still reach."""

        first = self._first_overlapping(event)
        last = self._last_overlapping(event)
        self._live[event.id] = _Entry(event, first, last)
        heapq.heappush(self._expiry, (last, event.id))
        for level, block in _decompose(first, min(last, self._ceiling - 1)):
            self._nodes.setdefault((level, block), {})[event.id] = event
            self._top_level = max(self._top_level, level)

    def overlapping(self, index: int) -> tuple[Event, ...]:
        """Return the live events that overlap window ``index``."""

        matched: list[Event] = []
        block = index
        for level in range(self._top_level + 1):
            bucket = self._nodes.get((level, block))
            if bucket is not None:
                matched.extend(bucket.values())
            block >>= 1
        self._events_examined += len(matched)
        return tuple(matched)

    def prune(self, first_open_index: int) -> None:
        """Drop events that no window at or after ``first_open_index`` can contain."""

        while self._expiry and self._expiry[0][0] < first_open_index:
            _, event_id = heapq.heappop(self._expiry)
            entry = self._live.pop(event_id)
            for key in _decompose(entry.first_index, min(entry.last_index, self._ceiling - 1)):
                bucket = self._nodes[key]
                del bucket[event_id]
                if not bucket:
                    del self._nodes[key]

    def horizon(self) -> float:
        """Return the greatest horizon among live events."""

        return max(event_horizon(entry.event) for entry in self._live.values())

    def clear(self) -> None:
        """Forget every live event while keeping accumulated query counters."""

        self._live.clear()
        self._nodes.clear()
        self._expiry.clear()

    def _window_start(self, index: int) -> float:
        return self._origin + index * self._hop

    def _first_overlapping(self, event: Event) -> int:
        """Return the smallest window index whose interval ends after the event starts.

        ``timestamp < window_end(i)`` is monotone in ``i``, so the boundary is exact. A
        result above the ceiling means the event begins past the buildable grid.
        """

        low, high = 0, self._ceiling + 1
        while low < high:
            middle = (low + high) // 2
            if event.timestamp_ms < self._window_start(middle) + self._window:
                high = middle
            else:
                low = middle + 1
        return low

    def _last_overlapping(self, event: Event) -> int:
        """Return the largest window index whose interval starts before the event ends.

        ``horizon > window_start(i)`` is monotone in ``i``. For zero-duration events the
        horizon is the next representable value, which makes ``horizon > start``
        equivalent to the builder's ``start <= timestamp`` without a special case.
        """

        horizon = event_horizon(event)
        low, high = 0, self._ceiling + 1
        while low < high:
            middle = (low + high) // 2
            if horizon > self._window_start(middle):
                low = middle + 1
            else:
                high = middle
        return low - 1
