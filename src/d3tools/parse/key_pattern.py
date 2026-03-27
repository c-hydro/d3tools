"""Key-pattern parsing and rendering utilities.

This module is a phase-1 scaffolding layer for dataset key/pattern handling.
Behavior is intentionally conservative and delegates to existing parser logic
where possible.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import os
from typing import Any, Optional

from ..timestepping import TimeStep
from .parse_utils import extract_date_and_tags
from .string_rendering import substitute_string


@dataclass(frozen=True)
class ParsedKey:
    """Parsed key result containing canonical time and extracted tags."""

    key : str
    time: dt.datetime
    tags: dict[str, str]


class KeyPattern:
    """Lightweight key-pattern interface for render/match/prefix operations."""

    def __init__(self, raw_pattern: str):
        self.raw_pattern = raw_pattern

    def render(
        self,
        time: Optional[dt.datetime | TimeStep] = None,
        tags: Optional[dict[str, Any]] = None,
        time_signature: str = "end",
    ) -> str:
        """Render a key from this pattern with optional time and tags."""
        tags = tags or {}
        raw_key = substitute_string(self.raw_pattern, tags)

        if time is None:
            return raw_key

        render_time = self._resolve_render_time(time, time_signature)
        return render_time.strftime(raw_key)

    def match(self, key: str) -> ParsedKey:
        """Parse a key into time and tags using legacy extraction behavior."""
        parsed_time, parsed_tags = extract_date_and_tags(key, self.raw_pattern)
        return ParsedKey(key = key, time=parsed_time, tags=parsed_tags)

    def prefix(self, time: Optional[Any] = None, tags: Optional[dict[str, Any]] = None) -> str:
        """Compute a discovery prefix from this key pattern.

        If `time` is a range-like object (has `.start` and `.end`),
        progressively specializes year/month/day when boundaries align.
        """
        tags = tags or {}

        if time is not None and hasattr(time, "start") and hasattr(time, "end"):
            start = time.start
            end = time.end
            key = self.render(time=None, tags=tags)

            if start.year == end.year:
                key = key.replace("%Y", str(start.year))
                if start.month == end.month:
                    key = key.replace("%m", f"{start.month:02d}")
                    if start.day == end.day:
                        key = key.replace("%d", f"{start.day:02d}")
                        key = key.replace("%j", f"{start.timetuple().tm_yday:03d}")
        else:
            key = self.render(time=time, tags=tags)

        prefix = os.path.dirname(key)
        while "%" in prefix or "{" in prefix:
            prefix = os.path.dirname(prefix)

        return prefix

    @staticmethod
    def _resolve_render_time(time: dt.datetime | TimeStep, time_signature: str) -> dt.datetime:
        if isinstance(time, dt.datetime):
            return time

        if time_signature == "start":
            return time.start
        if time_signature == "end":
            return time.end
        if time_signature == "end+1":
            return (time + 1).start

        raise ValueError(f"Invalid time signature: {time_signature}")


__all__ = ["ParsedKey", "KeyPattern"]
