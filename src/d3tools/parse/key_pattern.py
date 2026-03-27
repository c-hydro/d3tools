"""Key-pattern parsing and rendering utilities.

This module is the canonical home for dataset key/pattern matching and
rendering logic.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
import datetime as dt
import os
import re
from typing import Any, Optional

from ..timestepping import TimeStep
from .string_rendering import substitute_string


@dataclass(frozen=True)
class ParsedKey:
    """Parsed key result containing canonical time and extracted tags."""

    key : str
    time: dt.datetime
    tags: dict[str, str]


class KeyPattern:
    """Dataset key-pattern helper for rendering, matching, and prefix discovery.

    A key pattern is a string that can include:
    - datetime directives (e.g. ``%Y``, ``%m``, ``%d``, ``%j``)
    - tag placeholders (e.g. ``{tile}``, ``{variable}``)
    """

    def __init__(self, raw_pattern: str):
        """Initialize a key-pattern parser/renderer.

        Args:
            raw_pattern: Pattern string used to render keys and match existing keys.
        """
        self.raw_pattern = raw_pattern

    def render(
            self,
            time: Optional[dt.datetime | TimeStep] = None,
            tags: Optional[dict[str, Any]] = None,
            time_signature: str = "end",
        ) -> str:
        """Render a concrete key from the pattern.

        Args:
            time: Optional datetime or timestep used to resolve ``strftime`` fields.
            tags: Optional tag values used to resolve ``{tag}`` placeholders.
            time_signature: How to map a timestep to a datetime when ``time`` is
                a TimeStep. Must be one of ``'start'``, ``'end'``, ``'end+1'``.

        Returns:
            Rendered key string.

        Raises:
            ValueError: If ``time_signature`` is invalid.
        """
        tags = tags or {}
        raw_key = substitute_string(self.raw_pattern, tags)

        if time is None:
            return raw_key

        render_time = self._resolve_render_time(time, time_signature)
        return render_time.strftime(raw_key)

    def match(self, key: str) -> ParsedKey:
        """Parse a concrete key into time and tags.

        Args:
            key: Concrete key/path string to parse.

        Returns:
            ParsedKey containing original key, canonical datetime, and tags.

        Raises:
            ValueError: If the key does not match the pattern.
        """
        parsed_time, parsed_tags = self._extract_date_and_tags(key, self.raw_pattern)
        return ParsedKey(key = key, time=parsed_time, tags=parsed_tags)

    def prefix(self, time: Optional[Any] = None, tags: Optional[dict[str, Any]] = None) -> str:
        """Compute a directory prefix suitable for data discovery.

        If ``time`` is a range-like object (has ``.start`` and ``.end``),
        progressively specializes year/month/day when boundaries align.

        Args:
            time: Optional datetime-like value or range-like object.
            tags: Optional tag values for placeholder substitution.

        Returns:
            Directory prefix with unresolved pattern/tag fragments removed.
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
        """Resolve a render datetime from datetime/timestep input.

        Args:
            time: Datetime or timestep to resolve.
            time_signature: Mapping for timestep inputs.

        Returns:
            Datetime used by ``strftime`` during rendering.

        Raises:
            ValueError: If ``time_signature`` is invalid.
        """
        if isinstance(time, dt.datetime):
            return time

        if time_signature == "start":
            return time.start
        if time_signature == "end":
            return time.end
        if time_signature == "end+1":
            return (time + 1).start

        raise ValueError(f"Invalid time signature: {time_signature}")

    @staticmethod
    def _extract_date_and_tags(string: str, string_pattern: str) -> tuple[dt.datetime, dict[str, str]]:
        """Extract datetime and tags from a key using the provided pattern.

        This method preserves legacy parsing behavior, including:
        - support for tag names with ``.``, ``-``, and ``#``
        - duplicate group-name handling in regex patterns
        - support for both calendar-date and day-of-year formats

        Args:
            string: Concrete key/path to parse.
            string_pattern: Key pattern containing datetime directives and tags.

        Returns:
            Tuple ``(datetime, tags_dict)``.

        Raises:
            ValueError: If string does not match pattern or duplicate tag values
                conflict.
        """
        pattern = string_pattern
        pattern = re.sub(r"\{([\w#-\.]+)\}", r"(?P<\1>[^/]+)", pattern)
        pattern = pattern.replace("%Y", r"(?P<year>\d{4})")
        pattern = pattern.replace("%m", r"(?P<month>\d{2})")
        pattern = pattern.replace("%d", r"(?P<day>\d{2})")
        pattern = pattern.replace("%H", r"(?P<hour>\d{2})")
        pattern = pattern.replace("%M", r"(?P<minute>\d{2})")
        pattern = pattern.replace("%S", r"(?P<second>\d{2})")
        pattern = pattern.replace("%j", r"(?P<doy>\d{3})")

        substituted_names = re.findall(r"(?<=<)[\w#-\.]+(?=>)", pattern)
        if "file_version" in substituted_names:
            pattern = pattern.replace("(?P<file_version>[^/]+", "(?P<file_version>.+")
        names_map: dict[str, str] = {}

        for name in set(substituted_names):
            if "{" + name + "}" in string:
                new_name = name.replace(".", "").replace("-", "").replace("#", "").replace("_", "")
                string = string.replace("{" + name + "}", new_name)

        for name in set(substituted_names):
            if any(s in name for s in [".", "-", "#"]):
                new_name = copy.deepcopy(name).replace(".", "_p_").replace("-", "_d_").replace("#", "_h_")
                pattern = pattern.replace(f"(?P<{name}>", f"(?P<{new_name}>")
                names_map[new_name] = name
            else:
                names_map[name] = name

        substituted_names_2 = re.findall(r"(?<=<)[\w]+(?=>)", pattern)
        for name in set(substituted_names_2):
            count = substituted_names_2.count(name)
            if count > 1:
                for i in range(count - 1):
                    pattern = pattern.replace(f"(?P<{name}>", f"(?P<{name}{i}>", 1)
                    names_map[f"{name}{i}"] = name if name not in names_map else names_map[name]

        match = re.match(pattern, string)
        if not match:
            raise ValueError("The string does not match the pattern")

        all_tags = match.groupdict()
        if "doy" in all_tags:
            year = int(all_tags.pop("year", 1900))
            doy = int(all_tags.pop("doy"))
            date = dt.datetime(year, 1, 1) + dt.timedelta(days=doy - 1)
        else:
            year = int(all_tags.pop("year", 1900))
            month = int(all_tags.pop("month", 1))
            day = int(all_tags.pop("day", 1))
            hour = int(all_tags.pop("hour", 0))
            minute = int(all_tags.pop("minute", 0))
            second = int(all_tags.pop("second", 0))
            date = dt.datetime(year, month, day, hour, minute, second)

        tags: dict[str, str] = {}
        for key, value in names_map.items():
            if value in ["year", "month", "day", "hour", "minute", "second", "doy"]:
                continue
            if value not in tags:
                tags[value] = all_tags[key]
            elif tags[value] != all_tags[key]:
                raise ValueError(f"Duplicate values for tag {value} in the string")

        return date, tags


__all__ = ["ParsedKey", "KeyPattern"]
