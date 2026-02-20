"""Key-pattern parsing and rendering utilities.

This module is the canonical home for dataset key/pattern matching and
rendering logic.
"""

from __future__ import annotations

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


@dataclass(frozen=True)
class _CompiledMatcher:
    """Internal compiled representation of a key pattern for matching."""

    regex_pattern: str
    names_map: dict[str, str]
    substituted_names: tuple[str, ...]


class KeyParser:
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
        self._compiled_matcher: Optional[_CompiledMatcher] = None

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
        parsed_time, parsed_tags = self._extract_date_and_tags(key)
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

    def _extract_date_and_tags(self, string: str) -> tuple[dt.datetime, dict[str, str]]:
        """Extract datetime and tags from a key using this instance pattern.

        This method preserves legacy parsing behavior, including:
        - support for tag names with ``.``, ``-``, and ``#``
        - duplicate group-name handling in regex patterns
        - support for both calendar-date and day-of-year formats

        Args:
            string: Concrete key/path to parse.
        Returns:
            Tuple ``(datetime, tags_dict)``.

        Raises:
            ValueError: If string does not match pattern or duplicate tag values
                conflict.
        """
        matcher = self._get_compiled_matcher()
        string = self._normalize_input_key(string, matcher.substituted_names)

        match = re.match(matcher.regex_pattern, string)
        if not match:
            raise ValueError("The string does not match the pattern")

        all_tags = match.groupdict()
        date = self._parse_datetime(all_tags)
        tags = self._parse_tags(all_tags, matcher.names_map)
        return date, tags

    def _get_compiled_matcher(self) -> _CompiledMatcher:
        """Return cached matcher metadata for this pattern.

        Compiles the matcher on first use, then reuses it for subsequent
        ``match()`` calls to avoid rebuilding regex/group mappings.

        Returns:
            Internal compiled matcher metadata.
        """
        if self._compiled_matcher is None:
            self._compiled_matcher = self._compile_matcher()
        return self._compiled_matcher

    def _compile_matcher(self) -> _CompiledMatcher:
        """Compile ``self.raw_pattern`` into regex and group mapping metadata.

        This step applies legacy-compatible transformations:
        - converts ``{tag}`` placeholders to named regex groups
        - converts datetime directives (``%Y``, ``%m``, etc.) to groups
        - normalizes special-character group names
        - disambiguates duplicate group names

        Returns:
            Internal compiled matcher structure used during parsing.
        """
        pattern = self.raw_pattern
        pattern = re.sub(r"\{([\w#-\.]+)\}", r"(?P<\1>[^/]+)", pattern)
        pattern = pattern.replace("%Y", r"(?P<year>\d{4})")
        pattern = pattern.replace("%m", r"(?P<month>\d{2})")
        pattern = pattern.replace("%d", r"(?P<day>\d{2})")
        pattern = pattern.replace("%H", r"(?P<hour>\d{2})")
        pattern = pattern.replace("%M", r"(?P<minute>\d{2})")
        pattern = pattern.replace("%S", r"(?P<second>\d{2})")
        pattern = pattern.replace("%j", r"(?P<doy>\d{3})")

        substituted_names = tuple(re.findall(r"(?<=<)[\w#-\.]+(?=>)", pattern))
        if "file_version" in substituted_names:
            pattern = pattern.replace("(?P<file_version>[^/]+", "(?P<file_version>.+")
        names_map: dict[str, str] = {}

        for name in set(substituted_names):
            if any(s in name for s in [".", "-", "#"]):
                new_name = name.replace(".", "_p_").replace("-", "_d_").replace("#", "_h_")
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

        return _CompiledMatcher(
            regex_pattern=pattern,
            names_map=names_map,
            substituted_names=substituted_names,
        )

    @staticmethod
    def _normalize_input_key(string: str, substituted_names: tuple[str, ...]) -> str:
        """Normalize placeholder-like fragments in input key using legacy rules.

        Args:
            string: Input key/path string to normalize.
            substituted_names: Placeholder names discovered in compiled matcher.

        Returns:
            Normalized key string compatible with compiled group naming.
        """
        for name in set(substituted_names):
            if "{" + name + "}" in string:
                new_name = name.replace(".", "").replace("-", "").replace("#", "").replace("_", "")
                string = string.replace("{" + name + "}", new_name)
        return string

    @staticmethod
    def _parse_datetime(all_tags: dict[str, str]) -> dt.datetime:
        """Parse datetime components from extracted regex groups.

        Supports both:
        - year/day-of-year parsing via ``doy``
        - calendar component parsing via year/month/day/hour/minute/second

        Args:
            all_tags: Regex group dict. Time-related keys are consumed (popped).

        Returns:
            Parsed datetime object.
        """
        if "doy" in all_tags:
            year = int(all_tags.pop("year", 1900))
            doy = int(all_tags.pop("doy"))
            return dt.datetime(year, 1, 1) + dt.timedelta(days=doy - 1)

        year = int(all_tags.pop("year", 1900))
        month = int(all_tags.pop("month", 1))
        day = int(all_tags.pop("day", 1))
        hour = int(all_tags.pop("hour", 0))
        minute = int(all_tags.pop("minute", 0))
        second = int(all_tags.pop("second", 0))
        return dt.datetime(year, month, day, hour, minute, second)

    @staticmethod
    def _parse_tags(all_tags: dict[str, str], names_map: dict[str, str]) -> dict[str, str]:
        """Parse non-time tags from extracted regex groups.

        Args:
            all_tags: Regex group dict after time fields are consumed.
            names_map: Mapping from normalized group names to original tag names.

        Returns:
            Dictionary of parsed tag values keyed by original tag name.

        Raises:
            ValueError: If duplicate placeholders for the same tag resolve to
                conflicting values.
        """
        tags: dict[str, str] = {}
        for key, value in names_map.items():
            if value in ["year", "month", "day", "hour", "minute", "second", "doy"]:
                continue
            if value not in tags:
                tags[value] = all_tags[key]
            elif tags[value] != all_tags[key]:
                raise ValueError(f"Duplicate values for tag {value} in the string")

        return tags


__all__ = ["ParsedKey", "KeyParser"]
