"""
Tests for KeyPattern and ParsedKey parsing utilities.

Tests rendering, matching, and prefix resolution behavior in the new parser
surface.
"""
import datetime as dt
import pytest

from d3tools.parse import KeyPattern, ParsedKey
from d3tools.timestepping import Day, TimeRange


class TestParsedKey:
    """Test ParsedKey dataclass behavior."""

    def test_parsed_key_fields_are_set(self):
        """Test that ParsedKey stores key, time, and tags correctly."""
        parsed = ParsedKey(
            key='root/20240101_A.tif',
            time=dt.datetime(2024, 1, 1),
            tags={'tile': 'A'}
        )

        assert parsed.key == 'root/20240101_A.tif'
        assert parsed.time == dt.datetime(2024, 1, 1)
        assert parsed.tags == {'tile': 'A'}


class TestKeyPatternInitialization:
    """Test KeyPattern initialization."""

    def test_init_stores_raw_pattern(self):
        """Test that constructor stores input pattern unchanged."""
        key_pattern = KeyPattern('root/%Y/%m/file_%Y%m%d.tif')
        assert key_pattern.raw_pattern == 'root/%Y/%m/file_%Y%m%d.tif'


class TestKeyPatternRender:
    """Test KeyPattern.render method."""

    def test_render_without_time(self):
        """Test rendering with tags only (no datetime formatting)."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        rendered = key_pattern.render(tags={'tile': 'h18v04'})
        assert rendered == 'root/%Y/%m/file_h18v04_%Y%m%d.tif'

    def test_render_with_datetime(self):
        """Test rendering with datetime and tags."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        rendered = key_pattern.render(
            time=dt.datetime(2024, 2, 20),
            tags={'tile': 'h18v04'}
        )
        assert rendered == 'root/2024/02/file_h18v04_20240220.tif'

    def test_render_with_timestep_and_signatures(self):
        """Test rendering from timestep with supported time signatures."""
        key_pattern = KeyPattern('root/file_%Y%m%d.tif')
        timestep = Day.from_date(dt.datetime(2024, 2, 20))

        assert key_pattern.render(time=timestep, time_signature='start') == 'root/file_20240220.tif'
        assert key_pattern.render(time=timestep, time_signature='end') == 'root/file_20240220.tif'
        assert key_pattern.render(time=timestep, time_signature='end+1') == 'root/file_20240221.tif'

    def test_render_invalid_time_signature_raises(self):
        """Test invalid time signature raises ValueError."""
        key_pattern = KeyPattern('root/file_%Y%m%d.tif')
        timestep = Day.from_date(dt.datetime(2024, 2, 20))

        with pytest.raises(ValueError):
            key_pattern.render(time=timestep, time_signature='invalid')


class TestKeyPatternMatch:
    """Test KeyPattern.match method."""

    def test_match_basic_pattern(self):
        """Test matching a standard date+tag key."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        parsed = key_pattern.match('root/2024/02/file_h18v04_20240220.tif')

        assert isinstance(parsed, ParsedKey)
        assert parsed.key == 'root/2024/02/file_h18v04_20240220.tif'
        assert parsed.time == dt.datetime(2024, 2, 20)
        assert parsed.tags == {'tile': 'h18v04'}

    def test_match_with_doy_pattern(self):
        """Test matching with day-of-year placeholder (%j)."""
        key_pattern = KeyPattern('root/%Y/file_%Y%j.tif')
        parsed = key_pattern.match('root/2024/file_2024060.tif')

        assert parsed.time == dt.datetime(2024, 2, 29)
        assert parsed.tags == {}

    def test_match_with_special_char_tag_name(self):
        """Test matching tags whose names include . - and # characters."""
        key_pattern = KeyPattern('root/%Y/file_{my.tag-1#x}_%Y%m%d.tif')
        parsed = key_pattern.match('root/2024/file_A_20240115.tif')

        assert parsed.time == dt.datetime(2024, 1, 15)
        assert parsed.tags == {'my.tag-1#x': 'A'}

    def test_match_non_matching_key_raises(self):
        """Test ValueError is raised when key does not match pattern."""
        key_pattern = KeyPattern('root/%Y/%m/file_%Y%m%d.tif')

        with pytest.raises(ValueError):
            key_pattern.match('root/file_without_date.tif')


class TestKeyPatternPrefix:
    """Test KeyPattern.prefix method."""

    def test_prefix_without_time(self):
        """Test unresolved placeholders are stripped from prefix."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        prefix = key_pattern.prefix(tags={'tile': 'h18v04'})
        assert prefix == 'root'

    def test_prefix_with_datetime(self):
        """Test prefix resolution with a specific datetime."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        prefix = key_pattern.prefix(
            time=dt.datetime(2024, 2, 20),
            tags={'tile': 'h18v04'}
        )
        assert prefix == 'root/2024/02'

    def test_prefix_with_timerange(self):
        """Test prefix resolution with a TimeRange."""
        key_pattern = KeyPattern('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        time_range = TimeRange('2024-02-01', '2024-02-29')
        prefix = key_pattern.prefix(time=time_range, tags={'tile': 'h18v04'})
        assert prefix == 'root/2024/02'
