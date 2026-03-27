"""
Tests for KeyParser and ParsedKey parsing utilities.

Tests rendering, matching, and prefix resolution behavior in the new parser
surface.
"""
import datetime as dt
import pytest

from d3tools.parse import KeyParser, ParsedKey
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


class TestKeyParserInitialization:
    """Test KeyParser initialization."""

    def test_init_stores_raw_pattern(self):
        """Test that constructor stores input pattern unchanged."""
        key_pattern = KeyParser('root/%Y/%m/file_%Y%m%d.tif')
        assert key_pattern.raw_pattern == 'root/%Y/%m/file_%Y%m%d.tif'


class TestKeyParserRender:
    """Test KeyParser.render method."""

    def test_render_without_time(self):
        """Test rendering with tags only (no datetime formatting)."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        rendered = key_pattern.render(tags={'tile': 'h18v04'})
        assert rendered == 'root/%Y/%m/file_h18v04_%Y%m%d.tif'

    def test_render_with_datetime(self):
        """Test rendering with datetime and tags."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        rendered = key_pattern.render(
            time=dt.datetime(2024, 2, 20),
            tags={'tile': 'h18v04'}
        )
        assert rendered == 'root/2024/02/file_h18v04_20240220.tif'

    def test_render_with_timestep_and_signatures(self):
        """Test rendering from timestep with supported time signatures."""
        key_pattern = KeyParser('root/file_%Y%m%d.tif')
        timestep = Day.from_date(dt.datetime(2024, 2, 20))

        assert key_pattern.render(time=timestep, time_signature='start') == 'root/file_20240220.tif'
        assert key_pattern.render(time=timestep, time_signature='end') == 'root/file_20240220.tif'
        assert key_pattern.render(time=timestep, time_signature='end+1') == 'root/file_20240221.tif'

    def test_render_invalid_time_signature_raises(self):
        """Test invalid time signature raises ValueError."""
        key_pattern = KeyParser('root/file_%Y%m%d.tif')
        timestep = Day.from_date(dt.datetime(2024, 2, 20))

        with pytest.raises(ValueError):
            key_pattern.render(time=timestep, time_signature='invalid')


class TestKeyParserMatch:
    """Test KeyParser.match method."""

    def test_match_basic_pattern(self):
        """Test matching a standard date+tag key."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        parsed = key_pattern.match('root/2024/02/file_h18v04_20240220.tif')

        assert isinstance(parsed, ParsedKey)
        assert parsed.key == 'root/2024/02/file_h18v04_20240220.tif'
        assert parsed.time == dt.datetime(2024, 2, 20)
        assert parsed.tags == {'tile': 'h18v04'}

    def test_match_with_doy_pattern(self):
        """Test matching with day-of-year placeholder (%j)."""
        key_pattern = KeyParser('root/%Y/file_%Y%j.tif')
        parsed = key_pattern.match('root/2024/file_2024060.tif')

        assert parsed.time == dt.datetime(2024, 2, 29)
        assert parsed.tags == {}

    def test_match_with_special_char_tag_name(self):
        """Test matching tags whose names include . - and # characters."""
        key_pattern = KeyParser('root/%Y/file_{my.tag-1#x}_%Y%m%d.tif')
        parsed = key_pattern.match('root/2024/file_A_20240115.tif')

        assert parsed.time == dt.datetime(2024, 1, 15)
        assert parsed.tags == {'my.tag-1#x': 'A'}

    def test_match_non_matching_key_raises(self):
        """Test ValueError is raised when key does not match pattern."""
        key_pattern = KeyParser('root/%Y/%m/file_%Y%m%d.tif')

        with pytest.raises(ValueError):
            key_pattern.match('root/file_without_date.tif')

    def test_match_with_duplicate_tag_placeholders_same_value(self):
        """Test duplicate placeholders map to one tag when values are consistent."""
        key_pattern = KeyParser('root/{tile}/file_{tile}_%Y%m%d.tif')
        parsed = key_pattern.match('root/h18v04/file_h18v04_20240220.tif')

        assert parsed.time == dt.datetime(2024, 2, 20)
        assert parsed.tags == {'tile': 'h18v04'}

    def test_match_with_duplicate_tag_placeholders_different_values_raises(self):
        """Test duplicate placeholders with different values raise ValueError."""
        key_pattern = KeyParser('root/{tile}/file_{tile}_%Y%m%d.tif')

        with pytest.raises(ValueError):
            key_pattern.match('root/h18v04/file_h19v04_20240220.tif')

    def test_match_with_hour_minute_second(self):
        """Test matching parses hour/minute/second directives."""
        key_pattern = KeyParser('root/file_%Y%m%d_%H%M%S.tif')
        parsed = key_pattern.match('root/file_20240220_134501.tif')

        assert parsed.time == dt.datetime(2024, 2, 20, 13, 45, 1)
        assert parsed.tags == {}

    def test_match_file_version_allows_path_fragments(self):
        """Test file_version placeholder can include path separators."""
        key_pattern = KeyParser('root/{file_version}/file_%Y%m%d.tif')
        parsed = key_pattern.match('root/v1.2/build/file_20240220.tif')

        assert parsed.time == dt.datetime(2024, 2, 20)
        assert parsed.tags == {'file_version': 'v1.2/build'}


class TestKeyParserPrefix:
    """Test KeyParser.prefix method."""

    def test_prefix_without_time(self):
        """Test unresolved placeholders are stripped from prefix."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        prefix = key_pattern.prefix(tags={'tile': 'h18v04'})
        assert prefix == 'root'

    def test_prefix_with_datetime(self):
        """Test prefix resolution with a specific datetime."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        prefix = key_pattern.prefix(
            time=dt.datetime(2024, 2, 20),
            tags={'tile': 'h18v04'}
        )
        assert prefix == 'root/2024/02'

    def test_prefix_with_timerange(self):
        """Test prefix resolution with a TimeRange."""
        key_pattern = KeyParser('root/%Y/%m/file_{tile}_%Y%m%d.tif')
        time_range = TimeRange('2024-02-01', '2024-02-29')
        prefix = key_pattern.prefix(time=time_range, tags={'tile': 'h18v04'})
        assert prefix == 'root/2024/02'

    def test_prefix_with_timerange_and_doy(self):
        """Test prefix resolves day-of-year when range is a single day."""
        key_pattern = KeyParser('root/%Y/%j/file_%Y%j.tif')
        time_range = TimeRange('2024-02-29', '2024-02-29')
        prefix = key_pattern.prefix(time=time_range)
        assert prefix == 'root/2024/060'


class TestKeyParserCaching:
    """Test KeyParser internal matcher caching behavior."""

    def test_compiled_matcher_is_cached(self):
        """Test that compiled matcher metadata is reused between calls."""
        key_pattern = KeyParser('root/%Y/%m/file_%Y%m%d.tif')

        matcher1 = key_pattern._get_compiled_matcher()
        matcher2 = key_pattern._get_compiled_matcher()

        assert matcher1 is matcher2
