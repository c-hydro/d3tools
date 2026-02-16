"""
Comprehensive tests for StructuredTextMixin functionality.

Tests cover:
- Dataset instantiation with JSON files
- Read operations (JSON parsing)
- Write operations (JSON serialization)
- GeoJSON detection and delegation to VectorMixin
- Append mode for JSON lists
- Special data types (datetime, numpy arrays)
- Edge cases
"""
import pytest
import json
import datetime as dt
import numpy as np
from pathlib import Path
import warnings
import geopandas as gpd  # For GeoJSON handling in VectorMixin

from d3tools.data import LocalDataset, MemoryDataset
from d3tools.data.format_mixins.vector_mixin import VectorMixin
from unittest.mock import patch

class TestStructuredTextInstantiation:
    """Test that JSON files correctly instantiate with StructuredTextMixin."""
    
    def test_local_dataset_json_instantiation(self, tmp_path):
        """Test LocalDataset with .json file gets StructuredTextMixin."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"key": "value"}')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.json")
        
        assert 'StructuredTextMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'json'
    
    def test_memory_dataset_json_instantiation(self):
        """Test MemoryDataset with .json extension gets StructuredTextMixin."""
        dataset = MemoryDataset(key_pattern="data.json")
        
        assert 'StructuredTextMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'json'


class TestStructuredTextReadJSON:
    """Test reading JSON files."""
    
    def test_read_simple_json(self, tmp_path):
        """Test reading a simple JSON file."""
        json_file = tmp_path / "test.json"
        data = {"name": "test", "value": 42, "active": True}
        json_file.write_text(json.dumps(data))
        
        dataset = LocalDataset(path=str(tmp_path), file="test.json")
        result = dataset.get_data()
        
        assert result == data
    
    def test_read_nested_json(self, tmp_path):
        """Test reading nested JSON structures."""
        json_file = tmp_path / "nested.json"
        data = {
            "level1": {
                "level2": {
                    "level3": ["a", "b", "c"]
                }
            },
            "array": [1, 2, 3]
        }
        json_file.write_text(json.dumps(data))
        
        dataset = LocalDataset(path=str(tmp_path), file="nested.json")
        result = dataset.get_data()
        
        assert result == data
        assert result["level1"]["level2"]["level3"] == ["a", "b", "c"]
    
    def test_read_json_list(self, tmp_path):
        """Test reading JSON that contains a list at root level."""
        json_file = tmp_path / "list.json"
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        json_file.write_text(json.dumps(data))
        
        dataset = LocalDataset(path=str(tmp_path), file="list.json")
        result = dataset.get_data()
        
        assert result == data
        assert len(result) == 3
    
    def test_read_empty_json(self, tmp_path):
        """Test reading an empty JSON object."""
        json_file = tmp_path / "empty.json"
        json_file.write_text('{}')
        
        dataset = LocalDataset(path=str(tmp_path), file="empty.json")
        result = dataset.get_data()
        
        assert result == {}
    
    def test_read_json_with_unicode(self, tmp_path):
        """Test reading JSON with unicode characters."""
        json_file = tmp_path / "unicode.json"
        data = {"text": "Hello 世界", "emoji": "🌍", "greek": "Γειά"}
        json_file.write_text(json.dumps(data, ensure_ascii=False))
        
        dataset = LocalDataset(path=str(tmp_path), file="unicode.json")
        result = dataset.get_data()
        
        assert result == data
    
    def test_read_nonexistent_json_file(self, tmp_path):
        """Test reading a file that doesn't exist raises error."""
        dataset = LocalDataset(path=str(tmp_path), file="nonexistent.json")
        
        with pytest.raises(FileNotFoundError):
            dataset.get_data()
    
    def test_read_invalid_json(self, tmp_path):
        """Test reading invalid JSON raises error."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text('{"key": invalid}')
        
        dataset = LocalDataset(path=str(tmp_path), file="invalid.json")
        
        with pytest.raises(json.JSONDecodeError):
            dataset.get_data()


class TestStructuredTextWriteJSON:
    """Test writing JSON files."""
    
    def test_write_simple_json(self, tmp_path):
        """Test writing simple JSON data."""
        dataset = LocalDataset(path=str(tmp_path), file="output.json")
        
        data = {"name": "test", "value": 42}
        dataset.write_data(data)
        
        # Verify file was written correctly
        json_file = tmp_path / "output.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        assert written_data == data
    
    def test_write_json_overwrites(self, tmp_path):
        """Test that write overwrites existing content."""
        json_file = tmp_path / "overwrite.json"
        json_file.write_text('{"old": "data"}')
        
        dataset = LocalDataset(path=str(tmp_path), file="overwrite.json")
        new_data = {"new": "data"}
        dataset.write_data(new_data)
        
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        assert written_data == new_data
        assert "old" not in written_data
    
    def test_write_json_append_mode(self, tmp_path):
        """Test writing in append mode creates a list."""
        json_file = tmp_path / "append.json"
        json_file.write_text('{"id": 1}')
        
        dataset = LocalDataset(path=str(tmp_path), file="append.json")
        dataset.write_data({"id": 2}, append=True)
        
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        # Should create a list with both objects
        assert isinstance(written_data, list)
        assert len(written_data) == 2
        assert written_data[0] == {"id": 1}
        assert written_data[1] == {"id": 2}
    
    def test_write_json_append_to_list(self, tmp_path):
        """Test appending to existing JSON list."""
        json_file = tmp_path / "list.json"
        json_file.write_text('[{"id": 1}, {"id": 2}]')
        
        dataset = LocalDataset(path=str(tmp_path), file="list.json")
        dataset.write_data({"id": 3}, append=True)
        
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        assert len(written_data) == 3
        assert written_data[2] == {"id": 3}
    
    def test_write_creates_directory(self, tmp_path):
        """Test that write creates parent directories."""
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="file.json")
        
        dataset.write_data({"test": "data"})
        
        json_file = nested_path / "file.json"
        assert json_file.exists()
    
    def test_write_json_with_formatting(self, tmp_path):
        """Test that written JSON is properly formatted (indented)."""
        dataset = LocalDataset(path=str(tmp_path), file="formatted.json")
        
        data = {"nested": {"key": "value"}}
        dataset.write_data(data)
        
        json_file = tmp_path / "formatted.json"
        content = json_file.read_text()
        
        # Should be indented
        assert '\n' in content
        assert '    ' in content  # 4-space indent


class TestStructuredTextSpecialTypes:
    """Test handling of special data types (datetime, numpy arrays, etc.)."""
    
    def test_write_numpy_array_conversion(self, tmp_path):
        """Test that numpy arrays are converted to lists."""
        dataset = LocalDataset(path=str(tmp_path), file="numpy.json")
        
        data = {"array": np.array([1, 2, 3, 4, 5])}
        dataset.write_data(data)
        
        json_file = tmp_path / "numpy.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        assert written_data["array"] == [1, 2, 3, 4, 5]
        assert isinstance(written_data["array"], list)
    
    def test_write_datetime_conversion(self, tmp_path):
        """Test that datetime objects are converted to ISO format."""
        dataset = LocalDataset(path=str(tmp_path), file="datetime.json")
        
        test_datetime = dt.datetime(2023, 6, 15, 12, 30, 45)
        data = {"timestamp": test_datetime}
        dataset.write_data(data)
        
        json_file = tmp_path / "datetime.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        assert written_data["timestamp"] == "2023-06-15T12:30:45"
    
    def test_write_date_conversion(self, tmp_path):
        """Test that date objects are converted to ISO format."""
        dataset = LocalDataset(path=str(tmp_path), file="date.json")
        
        test_date = dt.date(2023, 6, 15)
        data = {"date": test_date}
        dataset.write_data(data)
        
        json_file = tmp_path / "date.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        assert written_data["date"] == "2023-06-15"
    
    def test_write_numpy_datetime_conversion(self, tmp_path):
        """Test that numpy datetime64 is converted to string."""
        dataset = LocalDataset(path=str(tmp_path), file="np_datetime.json")
        
        test_datetime = np.datetime64('2023-06-15T12:30:45')
        data = {"timestamp": test_datetime}
        dataset.write_data(data)
        
        json_file = tmp_path / "np_datetime.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        # Should be converted to ISO string
        assert isinstance(written_data["timestamp"], str)
        assert "2023-06-15" in written_data["timestamp"]
    
    def test_write_mixed_types(self, tmp_path):
        """Test handling multiple special types in one object."""
        dataset = LocalDataset(path=str(tmp_path), file="mixed.json")
        
        data = {
            "string": "text",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "array": np.array([1, 2, 3]),
            "datetime": dt.datetime(2023, 1, 1),
            "nested": {
                "date": dt.date(2024, 1, 1)
            }
        }
        dataset.write_data(data)
        
        json_file = tmp_path / "mixed.json"
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        assert written_data["string"] == "text"
        assert written_data["number"] == 42
        assert written_data["array"] == [1, 2, 3]
        assert isinstance(written_data["datetime"], str)
        assert isinstance(written_data["nested"]["date"], str)


class TestStructuredTextGeoJSONDetection:
    """Test GeoJSON detection and delegation to VectorMixin."""
    
    def test_detects_geojson_on_read_with_features_key(self, tmp_path):
        """Test that JSON with 'features' key is detected as GeoJSON during read."""
        json_file = tmp_path / "geojson_disguised.json"
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 0.0]
                    },
                    "properties": {"name": "test"}
                }
            ]
        }
        json_file.write_text(json.dumps(geojson_data))
        
        dataset = LocalDataset(path=str(tmp_path), file="geojson_disguised.json")
        
        # Should emit warning about .json extension
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            data = dataset.get_data()
            
            # Should have issued a warning
            assert len(w) == 1
            assert "appears to be GeoJSON" in str(w[0].message)
            assert ".geojson" in str(w[0].message)
        
        # Format should be changed to 'geojson'
        assert dataset.format == 'geojson'
        
        # Data should be a GeoDataFrame (delegated to VectorMixin)
        assert isinstance(data, gpd.GeoDataFrame)
    
    def test_detects_geojson_on_write_with_geodataframe(self, tmp_path):
        """Test that writing GeoDataFrame to .json file detects it as GeoJSON."""
        dataset = LocalDataset(path=str(tmp_path), file="output.json")
        
        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame({
            'id': [1, 2],
            'name': ['Point A', 'Point B']
        }, geometry=gpd.points_from_xy([0.0, 1.0], [0.0, 1.0]), crs="EPSG:4326")
        
        # Should emit warning about .json extension when writing GeoDataFrame
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dataset.write_data(gdf)
            
            # Should have issued a warning
            assert len(w) == 1
            assert "appears to be GeoJSON" in str(w[0].message)
            assert ".geojson" in str(w[0].message)
        
        # Format should be changed to 'geojson'
        assert dataset.format == 'geojson'
        
        # File should exist and be valid JSON
        json_file = tmp_path / "output.json"
        assert json_file.exists()
        
        with open(json_file, 'r') as f:
            written_data = json.load(f)
        
        # Should be GeoJSON FeatureCollection
        assert written_data['type'] == 'FeatureCollection'
        assert 'features' in written_data
        assert len(written_data['features']) == 2
    
    def test_normal_json_with_features_word(self, tmp_path):
        """Test that 'features' as a value doesn't trigger GeoJSON detection."""
        json_file = tmp_path / "normal.json"
        data = {
            "description": "This app has many features",
            "feature_count": 5
        }
        json_file.write_text(json.dumps(data))
        
        dataset = LocalDataset(path=str(tmp_path), file="normal.json")
        
        # Should not trigger GeoJSON detection
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = dataset.get_data()
            
            # Should not issue any warnings
            assert len(w) == 0
        
        # Format should remain 'json'
        assert dataset.format == 'json'
        
        # Result should be plain dict
        assert isinstance(result, dict)
        assert result == data
    
    def test_geojson_roundtrip_through_json_extension(self, tmp_path):
        """Test complete round-trip: write GeoDataFrame to .json, read back as GeoDataFrame."""
        dataset_write = LocalDataset(path=str(tmp_path), file="roundtrip.json")
        
        # Create original GeoDataFrame
        original_gdf = gpd.GeoDataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        }, geometry=gpd.points_from_xy([0.0, 1.0, 2.0], [0.0, 1.0, 2.0]), crs="EPSG:4326")
        
        # Write (should detect as GeoJSON)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            dataset_write.write_data(original_gdf)
        
        # Read back (should detect as GeoJSON)
        dataset_read = LocalDataset(path=str(tmp_path), file="roundtrip.json")
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            read_gdf = dataset_read.get_data()
        
        # Should be GeoDataFrame
        assert isinstance(read_gdf, gpd.GeoDataFrame)
        assert len(read_gdf) == 3
        assert list(read_gdf['id']) == [1, 2, 3]
        assert list(read_gdf['value']) == [10, 20, 30]
    
    def test_write_regular_dict_to_json_stays_json(self, tmp_path):
        """Test that writing regular dict to .json doesn't trigger GeoJSON detection."""
        dataset = LocalDataset(path=str(tmp_path), file="regular.json")
        
        regular_data = {
            "key1": "value1",
            "key2": 42,
            "nested": {"inner": "data"}
        }
        
        # Should not emit warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dataset.write_data(regular_data)
            
            # Should not issue any warnings
            assert len(w) == 0
        
        # Format should remain 'json'
        assert dataset.format == 'json'
        
        # Read back should be plain dict
        dataset_read = LocalDataset(path=str(tmp_path), file="regular.json")
        read_data = dataset_read.get_data()
        assert isinstance(read_data, dict)
        assert read_data == regular_data
    
    def test_geojson_delegation_preserves_metadata(self, tmp_path):
        """Test that GeoJSON delegation preserves metadata through write/read."""
        dataset_write = LocalDataset(path=str(tmp_path), file="with_metadata.json")
        
        # Create GeoDataFrame with metadata
        gdf = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=gpd.points_from_xy([0.0], [0.0]), crs="EPSG:4326")
        gdf.attrs = {'source': 'test', 'version': '1.0'}
        
        # Write
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            dataset_write.write_data(gdf)
        
        # Read back
        dataset_read = LocalDataset(path=str(tmp_path), file="with_metadata.json")
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            read_gdf = dataset_read.get_data()
        
        # Metadata should be preserved
        assert 'source' in read_gdf.attrs
        assert read_gdf.attrs['source'] == 'test'
        assert read_gdf.attrs['version'] == '1.0'

class TestStructuredTextMemoryDataset:
    """Test StructuredTextMixin with MemoryDataset."""
    
    def test_memory_write_and_read_json(self):
        """Test writing and reading from MemoryDataset (JSON)."""
        dataset = MemoryDataset(key_pattern="memory.json")
        
        data = {"key": "value", "number": 42}
        dataset.write_data(data)
        
        read_data = dataset.get_data()
        assert read_data == data
    
    def test_memory_append_not_working_json(self):
        """Test that append mode doesn't work in MemoryDataset (JSON)."""
        dataset = MemoryDataset(key_pattern="memory.json")
        
        data1 = {"id1": 1}
        dataset.write_data(data1)
        
        data2 = {"id2": 2}
        dataset.write_data(data2, append=True)
        
        read_data = dataset.get_data()
        assert isinstance(read_data, dict)
        assert read_data == data2  # Should overwrite, not append


class TestStructuredTextRoundTrip:
    """Test round-trip operations (write then read)."""
    
    def test_roundtrip_preserves_data(self, tmp_path):
        """Test that JSON writing and reading preserves exact data."""
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.json")
        
        original_data = {
            "string": "test",
            "integer": 42,
            "float": 3.14159,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "nested": {"key": "value"}
        }
        
        dataset.write_data(original_data)
        read_data = dataset.get_data()
        
        assert read_data == original_data
    
    def test_roundtrip_with_special_types(self, tmp_path):
        """Test round-trip with type conversions."""
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip_types.json")
        
        original_data = {
            "array": np.array([1, 2, 3]),
            "datetime": dt.datetime(2023, 6, 15)
        }
        
        dataset.write_data(original_data)
        read_data = dataset.get_data()
        
        # Types will be converted to JSON-compatible types
        assert read_data["array"] == [1, 2, 3]
        assert read_data["datetime"] == "2023-06-15T00:00:00"


class TestStructuredTextEdgeCases:
    """Test edge cases and error handling."""
    
    def test_special_characters_in_data(self, tmp_path):
        """Test handling special characters in data."""
        dataset = LocalDataset(path=str(tmp_path), file="special.json")
        
        data = {
            "quotes": 'He said "hello"',
            "newline": "line1\nline2",
            "tab": "col1\tcol2",
            "backslash": "path\\to\\file"
        }
        
        dataset.write_data(data)
        read_data = dataset.get_data()
        
        assert read_data == data
    
    def test_large_json_structure(self, tmp_path):
        """Test handling large JSON structures."""
        dataset = LocalDataset(path=str(tmp_path), file="large.json")
        
        # Create large data structure
        data = {"items": [{"id": i, "value": i * 2} for i in range(1000)]}
        
        dataset.write_data(data)
        read_data = dataset.get_data()
        
        assert len(read_data["items"]) == 1000
        assert read_data["items"][500]["id"] == 500


class TestStructuredTextFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties doesn't break initialization."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"test": true}')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.json")
        
        assert dataset is not None
        assert hasattr(dataset, 'format')
    
    def test_format_after_read_passthrough(self, tmp_path):
        """Test _format_after_read for non-GeoJSON is passthrough."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"key": "value"}')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.json")
        
        test_data = {"key": "value"}
        processed = dataset._format_after_read(test_data)
        assert processed == test_data
    
    def test_format_before_write_converts_types(self, tmp_path):
        """Test _format_before_write converts special types."""
        dataset = LocalDataset(path=str(tmp_path), file="test.json")
        
        data = {
            "array": np.array([1, 2, 3]),
            "datetime": dt.datetime(2023, 1, 1)
        }
        
        prepared = dataset._format_before_write(data)
        
        # Should have converted types
        assert isinstance(prepared["array"], list)
        assert isinstance(prepared["datetime"], str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
