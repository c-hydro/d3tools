"""
Comprehensive tests for VectorMixin functionality.

Tests cover:
- Dataset instantiation with Shapefile/GeoJSON files
- Read operations (geopandas GeoDataFrame)
- Write operations (to shapefile, geojson)
- Metadata handling in GeoJSON
- Append mode for both formats
- Datetime conversion in GeoJSON
- CRS handling
- Edge cases
"""
import pytest
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
import datetime as dt
import numpy as np
import json
from pathlib import Path

from d3tools.data import LocalDataset, MemoryDataset

class TestVectorInstantiation:
    """Test that vector files correctly instantiate with VectorMixin."""
    
    def test_local_dataset_shapefile_instantiation(self, tmp_path):
        """Test LocalDataset with .shp file gets VectorMixin."""
        # Create a simple shapefile
        gdf = gpd.GeoDataFrame(
            {'id': [1]}, 
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "test.shp"
        gdf.to_file(shp_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.shp")
        
        assert 'VectorMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'shp'
    
    def test_local_dataset_geojson_instantiation(self, tmp_path):
        """Test LocalDataset with .geojson file gets VectorMixin."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]}, 
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        geojson_file = tmp_path / "test.geojson"
        gdf.to_file(geojson_file, driver='GeoJSON')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.geojson")
        
        assert 'VectorMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'geojson'  # geojson extension maps to json format
    
    def test_memory_dataset_geojson_instantiation(self):
        """Test MemoryDataset with .geojson extension gets VectorMixin."""
        dataset = MemoryDataset(key_pattern="data.geojson")
        
        assert 'VectorMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'geojson'


class TestVectorReadShapefile:
    """Test reading Shapefile format."""
    
    def test_read_simple_shapefile(self, tmp_path):
        """Test reading a simple shapefile."""
        gdf = gpd.GeoDataFrame(
            {'id': [1, 2, 3], 'name': ['A', 'B', 'C']},
            geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "test.shp"
        gdf.to_file(shp_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.shp")
        data = dataset.get_data()
        
        assert isinstance(data, gpd.GeoDataFrame)
        assert len(data) == 3
        assert list(data['id']) == [1, 2, 3]
        assert list(data['name']) == ['A', 'B', 'C']
    
    def test_read_shapefile_preserves_crs(self, tmp_path):
        """Test that CRS is preserved when reading shapefile."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "crs_test.shp"
        gdf.to_file(shp_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="crs_test.shp")
        data = dataset.get_data()
        
        assert data.crs is not None
        assert data.crs.to_string() == "EPSG:4326"
    
    def test_read_shapefile_different_geometries(self, tmp_path):
        """Test reading shapefile with different geometry types."""
        # Points
        gdf_points = gpd.GeoDataFrame(
            {'type': ['point']},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        points_file = tmp_path / "points.shp"
        gdf_points.to_file(points_file)
        
        # Lines
        gdf_lines = gpd.GeoDataFrame(
            {'type': ['line']},
            geometry=[LineString([(0, 0), (1, 1)])],
            crs="EPSG:4326"
        )
        lines_file = tmp_path / "lines.shp"
        gdf_lines.to_file(lines_file)
        
        # Polygons
        gdf_polygons = gpd.GeoDataFrame(
            {'type': ['polygon']},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326"
        )
        polygons_file = tmp_path / "polygons.shp"
        gdf_polygons.to_file(polygons_file)
        
        # Test reading each
        dataset_points = LocalDataset(path=str(tmp_path), file="points.shp")
        data_points = dataset_points.get_data()
        assert data_points.geometry.iloc[0].geom_type == 'Point'
        
        dataset_lines = LocalDataset(path=str(tmp_path), file="lines.shp")
        data_lines = dataset_lines.get_data()
        assert data_lines.geometry.iloc[0].geom_type == 'LineString'
        
        dataset_polygons = LocalDataset(path=str(tmp_path), file="polygons.shp")
        data_polygons = dataset_polygons.get_data()
        assert data_polygons.geometry.iloc[0].geom_type == 'Polygon'


class TestVectorReadGeoJSON:
    """Test reading GeoJSON format."""
    
    def test_read_simple_geojson(self, tmp_path):
        """Test reading a simple GeoJSON file."""
        gdf = gpd.GeoDataFrame(
            {'id': [1, 2], 'value': [10, 20]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326"
        )
        geojson_file = tmp_path / "test.geojson"
        gdf.to_file(geojson_file, driver='GeoJSON')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.geojson")
        data = dataset.get_data()
        
        assert isinstance(data, gpd.GeoDataFrame)
        assert len(data) == 2
        assert list(data['id']) == [1, 2]
    
    def test_read_geojson_with_metadata(self, tmp_path):
        """Test reading GeoJSON with metadata field."""
        geojson_file = tmp_path / "with_metadata.geojson"
        geojson_content = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                    "properties": {"id": 1}
                }
            ],
            "metadata": {
                "source": "test",
                "version": "1.0",
                "date": "2023-01-01"
            }
        }
        with open(geojson_file, 'w') as f:
            json.dump(geojson_content, f)
        
        dataset = LocalDataset(path=str(tmp_path), file="with_metadata.geojson")
        data = dataset.get_data()
        
        # Metadata should be in attrs
        assert hasattr(data, 'attrs')
        assert data.attrs['source'] == 'test'
        assert data.attrs['version'] == '1.0'
        assert data.attrs['date'] == '2023-01-01'
    
    def test_read_geojson_without_metadata(self, tmp_path):
        """Test reading GeoJSON without metadata field."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        geojson_file = tmp_path / "no_metadata.geojson"
        gdf.to_file(geojson_file, driver='GeoJSON')
        
        dataset = LocalDataset(path=str(tmp_path), file="no_metadata.geojson")
        data = dataset.get_data()
        
        assert isinstance(data, gpd.GeoDataFrame)
        # attrs may exist but should be empty or not have 'metadata' key
        if hasattr(data, 'attrs'):
            assert 'metadata' not in data.attrs or data.attrs == {}

class TestVectorWriteShapefile:
    """Test writing Shapefile format."""
    
    def test_write_simple_shapefile(self, tmp_path):
        """Test writing a simple shapefile."""
        gdf = gpd.GeoDataFrame(
            {'id': [1, 2], 'name': ['A', 'B']},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="output.shp")
        dataset.write_data(gdf)
        
        # Verify file was written
        shp_file = tmp_path / "output.shp"
        assert shp_file.exists()
        
        # Read back and verify
        written_gdf = gpd.read_file(shp_file)
        assert len(written_gdf) == 2
        assert list(written_gdf['id']) == [1, 2]
    
    def test_write_shapefile_overwrites(self, tmp_path):
        """Test that write overwrites existing shapefile."""
        gdf1 = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "overwrite.shp"
        gdf1.to_file(shp_file)
        
        gdf2 = gpd.GeoDataFrame(
            {'id': [2, 3]},
            geometry=[Point(1, 1), Point(2, 2)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="overwrite.shp")
        dataset.write_data(gdf2)
        
        # Read back and verify it was overwritten
        written_gdf = gpd.read_file(shp_file)
        assert len(written_gdf) == 2
        assert list(written_gdf['id']) == [2, 3]
    
    def test_write_shapefile_append_mode(self, tmp_path):
        """Test writing in append mode."""
        gdf1 = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "append.shp"
        gdf1.to_file(shp_file)
        
        gdf2 = gpd.GeoDataFrame(
            {'id': [2]},
            geometry=[Point(1, 1)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="append.shp")
        dataset.write_data(gdf2, append=True)
        
        # Read back and verify both features exist
        written_gdf = gpd.read_file(shp_file)
        assert len(written_gdf) == 2
        assert list(written_gdf['id']) == [1, 2]
    
    def test_write_creates_directory(self, tmp_path):
        """Test that write creates parent directories."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="test.shp")
        dataset.write_data(gdf)
        
        shp_file = nested_path / "test.shp"
        assert shp_file.exists()


class TestVectorWriteGeoJSON:
    """Test writing GeoJSON format."""
    
    def test_write_simple_geojson(self, tmp_path):
        """Test writing a simple GeoJSON file."""
        gdf = gpd.GeoDataFrame(
            {'id': [1, 2], 'value': [10, 20]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="output.geojson")
        dataset.write_data(gdf)
        
        # Verify file was written
        geojson_file = tmp_path / "output.geojson"
        assert geojson_file.exists()
        
        # Read back and verify
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
        
        assert geojson_data['type'] == 'FeatureCollection'
        assert len(geojson_data['features']) == 2
    
    def test_write_geojson_with_metadata(self, tmp_path):
        """Test writing GeoJSON with metadata in attrs."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        gdf.attrs = {
            'source': 'test_source',
            'version': '2.0',
            'description': 'Test data'
        }
        
        dataset = LocalDataset(path=str(tmp_path), file="with_meta.geojson")
        dataset.write_data(gdf)
        
        # Read back and verify metadata was saved
        geojson_file = tmp_path / "with_meta.geojson"
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
        
        assert 'metadata' in geojson_data
        assert geojson_data['metadata']['source'] == 'test_source'
        assert geojson_data['metadata']['version'] == '2.0'
    
    def test_write_geojson_append_mode(self, tmp_path):
        """Test writing GeoJSON in append mode creates a list."""
        gdf1 = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        geojson_file = tmp_path / "append.geojson"
        gdf1.to_file(geojson_file, driver='GeoJSON')
        
        gdf2 = gpd.GeoDataFrame(
            {'id': [2]},
            geometry=[Point(1, 1)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="append.geojson")
        dataset.write_data(gdf2, append=True)
        
        # Read back and verify it's a list with both FeatureCollections
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
        
        assert isinstance(geojson_data, list)
        assert len(geojson_data) == 2
        assert geojson_data[0]['type'] == 'FeatureCollection'
        assert geojson_data[1]['type'] == 'FeatureCollection'
    
    def test_write_geojson_formatting(self, tmp_path):
        """Test that GeoJSON is written with proper indentation."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="formatted.geojson")
        dataset.write_data(gdf)
        
        geojson_file = tmp_path / "formatted.geojson"
        content = geojson_file.read_text()
        
        # Should be indented with 4 spaces
        assert '\n' in content
        assert '    ' in content


class TestVectorDatetimeConversion:
    """Test datetime conversion in GeoJSON format."""
    
    def test_write_geojson_converts_datetime(self, tmp_path):
        """Test that datetime objects are converted to ISO strings in GeoJSON."""
        gdf = gpd.GeoDataFrame(
            {'id': [1], 'timestamp': [dt.datetime(2023, 6, 15, 12, 30)]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="datetime.geojson")
        dataset.write_data(gdf)
        
        geojson_file = tmp_path / "datetime.geojson"
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
        
        # Timestamp should be converted to ISO string
        feature = geojson_data['features'][0]
        assert isinstance(feature['properties']['timestamp'], str)
        assert '2023-06-15' in feature['properties']['timestamp']
    
    def test_write_geojson_converts_date(self, tmp_path):
        """Test that date objects are converted to ISO strings in GeoJSON."""
        gdf = gpd.GeoDataFrame(
            {'id': [1], 'date': [dt.date(2023, 6, 15)]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="date.geojson")
        dataset.write_data(gdf)
        
        geojson_file = tmp_path / "date.geojson"
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
        
        feature = geojson_data['features'][0]
        assert feature['properties']['date'] == '2023-06-15'
    
    def test_write_geojson_converts_numpy_datetime(self, tmp_path):
        """Test that numpy datetime64 is converted to object in GeoJSON."""
        gdf = gpd.GeoDataFrame(
            {'id': [1], 'time': [np.datetime64('2023-06-15T12:30')]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="np_datetime.geojson")
        dataset.write_data(gdf)
        
        # Should not raise an error
        geojson_file = tmp_path / "np_datetime.geojson"
        assert geojson_file.exists()


class TestVectorMetadataMethods:
    """Test metadata handling methods."""
    
    def test_set_metadata(self, tmp_path):
        """Test set_metadata method."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="test.geojson")
        
        # Set metadata
        updated_gdf = dataset.set_metadata(gdf, source='test', version='1.0')
        
        assert 'source' in updated_gdf.attrs
        assert updated_gdf.attrs['source'] == 'test'
        assert updated_gdf.attrs['version'] == '1.0'
        assert 'time_produced' in updated_gdf.attrs
    
    def test_update_metadata(self, tmp_path):
        """Test update_metadata method."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        gdf.attrs = {'old_key': 'old_value'}
        
        dataset = LocalDataset(path=str(tmp_path), file="test.geojson")
        
        # Update metadata
        updated_gdf = dataset.update_metadata(gdf, new_key='new_value')
        
        assert 'old_key' in updated_gdf.attrs
        assert 'new_key' in updated_gdf.attrs
        assert updated_gdf.attrs['new_key'] == 'new_value'
    
    def test_get_metadata(self, tmp_path):
        """Test get_metadata method."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        gdf.attrs = {'key1': 'value1', 'key2': 'value2'}
        
        dataset = LocalDataset(path=str(tmp_path), file="test.geojson")
        
        # Get all metadata
        metadata = dataset.get_metadata(gdf)
        assert metadata == {'key1': 'value1', 'key2': 'value2'}
        
        # Get specific keys
        metadata = dataset.get_metadata(gdf, keys=['key1'])
        assert metadata == {'key1': 'value1'}
        
        # Get single key as string
        metadata = dataset.get_metadata(gdf, keys='key2')
        assert metadata == {'key2': 'value2'}


class TestVectorMemoryDataset:
    """Test VectorMixin with MemoryDataset."""
    
    def test_memory_write_and_read_geojson(self):
        """Test writing and reading from MemoryDataset (GeoJSON)."""
        dataset = MemoryDataset(key_pattern="memory.geojson")
        
        gdf = gpd.GeoDataFrame(
            {'id': [1, 2]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326"
        )
        dataset.write_data(gdf)
        
        read_gdf = dataset.get_data()
        assert isinstance(read_gdf, gpd.GeoDataFrame)
        assert len(read_gdf) == 2


class TestVectorRoundTrip:
    """Test round-trip operations (write then read)."""
    
    def test_roundtrip_shapefile(self, tmp_path):
        """Test that shapefile writing and reading preserves data."""
        original_gdf = gpd.GeoDataFrame(
            {'id': [1, 2, 3], 'name': ['A', 'B', 'C']},
            geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.shp")
        dataset.write_data(original_gdf)
        read_gdf = dataset.get_data()
        
        assert len(read_gdf) == len(original_gdf)
        assert list(read_gdf['id']) == list(original_gdf['id'])
        assert list(read_gdf['name']) == list(original_gdf['name'])
        assert read_gdf.crs.to_string() == original_gdf.crs.to_string()
    
    def test_roundtrip_geojson_with_metadata(self, tmp_path):
        """Test that GeoJSON preserves metadata through round-trip."""
        original_gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        original_gdf.attrs = {'source': 'test', 'version': '1.0'}
        
        dataset_write = LocalDataset(path=str(tmp_path), file="roundtrip.geojson")
        dataset_write.write_data(original_gdf)
        
        dataset_read = LocalDataset(path=str(tmp_path), file="roundtrip.geojson")
        read_gdf = dataset_read.get_data()
        
        assert read_gdf.attrs['source'] == 'test'
        assert read_gdf.attrs['version'] == '1.0'


class TestVectorEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_geodataframe(self, tmp_path):
        """Test handling empty GeoDataFrame."""
        gdf = gpd.GeoDataFrame(columns=['id', 'geometry'], crs="EPSG:4326")
        
        dataset = LocalDataset(path=str(tmp_path), file="empty.geojson")
        dataset.write_data(gdf)
        
        geojson_file = tmp_path / "empty.geojson"
        assert geojson_file.exists()
    
    def test_large_geodataframe(self, tmp_path):
        """Test handling large GeoDataFrame."""
        # Create 1000 points
        points = [Point(i, i) for i in range(1000)]
        gdf = gpd.GeoDataFrame(
            {'id': range(1000)},
            geometry=points,
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="large.geojson")
        dataset.write_data(gdf)
        
        read_gdf = dataset.get_data()
        assert len(read_gdf) == 1000
    
    def test_special_characters_in_properties(self, tmp_path):
        """Test handling special characters in feature properties."""
        gdf = gpd.GeoDataFrame(
            {'name': ['Test "quoted"', 'Line\nbreak', 'Tab\there']},
            geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
            crs="EPSG:4326"
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="special.geojson")
        dataset.write_data(gdf)
        
        read_gdf = dataset.get_data()
        assert list(read_gdf['name']) == list(gdf['name'])


class TestVectorFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties doesn't break initialization."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "test.shp"
        gdf.to_file(shp_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.shp")
        
        assert dataset is not None
        assert hasattr(dataset, 'format')
    
    def test_format_after_read_passthrough(self, tmp_path):
        """Test _format_after_read is a passthrough for vector data."""
        gdf = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326"
        )
        shp_file = tmp_path / "test.shp"
        gdf.to_file(shp_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.shp")
        
        processed = dataset._format_after_read(gdf)
        assert processed is gdf or processed.equals(gdf)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
