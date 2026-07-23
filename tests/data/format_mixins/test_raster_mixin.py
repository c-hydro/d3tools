"""
Comprehensive tests for RasterMixin functionality.

Tests cover:
- Dataset instantiation with NetCDF/GeoTIFF files
- Read operations (xarray DataArray/Dataset)
- Write operations (NetCDF, GeoTIFF, chunked writing)
- Template management (creation, application, get/set)
- Coordinate system handling (CRS, straightening)
- Metadata operations
- Nodata value handling
- Edge cases
"""
import pytest
import xarray as xr
import rioxarray as rxr
import numpy as np
import datetime as dt
from pathlib import Path

from d3tools.data import LocalDataset, MemoryDataset


class TestRasterInstantiation:
    """Test that raster files correctly instantiate with RasterMixin."""
    
    def test_local_dataset_netcdf_instantiation(self, tmp_path):
        """Test LocalDataset with .nc file gets RasterMixin."""
        # Create simple NetCDF
        data = xr.DataArray(
            np.random.rand(10, 10),
            dims=['y', 'x'],
            coords={'y': np.arange(10), 'x': np.arange(10)}
        )
        nc_file = tmp_path / "test.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.nc")
        
        assert 'RasterMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'netcdf'
    
    def test_local_dataset_geotiff_instantiation(self, tmp_path):
        """Test LocalDataset with .tif file gets RasterMixin."""
        # Create simple GeoTIFF
        data = xr.DataArray(
            np.random.rand(1, 10, 10),
            dims=['band', 'y', 'x'],
            coords={
                'band': [1],
                'y': np.linspace(10, 0, 10),
                'x': np.linspace(0, 10, 10)
            }
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        tif_file = tmp_path / "test.tif"
        data.rio.to_raster(tif_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.tif")
        
        assert 'RasterMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'geotiff'
    
    def test_memory_dataset_netcdf_instantiation(self):
        """Test MemoryDataset with .nc extension gets RasterMixin."""
        dataset = MemoryDataset(key_pattern="data.nc")
        
        assert 'RasterMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'netcdf'
    
    def test_raster_mixin_has_template_manager(self, tmp_path):
        """Test that RasterMixin initializes template_manager."""
        data = xr.DataArray(np.random.rand(10, 10), dims=['y', 'x'])
        nc_file = tmp_path / "test.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.nc")
        
        assert hasattr(dataset, 'template_manager')
        assert dataset.template_manager is not None


class TestRasterReadNetCDF:
    """Test reading NetCDF files."""
    
    def test_read_simple_netcdf_dataarray(self, tmp_path):
        """Test reading a simple NetCDF with single variable."""
        data = xr.DataArray(
            np.arange(20).reshape(4, 5),
            dims=['y', 'x'],
            coords={'y': np.arange(4), 'x': np.arange(5)},
            attrs={'units': 'meters', '_FillValue': -9999}
        )
        nc_file = tmp_path / "simple.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="simple.nc")
        read_data = dataset.get_data()
        
        assert isinstance(read_data, xr.DataArray)
        assert read_data.shape == (4, 5)
        assert 'y' in read_data.dims
        assert 'x' in read_data.dims
    
    def test_read_netcdf_dataset_single_var(self, tmp_path):
        """Test that Dataset with single variable is converted to DataArray."""
        da = xr.DataArray(
            np.random.rand(10, 10),
            dims=['y', 'x'],
            name='temperature'
        )
        ds = da.to_dataset()
        nc_file = tmp_path / "single_var.nc"
        ds.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="single_var.nc")
        read_data = dataset.get_data()
        
        # Should be converted to DataArray
        assert isinstance(read_data, xr.DataArray)
        assert read_data.name == 'temperature'
    
    def test_read_netcdf_preserves_attributes(self, tmp_path):
        """Test that NetCDF attributes are preserved."""
        data = xr.DataArray(
            np.random.rand(5, 5),
            dims=['y', 'x'],
            attrs={
                'units': 'mm',
                'long_name': 'precipitation',
                'source': 'test'
            }
        )
        nc_file = tmp_path / "attrs.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="attrs.nc")
        read_data = dataset.get_data()
        
        assert read_data.attrs['units'] == 'mm'
        assert read_data.attrs['source'] == 'test'
    
    def test_read_netcdf_with_time_dimension(self, tmp_path):
        """Test reading NetCDF with time dimension."""
        times = [dt.datetime(2023, 1, i) for i in range(1, 6)]
        data = xr.DataArray(
            np.random.rand(5, 10, 10),
            dims=['time', 'y', 'x'],
            coords={'time': times, 'y': np.arange(10), 'x': np.arange(10)}
        )
        nc_file = tmp_path / "timeseries.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="timeseries.nc")
        read_data = dataset.get_data()
        
        assert 'time' in read_data.dims
        assert len(read_data.time) == 5


class TestRasterReadGeoTIFF:
    """Test reading GeoTIFF files."""
    
    def test_read_simple_geotiff(self, tmp_path):
        """Test reading a simple GeoTIFF."""
        data = xr.DataArray(
            np.random.rand(1, 10, 10),
            dims=['band', 'y', 'x'],
            coords={
                'band': [1],
                'y': np.linspace(10, 0, 10),
                'x': np.linspace(0, 10, 10)
            }
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        tif_file = tmp_path / "simple.tif"
        data.rio.to_raster(tif_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="simple.tif")
        read_data = dataset.get_data()
        
        assert isinstance(read_data, xr.DataArray)
        assert 'band' in read_data.dims or 'y' in read_data.dims
    
    def test_read_geotiff_preserves_crs(self, tmp_path):
        """Test that CRS is preserved when reading GeoTIFF."""
        data = xr.DataArray(
            np.random.rand(1, 10, 10),
            dims=['band', 'y', 'x']
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        data.rio.write_coordinate_system(inplace=True)
        tif_file = tmp_path / "crs_test.tif"
        data.rio.to_raster(tif_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="crs_test.tif")
        read_data = dataset.get_data()
        
        assert read_data.rio.crs is not None
        assert "4326" in str(read_data.rio.crs)
    
    def test_read_large_geotiff_uses_chunks(self, tmp_path):
        """Test that large GeoTIFFs are read with dask chunks."""
        # Create a file that will be > 500 MB threshold
        # For testing, we'll just check the logic without creating huge file
        # Instead create small file and test with low threshold
        data = xr.DataArray(
            np.random.rand(1, 100, 100),
            dims=['band', 'y', 'x']
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        tif_file = tmp_path / "large.tif"
        data.rio.to_raster(tif_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="large.tif")
        # Read with very low threshold to trigger chunking
        # we use the raw _read_from_file method because get_data() doesn't pass down kwargs
        full_path = dataset.get_key()
        read_data = dataset._read_from_file(full_path, chunk_threshold=0.001)
        
        # If chunked, data.chunks will not be None
        assert isinstance(read_data, xr.DataArray)
        assert read_data.chunks is not None


class TestRasterWriteNetCDF:
    """Test writing NetCDF files."""
    
    def test_write_simple_netcdf(self, tmp_path):
        """Test writing a simple NetCDF file."""
        data = xr.DataArray(
            np.arange(20).reshape(4, 5),
            dims=['y', 'x'],
            coords={'y': np.arange(4), 'x': np.arange(5)},
            attrs={'_FillValue': -9999}
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="output.nc")
        dataset.write_data(data, as_is=True)
        
        # Verify file was written
        nc_file = tmp_path / "output.nc"
        assert nc_file.exists()
        
        # Read back and verify
        written_data = xr.open_dataarray(nc_file, engine = 'h5netcdf')
        assert written_data.shape == (4, 5)
    
    def test_write_netcdf_overwrites(self, tmp_path):
        """Test that write overwrites existing NetCDF."""
        data1 = xr.DataArray(np.ones((3, 3)), dims=['y', 'x'])
        nc_file = tmp_path / "overwrite.nc"
        data1.to_netcdf(nc_file, engine = 'h5netcdf')
        
        data2 = xr.DataArray(np.ones((5, 5)) * 2, dims=['y', 'x'], attrs={'_FillValue': -9999})
        
        dataset = LocalDataset(path=str(tmp_path), file="overwrite.nc")
        dataset.write_data(data2, as_is=True)
        
        # Read back and verify it was overwritten
        written_data = xr.open_dataarray(nc_file, engine = 'h5netcdf')
        assert written_data.shape == (5, 5)
        assert float(written_data.mean()) == 2.0
    
    def test_write_netcdf_creates_directory(self, tmp_path):
        """Test that write creates parent directories."""
        data = xr.DataArray(np.random.rand(5, 5), dims=['y', 'x'], attrs={'_FillValue': -9999})
        
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="test.nc")
        dataset.write_data(data, as_is=True)
        
        nc_file = nested_path / "test.nc"
        assert nc_file.exists()
    
    def test_write_netcdf_preserves_attributes(self, tmp_path):
        """Test that attributes are preserved when writing."""
        data = xr.DataArray(
            np.random.rand(5, 5),
            dims=['y', 'x'],
            attrs={
                'units': 'mm',
                'source': 'test',
                '_FillValue': -9999
            }
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="attrs.nc")
        dataset.write_data(data, as_is=True)
        
        written_data = xr.open_dataarray(tmp_path / "attrs.nc", engine = 'h5netcdf')
        assert written_data.attrs['units'] == 'mm'
        assert written_data.attrs['source'] == 'test'


class TestRasterWriteGeoTIFF:
    """Test writing GeoTIFF files."""
    
    def test_write_simple_geotiff(self, tmp_path):
        """Test writing a simple GeoTIFF file."""
        data = xr.DataArray(
            np.random.rand(1, 10, 10),
            dims=['band', 'y', 'x'],
            coords={
                'band': [1],
                'y': np.linspace(10, 0, 10),
                'x': np.linspace(0, 10, 10)
            },
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        
        dataset = LocalDataset(path=str(tmp_path), file="output.tif")
        dataset.write_data(data, as_is=True)
        
        # Verify file was written
        tif_file = tmp_path / "output.tif"
        assert tif_file.exists()
        
        # Read back and verify
        written_data = rxr.open_rasterio(tif_file)
        assert written_data.shape[1:] == (10, 10)  # Ignore band dimension
    
    def test_write_geotiff_preserves_crs(self, tmp_path):
        """Test that CRS is preserved when writing GeoTIFF."""
        data = xr.DataArray(
            np.random.rand(1, 10, 10),
            dims=['band', 'y', 'x'],
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        data.rio.write_coordinate_system(inplace=True)
        
        dataset = LocalDataset(path=str(tmp_path), file="crs.tif")
        dataset.write_data(data, as_is=True)
        
        written_data = rxr.open_rasterio(tmp_path / "crs.tif")
        assert written_data.rio.crs is not None
        assert "4326" in str(written_data.rio.crs)
    
    def test_write_geotiff_chunked(self, tmp_path):
        """Test writing chunked GeoTIFF (triggers save_raster_in_chunks)."""
        data = xr.DataArray(
            np.random.rand(1, 100, 100),
            dims=['band', 'y', 'x'],
            coords={
                'band': [1],
                'y': np.linspace(10, 0, 100),
                'x': np.linspace(0, 10, 100)
            },
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        # Make it chunked
        data = data.chunk({'y': 50, 'x': 50})
        
        dataset = LocalDataset(path=str(tmp_path), file="chunked.tif")
        dataset.write_data(data, as_is=True)
        
        tif_file = tmp_path / "chunked.tif"
        assert tif_file.exists()


class TestRasterTemplateManagement:
    """Test template management integration with RasterMixin.
    
    Note: Detailed template functionality is tested in test_template_manager.py
    and test_template_manager_advanced.py. These tests focus on RasterMixin's
    integration with the template system.
    """
    
    def test_raster_mixin_has_template_manager(self, tmp_path):
        """Test that RasterMixin properly initializes template_manager."""
        data = xr.DataArray(
            np.random.rand(10, 10),
            dims=['y', 'x'],
            coords={'y': np.arange(10), 'x': np.arange(10)},
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "test.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.tif")
        
        # RasterMixin should have initialized template_manager
        assert hasattr(dataset, 'template_manager')
        from d3tools.spatial.template_manager import TemplateManager
        assert isinstance(dataset.template_manager, TemplateManager)
    
    def test_template_integration_workflow(self, tmp_path):
        """Test basic template workflow through RasterMixin interface."""
        data = xr.DataArray(
            np.random.rand(10, 10),
            dims=['y', 'x'],
            coords={
                'y': np.linspace(45, 35, 10),
                'x': np.linspace(-10, 0, 10)
            },
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        nc_file = tmp_path / "template_test.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="template_test.tif")
        
        # Set template
        dataset.set_template(data)
        
        # Get template - should exist now
        template_dict = dataset.get_template_dict(make_it=False)
        assert template_dict is not None
        
        # Apply template to new data
        numpy_data = np.random.rand(10, 10)
        result = dataset.set_data_to_template(numpy_data, template_dict)
        
        assert isinstance(result, xr.DataArray)
        assert result.shape == (10, 10)


class TestRasterCoordinateHandling:
    """Test coordinate system operations."""
    
    def test_straighten_data_descending_lats(self, tmp_path):
        """Test that data is sorted to have descending latitudes."""
        # Create data with ascending latitudes
        data = xr.DataArray(
            np.arange(20).reshape(4, 5),
            dims=['y', 'x'],
            coords={
                'y': np.array([10, 20, 30, 40]),  # Ascending
                'x': np.arange(5)
            },
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        data.rio.write_coordinate_system(inplace=True)

        nc_file = tmp_path / "ascending.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="ascending.tif")
        read_data = dataset.get_data()
        
        # Should be sorted to descending
        assert read_data.y.values[0] > read_data.y.values[-1]
    
    def test_nodata_handling(self, tmp_path):
        """Test that nodata values are properly handled."""
        data = xr.DataArray(
            np.array([[1, 2, -9999], [4, 5, 6]]),
            dims=['y', 'x'],
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "nodata.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="nodata.tif")
        read_data = dataset.get_data()
        
        # _FillValue should be in attrs
        assert '_FillValue' in read_data.attrs


class TestRasterMetadataMethods:
    """Test metadata operations on raster data."""
    
    def test_set_metadata(self, tmp_path):
        """Test set_metadata method."""
        data = xr.DataArray(
            np.random.rand(5, 5),
            dims=['y', 'x'],
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "meta_test.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="meta_test.tif")
        
        updated_data = dataset.set_metadata(data, source='test', version='1.0')
        
        assert 'source' in updated_data.attrs
        assert updated_data.attrs['source'] == 'test'
        assert 'time_produced' in updated_data.attrs
    
    def test_update_metadata(self, tmp_path):
        """Test update_metadata method."""
        data = xr.DataArray(
            np.random.rand(5, 5),
            dims=['y', 'x'],
            attrs={'old_key': 'old_value', '_FillValue': -9999}
        )
        nc_file = tmp_path / "update_test.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="update_test.tif")
        
        updated_data = dataset.update_metadata(data, new_key='new_value')
        
        assert 'old_key' in updated_data.attrs
        assert 'new_key' in updated_data.attrs
    
    def test_get_metadata(self, tmp_path):
        """Test get_metadata method."""
        data = xr.DataArray(
            np.random.rand(5, 5),
            dims=['y', 'x'],
            attrs={'key1': 'value1', 'key2': 'value2', '_FillValue': -9999}
        )
        nc_file = tmp_path / "get_meta.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="get_meta.tif")
        
        # Get all metadata
        metadata = dataset.get_metadata(data)
        assert 'key1' in metadata
        assert 'key2' in metadata
        
        # Get specific keys
        metadata = dataset.get_metadata(data, keys=['key1'])
        assert metadata == {'key1': 'value1'}


class TestRasterMemoryDataset:
    """Test RasterMixin with MemoryDataset."""
    
    def test_memory_write_and_read(self):
        """Test writing and reading from MemoryDataset."""
        dataset = MemoryDataset(key_pattern="memory.tif")
        
        data = xr.DataArray(
            np.random.rand(10, 10),
            dims=['y', 'x'],
            coords={'y': np.arange(10), 'x': np.arange(10)},
            attrs={'_FillValue': -9999}
        )
        dataset.write_data(data)
        
        read_data = dataset.get_data()
        assert isinstance(read_data, xr.DataArray)
        assert read_data.shape == (10, 10)


class TestRasterRoundTrip:
    """Test round-trip operations (write then read)."""
    
    def test_roundtrip_netcdf(self, tmp_path):
        """Test that NetCDF writing and reading preserves data."""
        original_data = xr.DataArray(
            np.arange(400).reshape(20, 20),
            dims=['y', 'x'],
            coords={'y': np.linspace(10, 0, 20),
                    'x': np.linspace(0, 10, 20)},
            attrs={'units': 'mm', '_FillValue': -9999}
        )
        
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.nc")
        dataset.write_data(original_data, as_is=True)
        read_data = dataset.get_data()
        
        assert read_data.shape == original_data.shape
        assert read_data.attrs['units'] == 'mm'
        np.testing.assert_array_equal(read_data.values, original_data.values)
    
    def test_roundtrip_geotiff(self, tmp_path):
        """Test that GeoTIFF preserves data through round-trip."""
        original_data = xr.DataArray(
            np.arange(400).reshape(20, 20),
            dims=['y', 'x'],
            coords={
                'y': np.linspace(10, 0, 20),
                'x': np.linspace(0, 10, 20)
            },
            attrs={'_FillValue': -9999}
        )
        original_data.rio.write_crs("EPSG:4326", inplace=True)
        
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.tif")
        dataset.write_data(original_data, as_is=True)
        read_data = dataset.get_data()
        
        assert read_data.shape[1:] == original_data.shape  # Ignore band dim differences
        assert read_data.rio.crs is not None
        np.testing.assert_array_equal(read_data.values[0,:,:], original_data.values)

class TestRasterEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_dataarray_raises_error(self, tmp_path):
        """Test handling empty DataArray."""
        data = xr.DataArray(
            np.array([]).reshape(0, 5),
            dims=['y', 'x'],
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "empty.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="empty.tif")
        with pytest.raises(Exception):
            read_data = dataset.get_data()
    
    def test_large_dataarray(self, tmp_path):
        """Test handling large DataArray."""
        # Create moderately large array (not huge for test speed)
        data = xr.DataArray(
            np.random.rand(100, 100),
            dims=['y', 'x'],
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "large.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="large.tif")
        read_data = dataset.get_data()
        
        assert read_data.shape == (1, 100, 100)
    
    def test_multidimensional_data(self, tmp_path):
        """Test handling 3D+ data."""
        data = xr.DataArray(
            np.random.rand(3, 10, 10),
            dims=['time', 'y', 'x'],
            coords={
                'time': [1, 2, 3],
                'y': np.arange(10),
                'x': np.arange(10)
            },
            attrs={'_FillValue': -9999}
        )
        nc_file = tmp_path / "3d.nc"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="3d.nc")
        read_data = dataset.get_data()
        
        assert len(read_data.dims) == 3
        assert 'time' in read_data.dims


class TestRasterFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties initializes template_manager."""
        data = xr.DataArray(np.random.rand(5, 5), dims=['y', 'x'], attrs={'_FillValue': -9999})
        nc_file = tmp_path / "test.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.tif")
        
        assert hasattr(dataset, 'template_manager')
        assert dataset.template_manager is not None
    
    def test_validate_data(self, tmp_path):
        """Test validate_data method."""
        data = xr.DataArray(
            np.random.rand(1, 5, 5),
            dims=['band', 'y', 'x'],
            attrs={'_FillValue': -9999}
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        nc_file = tmp_path / "validate.tif"
        data.to_netcdf(nc_file, engine = 'h5netcdf')
        
        dataset = LocalDataset(path=str(tmp_path), file="validate.tif")
        
        validated = dataset.validate_data(data)
        assert isinstance(validated, xr.DataArray)
        # Should have nodata written
        assert validated.rio.nodata is not None or '_FillValue' in validated.attrs


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
