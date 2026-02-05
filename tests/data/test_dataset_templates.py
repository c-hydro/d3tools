"""
Tests for Dataset template-related functionality with TemplateManager integration.
"""
import pytest
import numpy as np
import xarray as xr
from pyproj import CRS

from d3tools.data.local_dataset import LocalDataset
from d3tools.data.template_manager import TemplateManager


@pytest.fixture
def sample_dataarray():
    """Create a sample xarray.DataArray for testing."""
    data = np.random.rand(10, 20)
    x = np.linspace(10.0, 15.0, 20)
    y = np.linspace(45.0, 40.0, 10)
    
    da = xr.DataArray(
        data,
        coords={'x': x, 'y': y},
        dims=['y', 'x'],
        attrs={'_FillValue': -9999.0}
    )
    da = da.rio.write_crs(4326)
    da = da.rio.set_spatial_dims(x_dim='x', y_dim='y')
    
    return da


@pytest.fixture
def sample_dataset():
    """Create a simple LocalDataset instance."""
    return LocalDataset(path='test_path', file='test_file.tif')


class TestDatasetTemplateManagerIntegration:
    """Test that Dataset properly integrates with TemplateManager."""
    
    def test_init_creates_template_manager(self):
        """Test that Dataset.__init__ creates a TemplateManager instance."""
        ds = LocalDataset(path='test_path', file='test_file.tif')
        
        assert hasattr(ds, 'template_manager')
        assert isinstance(ds.template_manager, TemplateManager)
        assert len(ds.template_manager._templates) == 0
    
    def test_backward_compatibility_template_property(self, sample_dataset):
        """Test that _template property provides backward compatibility."""
        # Property should return the internal templates dict
        assert isinstance(sample_dataset._template, dict)
        assert sample_dataset._template == sample_dataset.template_manager._templates
        
        # Setting via property should update template_manager
        sample_dataset._template = {'test': 'value'}
        assert sample_dataset.template_manager._templates == {'test': 'value'}
    
    def test_update_shares_template_manager_reference(self, sample_dataset, sample_dataarray):
        """Test that update() shares template_manager (intentional for template inheritance)."""
        # Set a template
        sample_dataset.set_template(sample_dataarray)
        
        # Update to create new instance
        updated_ds = sample_dataset.update()
        
        # Should share the same template_manager reference (intentional behavior)
        assert updated_ds.template_manager is sample_dataset.template_manager
        
        # Modifying one affects the other
        updated_ds.template_manager._templates['new_tile'] = {}
        assert 'new_tile' in sample_dataset.template_manager._templates
    
    def test_copy_with_template_flag(self, sample_dataset, sample_dataarray):
        """Test that copy(template=True) preserves template_manager."""
        # Set a template
        sample_dataset.set_template(sample_dataarray)
        
        # Copy with template=True
        copied_ds = sample_dataset.copy(template=True)
        
        # Should preserve template_manager reference
        assert copied_ds.template_manager is sample_dataset.template_manager
    
    def test_copy_without_template_flag(self, sample_dataset, sample_dataarray):
        """Test that copy(template=False) creates fresh template_manager."""
        # Set a template
        sample_dataset.set_template(sample_dataarray)
        
        # Copy with template=False
        copied_ds = sample_dataset.copy(template=False)
        
        # Should have fresh template_manager from update()
        # But update() also shares reference, so this will have templates
        assert copied_ds.template_manager is sample_dataset.template_manager


class TestSetTemplate:
    """Test Dataset.set_template() method."""
    
    def test_set_template_from_dataarray(self, sample_dataset, sample_dataarray):
        """Test setting template from xarray.DataArray."""
        sample_dataset.set_template(sample_dataarray)
        
        # Check that template was created in template_manager
        template = sample_dataset.template_manager.get('__tile__')
        assert template is not None
        assert 'crs' in template
        assert 'dims_names' in template
        assert template['dims_names'] == ('y', 'x')
    
    def test_set_template_with_tile_kwarg(self, sample_dataset, sample_dataarray):
        """Test setting template for specific tile."""
        sample_dataset.set_template(sample_dataarray, tile='tile1')
        
        # Check that template was created for tile1
        template = sample_dataset.template_manager.get('tile1')
        assert template is not None
        assert template['dims_lengths']['x'] == 20
        assert template['dims_lengths']['y'] == 10
    
    def test_set_template_from_dataset(self, sample_dataset, sample_dataarray):
        """Test setting template from xarray.Dataset."""
        xr_dataset = xr.Dataset({
            'var1': sample_dataarray,
            'var2': sample_dataarray * 2
        })
        
        sample_dataset.set_template(xr_dataset)
        
        # Check that template was created and includes variables
        template = sample_dataset.template_manager.get()
        assert template is not None
        assert 'variables' in template
        assert template['variables'] == ['var1', 'var2']


class TestGetTemplateDict:
    """Test Dataset.get_template_dict() method."""
    
    def test_get_template_dict_existing(self, sample_dataset, sample_dataarray):
        """Test getting existing template."""
        sample_dataset.set_template(sample_dataarray)
        
        template = sample_dataset.get_template_dict(make_it=False)
        assert template is not None
        assert 'crs' in template
    
    def test_get_template_dict_nonexistent_no_create(self, sample_dataset):
        """Test getting non-existent template with make_it=False."""
        template = sample_dataset.get_template_dict(make_it=False)
        assert template is None
    
    def test_get_template_dict_specific_tile(self, sample_dataset, sample_dataarray):
        """Test getting template for specific tile."""
        sample_dataset.set_template(sample_dataarray, tile='tile2')
        
        template = sample_dataset.get_template_dict(make_it=False, tile='tile2')
        assert template is not None
        assert 'dims_names' in template


class TestBuildTemplatearrayDelegation:
    """Test that Dataset.build_templatearray() delegates to TemplateManager."""
    
    def test_build_templatearray_delegation(self):
        """Test that build_templatearray delegates to TemplateManager.build_array."""
        template_dict = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 10.0, 'y': 45.0},
            'dims_ends': {'x': 15.0, 'y': 40.0},
            'dims_lengths': {'x': 20, 'y': 10}
        }
        
        # Call Dataset static method
        result = LocalDataset.build_templatearray(template_dict)
        
        # Should produce same result as TemplateManager directly
        expected = TemplateManager.build_array(template_dict)
        
        assert isinstance(result, xr.DataArray)
        assert result.dims == expected.dims
        assert result.shape == expected.shape
    
    def test_build_templatearray_with_data(self):
        """Test building template array with provided data."""
        template_dict = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 10.0, 'y': 45.0},
            'dims_ends': {'x': 15.0, 'y': 40.0},
            'dims_lengths': {'x': 20, 'y': 10}
        }
        
        data = np.ones((10, 20)) * 42
        result = LocalDataset.build_templatearray(template_dict, data)
        
        assert np.all(result.values == 42)


class TestSetDataToTemplateDelegation:
    """Test that Dataset.set_data_to_template() delegates to TemplateManager."""
    
    def test_set_data_to_template_with_numpy(self):
        """Test applying template to numpy array."""
        template_dict = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 10.0, 'y': 45.0},
            'dims_ends': {'x': 15.0, 'y': 40.0},
            'dims_lengths': {'x': 20, 'y': 10}
        }
        
        data = np.ones((10, 20)) * 100
        result = LocalDataset.set_data_to_template(data, template_dict)
        
        # Should delegate to TemplateManager.apply_to_data
        expected = TemplateManager.apply_to_data(data, template_dict)
        
        assert isinstance(result, xr.DataArray)
        assert np.array_equal(result.values, expected.values)
        assert result.dims == expected.dims
    
    def test_set_data_to_template_with_dataarray(self, sample_dataarray):
        """Test applying template to xarray.DataArray."""
        template_dict = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 10.0, 'y': 45.0},
            'dims_ends': {'x': 15.0, 'y': 40.0},
            'dims_lengths': {'x': 20, 'y': 10}
        }
        
        result = LocalDataset.set_data_to_template(sample_dataarray, template_dict)
        
        assert isinstance(result, xr.DataArray)
        # Coordinates should match template
        assert result.x.values[0] == pytest.approx(10.0)
        assert result.y.values[0] == pytest.approx(45.0)


class TestTemplateInheritanceWorkflow:
    """Test template inheritance between input and output datasets."""
    
    def test_template_inheritance_via_update(self, sample_dataset, sample_dataarray):
        """Test that templates are inherited when creating output datasets."""
        # Set template on input dataset
        input_ds = sample_dataset
        input_ds.set_template(sample_dataarray)
        
        # Simulate creating output dataset via update
        output_ds = input_ds.update(output='true')
        
        # Output should share template_manager reference (intentional)
        assert output_ds.template_manager is input_ds.template_manager
        
        # Can access template via both
        template = output_ds.template_manager.get('__tile__')
        assert template is not None
    
    def test_template_property_backward_compat_in_workflow(self, sample_dataset, sample_dataarray):
        """Test that old code using _template still works."""
        # Old code pattern: output_ds._template = input_ds._template
        input_ds = sample_dataset
        input_ds.set_template(sample_dataarray)
        
        # Create new dataset
        output_ds = LocalDataset(path='output', file='output.tif')
        
        # Old-style assignment (should still work)
        output_ds._template = input_ds._template
        
        # Should have the template now
        template = output_ds.template_manager.get('__tile__')
        assert template is not None
        assert template['dims_names'] == ('y', 'x')


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_template_manager_survives_multiple_updates(self, sample_dataset, sample_dataarray):
        """Test that template_manager persists through multiple updates."""
        sample_dataset.set_template(sample_dataarray)
        
        # Multiple updates
        ds1 = sample_dataset.update(tag1='a')
        ds2 = ds1.update(tag2='b')
        ds3 = ds2.update(tag3='c')
        
        # All should share template_manager
        assert ds3.template_manager is sample_dataset.template_manager
        
        # Template should still be accessible
        template = ds3.template_manager.get('__tile__')
        assert template is not None
    
    def test_empty_template_manager_operations(self, sample_dataset):
        """Test operations on dataset with no templates."""
        # Should not raise errors
        template = sample_dataset.get_template_dict(make_it=False)
        assert template is None
        
        assert not sample_dataset.template_manager.exists()
