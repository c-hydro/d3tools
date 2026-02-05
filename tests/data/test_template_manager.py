"""
Tests for TemplateManager class.
"""
import pytest
import numpy as np
import xarray as xr
from pyproj import CRS

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
        attrs={'_FillValue': -9999.0, 'crs': CRS.from_epsg(4326)}
    )
    da = da.rio.write_crs(4326)
    da = da.rio.set_spatial_dims(x_dim='x', y_dim='y')
    
    return da


@pytest.fixture
def sample_dataset(sample_dataarray):
    """Create a sample xarray.Dataset for testing."""
    ds = xr.Dataset({
        'var1': sample_dataarray,
        'var2': sample_dataarray * 2
    })
    return ds


@pytest.fixture
def sample_template_dict():
    """Create a sample template dictionary."""
    return {
        'crs': CRS.from_epsg(4326).to_wkt(),
        '_FillValue': -9999.0,
        'dims_names': ('y', 'x'),
        'spatial_dims': ('x', 'y'),
        'dims_starts': {'x': 10.0, 'y': 45.0},
        'dims_ends': {'x': 15.0, 'y': 40.0},
        'dims_lengths': {'x': 20, 'y': 10}
    }


class TestTemplateManagerInit:
    """Test TemplateManager initialization."""
    
    def test_init(self):
        """Test that TemplateManager initializes with empty dict."""
        mgr = TemplateManager()
        assert isinstance(mgr._templates, dict)
        assert len(mgr._templates) == 0


class TestTemplateManagerSet:
    """Test template creation and storage."""
    
    def test_set_from_dataarray(self, sample_dataarray):
        """Test setting template from xarray.DataArray."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        assert 'tile1' in mgr._templates
        template = mgr._templates['tile1']
        
        # Check all required keys exist
        assert 'crs' in template
        assert '_FillValue' in template
        assert 'dims_names' in template
        assert 'spatial_dims' in template
        assert 'dims_starts' in template
        assert 'dims_ends' in template
        assert 'dims_lengths' in template
        
        # Check values
        assert template['dims_names'] == ('y', 'x')
        assert template['spatial_dims'] == ('x', 'y')
        assert template['dims_lengths']['x'] == 20
        assert template['dims_lengths']['y'] == 10
        assert template['_FillValue'] == -9999.0
    
    def test_set_from_dataset(self, sample_dataset):
        """Test setting template from xarray.Dataset."""
        mgr = TemplateManager()
        mgr.set(sample_dataset, spatial_key='tile1')
        
        assert 'tile1' in mgr._templates
        template = mgr._templates['tile1']
        
        # Should extract first variable
        assert 'variables' in template
        assert template['variables'] == ['var1', 'var2']
    
    def test_set_default_spatial_key(self, sample_dataarray):
        """Test setting template with default spatial key."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray)
        
        assert '__tile__' in mgr._templates
    
    def test_set_multiple_tiles(self, sample_dataarray):
        """Test setting templates for multiple tiles."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        mgr.set(sample_dataarray, spatial_key='tile2')
        
        assert len(mgr._templates) == 2
        assert 'tile1' in mgr._templates
        assert 'tile2' in mgr._templates


class TestTemplateManagerGet:
    """Test template retrieval."""
    
    def test_get_existing(self, sample_dataarray):
        """Test getting an existing template."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        template = mgr.get('tile1')
        assert template is not None
        assert template['dims_names'] == ('y', 'x')
    
    def test_get_nonexistent(self):
        """Test getting a non-existent template returns None."""
        mgr = TemplateManager()
        template = mgr.get('nonexistent')
        assert template is None
    
    def test_get_default_key(self, sample_dataarray):
        """Test getting with default spatial key."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray)
        
        template = mgr.get()
        assert template is not None


class TestTemplateManagerExists:
    """Test template existence checking."""
    
    def test_exists_specific_key(self, sample_dataarray):
        """Test checking if specific template exists."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        assert mgr.exists('tile1')
        assert not mgr.exists('tile2')
    
    def test_exists_any(self, sample_dataarray):
        """Test checking if any templates exist."""
        mgr = TemplateManager()
        assert not mgr.exists()
        
        mgr.set(sample_dataarray, spatial_key='tile1')
        assert mgr.exists()


class TestTemplateManagerGetAll:
    """Test getting all templates."""
    
    def test_get_all_empty(self):
        """Test getting all templates when empty."""
        mgr = TemplateManager()
        all_templates = mgr.get_all()
        assert all_templates == {}
    
    def test_get_all_multiple(self, sample_dataarray):
        """Test getting all templates with multiple tiles."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        mgr.set(sample_dataarray, spatial_key='tile2')
        
        all_templates = mgr.get_all()
        assert len(all_templates) == 2
        assert 'tile1' in all_templates
        assert 'tile2' in all_templates
    
    def test_get_all_returns_copy(self, sample_dataarray):
        """Test that get_all returns a copy, not reference."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        all_templates = mgr.get_all()
        all_templates['new_key'] = {}
        
        # Original should not be modified
        assert 'new_key' not in mgr._templates


class TestTemplateManagerClear:
    """Test template clearing."""
    
    def test_clear_specific(self, sample_dataarray):
        """Test clearing a specific template."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        mgr.set(sample_dataarray, spatial_key='tile2')
        
        mgr.clear('tile1')
        assert not mgr.exists('tile1')
        assert mgr.exists('tile2')
    
    def test_clear_all(self, sample_dataarray):
        """Test clearing all templates."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        mgr.set(sample_dataarray, spatial_key='tile2')
        
        mgr.clear()
        assert not mgr.exists()
    
    def test_clear_nonexistent(self):
        """Test clearing non-existent key doesn't raise error."""
        mgr = TemplateManager()
        mgr.clear('nonexistent')  # Should not raise


class TestTemplateManagerCopy:
    """Test template manager copying."""
    
    def test_shallow_copy(self, sample_dataarray):
        """Test shallow copy shares template dict reference."""
        mgr1 = TemplateManager()
        mgr1.set(sample_dataarray, spatial_key='tile1')
        
        mgr2 = mgr1.copy(deep=False)
        
        # They should have separate template dicts but same content
        assert mgr2._templates is not mgr1._templates
        assert mgr2._templates == mgr1._templates
    
    def test_deep_copy(self, sample_dataarray):
        """Test deep copy creates independent copy."""
        mgr1 = TemplateManager()
        mgr1.set(sample_dataarray, spatial_key='tile1')
        
        mgr2 = mgr1.copy(deep=True)
        
        # Modify mgr2
        mgr2.clear('tile1')
        
        # mgr1 should be unaffected
        assert mgr1.exists('tile1')
        assert not mgr2.exists('tile1')


class TestTemplateManagerIterSpatialKeys:
    """Test spatial key iteration."""
    
    def test_iter_empty(self):
        """Test iterating over empty manager."""
        mgr = TemplateManager()
        keys = list(mgr.iter_spatial_keys())
        assert keys == []
    
    def test_iter_multiple(self, sample_dataarray):
        """Test iterating over multiple keys."""
        mgr = TemplateManager()
        mgr.set(sample_dataarray, spatial_key='tile1')
        mgr.set(sample_dataarray, spatial_key='tile2')
        mgr.set(sample_dataarray, spatial_key='tile3')
        
        keys = list(mgr.iter_spatial_keys())
        assert set(keys) == {'tile1', 'tile2', 'tile3'}


class TestTemplateManagerBuildArray:
    """Test building xarray.DataArray from template."""
    
    def test_build_array_no_data(self, sample_template_dict):
        """Test building array with no data (filled with nodata)."""
        arr = TemplateManager.build_array(sample_template_dict)
        
        assert isinstance(arr, xr.DataArray)
        assert arr.dims == ('y', 'x')
        assert arr.shape == (10, 20)
        assert np.all(arr.values == -9999.0)
    
    def test_build_array_with_data(self, sample_template_dict):
        """Test building array with provided data."""
        data = np.ones((10, 20)) * 42
        arr = TemplateManager.build_array(sample_template_dict, data)
        
        assert isinstance(arr, xr.DataArray)
        assert np.all(arr.values == 42)
    
    def test_build_array_coordinates(self, sample_template_dict):
        """Test that coordinates are correctly set."""
        arr = TemplateManager.build_array(sample_template_dict)
        
        # Check coordinate bounds
        assert arr.x.values[0] == pytest.approx(10.0)
        assert arr.x.values[-1] == pytest.approx(15.0)
        assert arr.y.values[0] == pytest.approx(45.0)
        assert arr.y.values[-1] == pytest.approx(40.0)
    
    def test_build_array_crs(self, sample_template_dict):
        """Test that CRS is correctly set."""
        arr = TemplateManager.build_array(sample_template_dict)
        
        assert arr.rio.crs is not None
        assert arr.rio.crs == sample_template_dict['crs']


class TestTemplateManagerApplyToData:
    """Test applying template to data."""
    
    def test_apply_to_numpy(self, sample_template_dict):
        """Test applying template to numpy array."""
        data = np.ones((10, 20)) * 42
        result = TemplateManager.apply_to_data(data, sample_template_dict)
        
        assert isinstance(result, xr.DataArray)
        assert np.all(result.values == 42)
        assert result.dims == ('y', 'x')
    
    def test_apply_to_dataarray(self, sample_dataarray, sample_template_dict):
        """Test applying template to xarray.DataArray."""
        result = TemplateManager.apply_to_data(sample_dataarray, sample_template_dict)
        
        assert isinstance(result, xr.DataArray)
        # Coordinates should match template, not original
        assert result.x.values[0] == pytest.approx(10.0)
        assert result.y.values[0] == pytest.approx(45.0)
    
    def test_apply_to_dataset(self, sample_dataset, sample_template_dict):
        """Test applying template to xarray.Dataset."""
        # Add variables list to template
        template_with_vars = sample_template_dict.copy()
        template_with_vars['variables'] = ['var1', 'var2']
        
        result = TemplateManager.apply_to_data(sample_dataset, template_with_vars)
        
        assert isinstance(result, xr.Dataset)
        assert set(result.data_vars) == {'var1', 'var2'}
        
        # All variables should have template coordinates
        for var in result.data_vars:
            assert result[var].x.values[0] == pytest.approx(10.0)


class TestTemplateManagerIntegration:
    """Integration tests combining multiple operations."""
    
    def test_set_get_apply_workflow(self, sample_dataarray):
        """Test complete workflow: set template, get it, apply to new data."""
        mgr = TemplateManager()
        
        # Set template from sample
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        # Get template
        template = mgr.get('tile1')
        assert template is not None
        
        # Apply to new data
        new_data = np.ones((10, 20)) * 100
        result = TemplateManager.apply_to_data(new_data, template)
        
        assert isinstance(result, xr.DataArray)
        assert np.all(result.values == 100)
        # Should have same coordinates as original
        assert len(result.x) == len(sample_dataarray.x)
        assert len(result.y) == len(sample_dataarray.y)
    
    def test_multi_tile_workflow(self, sample_dataarray):
        """Test workflow with multiple tiles."""
        mgr = TemplateManager()
        
        # Set templates for multiple tiles
        for i in range(3):
            mgr.set(sample_dataarray, spatial_key=f'tile{i}')
        
        # Verify all exist
        for i in range(3):
            assert mgr.exists(f'tile{i}')
        
        # Iterate and apply
        for key in mgr.iter_spatial_keys():
            template = mgr.get(key)
            arr = TemplateManager.build_array(template)
            assert arr.shape == (10, 20)
