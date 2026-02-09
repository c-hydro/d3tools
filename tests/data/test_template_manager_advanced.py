"""
Tests for TemplateManager validation, caching, and error handling features.
"""
import pytest
import numpy as np
import xarray as xr
import tempfile
import json
from pathlib import Path
from pyproj import CRS

from d3tools.spatial.template_manager import TemplateManager
from d3tools.errors import TemplateValidationError


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
def temp_cache_dir():
    """Create a temporary directory for cache testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestTemplateManagerValidation:
    """Test template validation functionality."""
    
    def test_validation_enabled_by_default(self):
        """Test that validation is enabled by default."""
        mgr = TemplateManager()
        assert mgr._validate is True
    
    def test_validation_can_be_disabled(self):
        """Test that validation can be disabled."""
        mgr = TemplateManager(validate=False)
        assert mgr._validate is False
    
    def test_valid_template_passes_validation(self, sample_dataarray):
        """Test that a valid template passes validation."""
        mgr = TemplateManager(validate=True)
        
        # Should not raise
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        template = mgr.get('tile1')
        assert template is not None
    
    def test_validation_checks_required_keys(self):
        """Test that validation catches missing required keys."""
        mgr = TemplateManager(validate=True)
        
        # Create an incomplete template by calling _validate_template directly
        incomplete_template = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            'dims_names': ('y', 'x'),
            # Missing other required keys
        }
        
        with pytest.raises(TemplateValidationError) as exc_info:
            mgr._validate_template(incomplete_template, 'test_tile')
        
        assert 'Missing required keys' in str(exc_info.value)
        assert '_FillValue' in str(exc_info.value)
    
    def test_validation_checks_crs_not_none(self):
        """Test that validation catches None CRS."""
        mgr = TemplateManager(validate=True)
        
        invalid_template = {
            'crs': None,
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 0, 'y': 0},
            'dims_ends': {'x': 10, 'y': 10},
            'dims_lengths': {'x': 10, 'y': 10}
        }
        
        with pytest.raises(TemplateValidationError) as exc_info:
            mgr._validate_template(invalid_template, 'test_tile')
        
        assert 'CRS cannot be None' in str(exc_info.value)
    
    def test_validation_checks_spatial_dims_count(self):
        """Test that validation requires exactly 2 spatial dims."""
        mgr = TemplateManager(validate=True)
        
        invalid_template = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x', 'z'),
            'spatial_dims': ('x', 'y', 'z'),  # 3 instead of 2
            'dims_starts': {'x': 0, 'y': 0, 'z': 0},
            'dims_ends': {'x': 10, 'y': 10, 'z': 5},
            'dims_lengths': {'x': 10, 'y': 10, 'z': 5}
        }
        
        with pytest.raises(TemplateValidationError) as exc_info:
            mgr._validate_template(invalid_template, 'test_tile')
        
        assert 'exactly 2 spatial dimensions' in str(exc_info.value)
    
    def test_validation_checks_positive_lengths(self):
        """Test that validation requires positive dimension lengths."""
        mgr = TemplateManager(validate=True)
        
        invalid_template = {
            'crs': CRS.from_epsg(4326).to_wkt(),
            '_FillValue': -9999.0,
            'dims_names': ('y', 'x'),
            'spatial_dims': ('x', 'y'),
            'dims_starts': {'x': 0, 'y': 0},
            'dims_ends': {'x': 10, 'y': 10},
            'dims_lengths': {'x': -10, 'y': 10}  # Negative length
        }
        
        with pytest.raises(TemplateValidationError) as exc_info:
            mgr._validate_template(invalid_template, 'test_tile')
        
        assert 'Must be positive' in str(exc_info.value)
    
    def test_validation_can_be_bypassed(self, sample_dataarray):
        """Test that validation can be disabled for performance."""
        mgr = TemplateManager(validate=False)
        
        # Should work even with validation disabled
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        template = mgr.get('tile1')
        assert template is not None


class TestTemplateManagerCaching:
    """Test disk caching functionality."""
    
    def test_cache_dir_created_on_init(self, temp_cache_dir):
        """Test that cache directory is created on initialization."""
        cache_path = Path(temp_cache_dir) / "templates"
        mgr = TemplateManager(cache_dir=str(cache_path))
        
        assert cache_path.exists()
        assert cache_path.is_dir()
    
    def test_template_saved_to_cache(self, sample_dataarray, temp_cache_dir):
        """Test that templates are automatically saved to cache."""
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        # Check that cache file was created
        cache_file = Path(temp_cache_dir) / "template_tile1.json"
        assert cache_file.exists()
        
        # Check that it's valid JSON
        with open(cache_file, 'r') as f:
            cached_data = json.load(f)
        
        assert 'crs' in cached_data
        assert 'dims_names' in cached_data
    
    def test_template_loaded_from_cache(self, sample_dataarray, temp_cache_dir):
        """Test that templates can be loaded from cache."""
        # Create template and cache it
        mgr1 = TemplateManager(cache_dir=temp_cache_dir)
        mgr1.set(sample_dataarray, spatial_key='tile1')
        original_template = mgr1.get('tile1')
        
        # Create new manager with same cache dir
        mgr2 = TemplateManager(cache_dir=temp_cache_dir)
        
        # Template not in memory yet
        assert 'tile1' not in mgr2._templates
        
        # Get should load from cache
        loaded_template = mgr2.get('tile1', load_from_cache=True)
        
        assert loaded_template is not None
        assert loaded_template['crs'] == original_template['crs']
        assert loaded_template['dims_lengths'] == original_template['dims_lengths']
    
    def test_get_without_cache_load(self, sample_dataarray, temp_cache_dir):
        """Test that get with load_from_cache=False doesn't load."""
        mgr1 = TemplateManager(cache_dir=temp_cache_dir)
        mgr1.set(sample_dataarray, spatial_key='tile1')
        
        mgr2 = TemplateManager(cache_dir=temp_cache_dir)
        loaded_template = mgr2.get('tile1', load_from_cache=False)
        
        # Should return None since not in memory and didn't load from cache
        assert loaded_template is None
    
    def test_cache_with_multiple_tiles(self, sample_dataarray, temp_cache_dir):
        """Test caching with multiple tiles (e.g., 286 tiles scenario)."""
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        
        # Create templates for multiple tiles
        num_tiles = 10  # Use 10 for test speed
        for i in range(num_tiles):
            mgr.set(sample_dataarray, spatial_key=f'tile{i}')
        
        # Check all cache files created
        cache_files = list(Path(temp_cache_dir).glob("template_tile*.json"))
        assert len(cache_files) == num_tiles
        
        # Clear memory
        mgr._templates.clear()
        
        # Load one tile from cache
        tile5 = mgr.get('tile5', load_from_cache=True)
        assert tile5 is not None
        
        # Only tile5 should be in memory now
        assert len(mgr._templates) == 1
        assert 'tile5' in mgr._templates
    
    def test_clear_cache(self, sample_dataarray, temp_cache_dir):
        """Test clearing cached templates."""
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        
        # Create some cached templates
        for i in range(5):
            mgr.set(sample_dataarray, spatial_key=f'tile{i}')
        
        cache_files_before = list(Path(temp_cache_dir).glob("template_*.json"))
        assert len(cache_files_before) == 5
        
        # Clear cache
        mgr.clear_cache()
        
        cache_files_after = list(Path(temp_cache_dir).glob("template_*.json"))
        assert len(cache_files_after) == 0
    
    def test_corrupted_cache_file_handled(self, temp_cache_dir):
        """Test that corrupted cache files don't crash the system."""
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        
        # Create a corrupted cache file
        cache_file = Path(temp_cache_dir) / "template_corrupt.json"
        with open(cache_file, 'w') as f:
            f.write("{'invalid': json")  # Invalid JSON
        
        # Should return False and not crash
        result = mgr.load_from_cache('corrupt')
        assert result is False
        assert 'corrupt' not in mgr._templates
    
    def test_no_caching_without_cache_dir(self, sample_dataarray):
        """Test that caching is skipped when no cache_dir provided."""
        mgr = TemplateManager()  # No cache_dir
        
        mgr.set(sample_dataarray, spatial_key='tile1')
        
        # Template should be in memory
        assert 'tile1' in mgr._templates
        
        # But no cache operations should occur (no errors)
        mgr.clear_cache()  # Should not crash


class TestTemplateManagerCopyWithCache:
    """Test that copy() preserves cache settings."""
    
    def test_copy_preserves_cache_dir(self, temp_cache_dir):
        """Test that copy preserves cache_dir setting."""
        mgr1 = TemplateManager(cache_dir=temp_cache_dir, validate=False)
        mgr2 = mgr1.copy()
        
        assert mgr2._cache_dir == mgr1._cache_dir
        assert mgr2._validate == mgr1._validate
    
    def test_copy_preserves_validation_setting(self):
        """Test that copy preserves validation setting."""
        mgr1 = TemplateManager(validate=False)
        mgr2 = mgr1.copy()
        
        assert mgr2._validate is False


class TestTemplateManagerMemoryOptimization:
    """Test memory optimization scenarios."""
    
    def test_lazy_loading_reduces_memory(self, sample_dataarray, temp_cache_dir):
        """Test that lazy loading keeps memory usage low with many tiles."""
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        
        # Create 20 tiles
        num_tiles = 20
        for i in range(num_tiles):
            mgr.set(sample_dataarray, spatial_key=f'tile{i}')
        
        # Clear memory
        mgr._templates.clear()
        
        # Load only 3 tiles
        mgr.get('tile5', load_from_cache=True)
        mgr.get('tile10', load_from_cache=True)
        mgr.get('tile15', load_from_cache=True)
        
        # Only 3 should be in memory
        assert len(mgr._templates) == 3
        
        # But all 20 should be available via cache
        cache_files = list(Path(temp_cache_dir).glob("template_*.json"))
        assert len(cache_files) == num_tiles
    
    def test_cache_enables_selective_loading(self, sample_dataarray, temp_cache_dir):
        """Test that cache allows loading only needed tiles."""
        mgr1 = TemplateManager(cache_dir=temp_cache_dir)
        
        # Create templates for tiles 0-9
        for i in range(10):
            mgr1.set(sample_dataarray, spatial_key=f'tile{i}')
        
        # New manager - simulate restart or different process
        mgr2 = TemplateManager(cache_dir=temp_cache_dir)
        
        # Load only even tiles
        for i in range(0, 10, 2):
            mgr2.get(f'tile{i}', load_from_cache=True)
        
        # Should have 5 tiles in memory
        assert len(mgr2._templates) == 5
        assert all(f'tile{i}' in mgr2._templates for i in range(0, 10, 2))


class TestTemplateManagerIntegrationScenarios:
    """Test real-world usage scenarios."""
    
    def test_viirs_fapar_286_tiles_scenario(self, sample_dataarray, temp_cache_dir):
        """
        Test scenario with 286 tiles (VIIRS FAPAR dataset).
        
        With caching: only active tiles in memory
        Without caching: all 286 templates in memory
        """
        mgr = TemplateManager(cache_dir=temp_cache_dir)
        
        # Simulate 286 tiles (use 50 for test speed)
        num_tiles = 50
        
        # First pass: create all templates
        for i in range(num_tiles):
            mgr.set(sample_dataarray, spatial_key=f'h{i:02d}v{i:02d}')
        
        # Simulate workflow restart
        mgr._templates.clear()
        
        # Second pass: process only 5 tiles
        active_tiles = ['h00v00', 'h10v10', 'h20v20', 'h30v30', 'h40v40']
        for tile in active_tiles:
            template = mgr.get(tile, load_from_cache=True)
            assert template is not None
        
        # Memory contains only 5 tiles, not all 50
        assert len(mgr._templates) == len(active_tiles)
        
        # But all 50 are available on disk
        cache_files = list(Path(temp_cache_dir).glob("template_*.json"))
        assert len(cache_files) == num_tiles
