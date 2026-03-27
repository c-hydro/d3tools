"""
Tests for Dataset update/copy behavior contracts.

These tests lock down current behavior used by downstream workflows
while update/copy internals are refactored.
"""

from d3tools.data import LocalDataset, MemoryDataset


class TestDatasetUpdateCopyContract:
    """Contract tests for Dataset.update() / Dataset.copy()."""

    def test_update_non_inplace_returns_new_and_keeps_source_unchanged(self):
        """Test update() creates a new dataset and does not mutate the source."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='field_{var}.tif',
            name='ds_{region}_{var}',
        )

        updated = ds.update(region='it', var='tp')

        assert updated is not ds
        assert ds.key_pattern == '/data/{region}/field_{var}.tif'
        assert ds.name == 'ds_{region}_{var}'
        assert ds.tags == {}

        assert updated.key_pattern == '/data/it/field_tp.tif'
        assert updated.name == 'ds_it_tp'
        assert updated.tags.get('region') == 'it'
        assert updated.tags.get('var') == 'tp'

    def test_update_in_place_mutates_source(self):
        """Test update(in_place=True) mutates and returns the same dataset."""
        ds = LocalDataset(path='/data/{region}', filename='field.tif')

        out = ds.update(in_place=True, region='it')

        assert out is ds
        assert ds.key_pattern == '/data/it/field.tif'
        assert ds.tags.get('region') == 'it'

    def test_copy_keeps_shared_manager_contract_and_independent_tags(self):
        """Test copy() preserves shared manager behavior and clone-local tags."""
        ds = LocalDataset(path='/data', filename='field.tif')
        ds.template_manager._templates['__tile__'] = {'dummy': 1}
        ds.log = object()
        ds.thumbnail = object()

        c1 = ds.copy()
        c2 = ds.copy()

        assert c1.template_manager is ds.template_manager
        assert c2.template_manager is ds.template_manager
        assert c1.log is ds.log
        assert c2.log is ds.log
        assert c1.thumbnail is ds.thumbnail
        assert c2.thumbnail is ds.thumbnail

        c1.tags['window'] = 'w1'
        assert 'window' not in c2.tags
        assert 'window' not in ds.tags

    def test_memory_update_preserves_keep_after_reading_flag(self):
        """Test MemoryDataset.update() keeps keep_after_reading behavior."""
        ds = MemoryDataset(key_pattern='memory_{tile}.txt', keep_after_reading=True)
        ds.write_data('a', tile='t1')
        ds.write_data('b', tile='t2')

        updated = ds.update(tile='t1')

        assert isinstance(updated, MemoryDataset)
        assert updated.keep_after_reading is True
        assert updated.check_data() is True
        assert updated.get_data() == 'a'

    def test_dryes_style_tagged_variants_are_independent(self):
        """Test repeated update(**tags) creates independent tagged variants."""
        data_ds = LocalDataset(path='/data/{Ttype}', filename='temp.tif')

        min_ds = data_ds.update(Ttype='min')
        max_ds = data_ds.update(Ttype='max')

        assert data_ds.tags == {}
        assert min_ds.key_pattern == '/data/min/temp.tif'
        assert max_ds.key_pattern == '/data/max/temp.tif'
        assert min_ds.tags.get('Ttype') == 'min'
        assert max_ds.tags.get('Ttype') == 'max'

        min_ds.tags['extra'] = 'x'
        assert 'extra' not in max_ds.tags
        assert 'extra' not in data_ds.tags
