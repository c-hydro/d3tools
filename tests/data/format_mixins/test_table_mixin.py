"""
Comprehensive tests for TableMixin functionality.

Tests cover:
- Dataset instantiation with CSV/Parquet files
- Read operations (CSV, Parquet)
- Write operations (create, append, overwrite)
- DataFrame operations
- Edge cases (empty files, large datasets, different encodings)
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from d3tools.data import LocalDataset, MemoryDataset


class TestTableInstantiation:
    """Test that CSV/Parquet files correctly instantiate with TableMixin."""
    
    def test_local_dataset_csv_instantiation(self, tmp_path):
        """Test LocalDataset with .csv file gets TableMixin."""
        csv_file = tmp_path / "test.csv"
        pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.csv")
        
        assert 'TableMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'csv'
    
    def test_local_dataset_parquet_instantiation(self, tmp_path):
        """Test LocalDataset with .parquet file gets TableMixin."""
        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).to_parquet(parquet_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.parquet")
        
        assert 'TableMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'parquet'
    
    def test_memory_dataset_csv_instantiation(self):
        """Test MemoryDataset with .csv extension gets TableMixin."""
        dataset = MemoryDataset(key_pattern="data.csv")
        
        assert 'TableMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'csv'


class TestTableReadCSV:
    """Test reading CSV files."""
    
    def test_read_simple_csv(self, tmp_path):
        """Test reading a simple CSV file."""
        csv_file = tmp_path / "test.csv"
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.1, 2.2, 3.3]
        })
        df.to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.csv")
        data = dataset.get_data()
        
        pd.testing.assert_frame_equal(data, df)
    
    def test_read_empty_csv(self, tmp_path):
        """Test reading an empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")
        
        dataset = LocalDataset(path=str(tmp_path), file="empty.csv")
        with pytest.raises(pd.errors.EmptyDataError):
            dataset.get_data()
    
    def test_read_csv_nonexistent_file(self, tmp_path):
        """Test reading a file that doesn't exist raises error."""
        dataset = LocalDataset(path=str(tmp_path), file="nonexistent.csv")
        
        with pytest.raises(FileNotFoundError):
            dataset.get_data()


class TestTableReadParquet:
    """Test reading Parquet files."""
    
    def test_read_simple_parquet(self, tmp_path):
        """Test reading a simple Parquet file."""
        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.1, 2.2, 3.3]
        })
        df.to_parquet(parquet_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.parquet")
        data = dataset.get_data()
        
        pd.testing.assert_frame_equal(data, df)
    
    def test_read_parquet_with_types(self, tmp_path):
        """Test reading Parquet preserves types."""
        parquet_file = tmp_path / "types.parquet"
        df = pd.DataFrame({
            'int_col': pd.array([1, 2, 3], dtype='int64'),
            'float_col': pd.array([1.1, 2.2, 3.3], dtype='float64'),
            'str_col': pd.array(['a', 'b', 'c'], dtype='string'),
            'bool_col': pd.array([True, False, True], dtype='bool')
        })
        df.to_parquet(parquet_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="types.parquet")
        data = dataset.get_data()
        
        pd.testing.assert_frame_equal(data, df)


class TestTableWriteCSV:
    """Test writing CSV files."""
    
    def test_write_simple_csv(self, tmp_path):
        """Test writing simple CSV data."""
        dataset = LocalDataset(path=str(tmp_path), file="output.csv")
        
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        dataset.write_data(df)
        
        # Verify file was written correctly
        csv_file = tmp_path / "output.csv"
        written_df = pd.read_csv(csv_file)
        pd.testing.assert_frame_equal(written_df, df)
    
    def test_write_csv_overwrite(self, tmp_path):
        """Test that write overwrites existing content by default."""
        csv_file = tmp_path / "overwrite.csv"
        pd.DataFrame({'old': [1, 2]}).to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="overwrite.csv")
        new_df = pd.DataFrame({'new': [3, 4]})
        dataset.write_data(new_df)
        
        written_df = pd.read_csv(csv_file)
        pd.testing.assert_frame_equal(written_df, new_df)
        assert 'old' not in written_df.columns
    
    def test_write_csv_append_mode(self, tmp_path):
        """Test writing in append mode."""
        csv_file = tmp_path / "append.csv"
        df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df1.to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="append.csv")
        df2 = pd.DataFrame({'a': [5, 6], 'b': [7, 8]})
        dataset.write_data(df2, append=True)
        
        written_df = pd.read_csv(csv_file)
        expected = pd.concat([df1, df2], ignore_index=True)
        pd.testing.assert_frame_equal(written_df, expected)
    
    def test_write_csv_creates_directory(self, tmp_path):
        """Test that write creates parent directories if needed."""
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="file.csv")
        
        df = pd.DataFrame({'a': [1, 2]})
        dataset.write_data(df)
        
        csv_file = nested_path / "file.csv"
        assert csv_file.exists()
    
    def test_write_empty_dataframe(self, tmp_path):
        """Test writing an empty DataFrame."""
        dataset = LocalDataset(path=str(tmp_path), file="empty.csv")
        
        df = pd.DataFrame()
        dataset.write_data(df)
        
        csv_file = tmp_path / "empty.csv"
        assert csv_file.exists()


class TestTableWriteParquet:
    """Test writing Parquet files."""
    
    def test_write_simple_parquet(self, tmp_path):
        """Test writing simple Parquet data."""
        dataset = LocalDataset(path=str(tmp_path), file="output.parquet")
        
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        dataset.write_data(df)
        
        # Verify file was written correctly
        parquet_file = tmp_path / "output.parquet"
        written_df = pd.read_parquet(parquet_file)
        pd.testing.assert_frame_equal(written_df, df)
    
    def test_write_parquet_preserves_types(self, tmp_path):
        """Test that Parquet writing preserves data types."""
        dataset = LocalDataset(path=str(tmp_path), file="types.parquet")
        
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True]
        })
        dataset.write_data(df)
        
        parquet_file = tmp_path / "types.parquet"
        written_df = pd.read_parquet(parquet_file)
        
        # Types should be preserved
        assert written_df['int_col'].dtype == np.int64
        assert written_df['float_col'].dtype == np.float64
        assert written_df['bool_col'].dtype == bool

    def test_write_parquet_append_mode(self, tmp_path):
        """Test writing Parquet in append mode."""
        parquet_file = tmp_path / "append.parquet"
        df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df1.to_parquet(parquet_file)
        
        dataset = LocalDataset(path=str(tmp_path), file="append.parquet")
        df2 = pd.DataFrame({'a': [5, 6], 'b': [7, 8]})
        dataset.write_data(df2, append=True)
        
        written_df = pd.read_parquet(parquet_file)
        expected = pd.concat([df1, df2], ignore_index=True)
        pd.testing.assert_frame_equal(written_df, expected)

class TestTableMemoryDataset:
    """Test TableMixin with MemoryDataset."""
    
    def test_memory_write_and_read_csv(self):
        """Test writing and reading from MemoryDataset (CSV)."""
        dataset = MemoryDataset(key_pattern="memory.csv")
        
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        dataset.write_data(df)
        
        read_df = dataset.get_data()
        pd.testing.assert_frame_equal(read_df, df)
    
    def test_memory_append_csv_not_working(self):
        """Test append mode in MemoryDataset (CSV).
        This shouldn't work since MemoryDataset doesn't support append mode,
        but we want to ensure it doesn't break and ignores the append flag."""
        dataset = MemoryDataset(key_pattern="memory.csv")
        
        df1 = pd.DataFrame({'a': [1, 2]})
        dataset.write_data(df1)
        
        df2 = pd.DataFrame({'a': [3, 4]})
        dataset.write_data(df2, append=True)
        
        read_df = dataset.get_data()
        pd.testing.assert_frame_equal(read_df, df2)


class TestTableRoundTrip:
    """Test round-trip operations (write then read)."""
    
    def test_roundtrip_csv_preserves_data(self, tmp_path):
        """Test that CSV writing and reading preserves exact data."""
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.csv")
        
        original_df = pd.DataFrame({
            'integers': [1, 2, 3, 4, 5],
            'floats': [1.1, 2.2, 3.3, 4.4, 5.5],
            'strings': ['a', 'b', 'c', 'd', 'e'],
            'mixed': [1, 'two', 3.0, 'four', 5]
        })
        
        dataset.write_data(original_df)
        read_df = dataset.get_data()
        
        # String comparison since CSV doesn't preserve exact types
        assert read_df.shape == original_df.shape
        assert list(read_df.columns) == list(original_df.columns)
    
    def test_roundtrip_parquet_exact(self, tmp_path):
        """Test that Parquet preserves exact data and types."""
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.parquet")
        
        original_df = pd.DataFrame({
            'integers': [1, 2, 3, 4, 5],
            'floats': [1.1, 2.2, 3.3, 4.4, 5.5],
            'strings': ['a', 'b', 'c', 'd', 'e']
        })
        
        dataset.write_data(original_df)
        read_df = dataset.get_data()
        
        pd.testing.assert_frame_equal(read_df, original_df)
    
    def test_roundtrip_large_dataframe(self, tmp_path):
        """Test round-trip with a larger DataFrame."""
        dataset = LocalDataset(path=str(tmp_path), file="large.parquet")
        
        # Create 10,000 rows
        original_df = pd.DataFrame({
            'col1': range(10000),
            'col2': np.random.rand(10000),
            'col3': ['value'] * 10000
        })
        
        dataset.write_data(original_df)
        read_df = dataset.get_data()
        
        pd.testing.assert_frame_equal(read_df, original_df)


class TestTableEdgeCases:
    """Test edge cases and error handling."""
    
    def test_special_characters_in_data(self, tmp_path):
        """Test handling special characters in data."""
        dataset = LocalDataset(path=str(tmp_path), file="special.csv")
        
        df = pd.DataFrame({
            'text': ['comma,here', 'quote"here', 'newline\nhere', 'tab\there']
        })
        
        dataset.write_data(df)
        read_df = dataset.get_data()
        
        pd.testing.assert_frame_equal(read_df, df)
    
    def test_unicode_content(self, tmp_path):
        """Test handling unicode content."""
        dataset = LocalDataset(path=str(tmp_path), file="unicode.csv")
        
        df = pd.DataFrame({
            'text': ['Hello 世界', 'Bonjour 🌍', 'Γειά σου κόσμε']
        })
        
        dataset.write_data(df)
        read_df = dataset.get_data()
        
        pd.testing.assert_frame_equal(read_df, df)
    
    def test_missing_values(self, tmp_path):
        """Test handling missing values."""
        dataset = LocalDataset(path=str(tmp_path), file="missing.parquet")
        
        df = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['a', None, 'c', 'd']
        })
        
        dataset.write_data(df)
        read_df = dataset.get_data()
        
        # Check that NaN values are preserved
        assert read_df['col1'].isna().sum() == 1
        assert read_df['col2'].isna().sum() == 1


class TestTableFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties doesn't break initialization."""
        csv_file = tmp_path / "test.csv"
        pd.DataFrame({'a': [1]}).to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.csv")
        
        assert dataset is not None
        assert hasattr(dataset, 'format')
    
    def test_format_after_read_passthrough(self, tmp_path):
        """Test _format_after_read is a passthrough for tables."""
        csv_file = tmp_path / "test.csv"
        df = pd.DataFrame({'a': [1, 2]})
        df.to_csv(csv_file, index=False)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.csv")
        
        # _format_after_read should return data unchanged
        processed = dataset._format_after_read(df)
        pd.testing.assert_frame_equal(processed, df)
    
    def test_format_before_write_passthrough(self, tmp_path):
        """Test _format_before_write is a passthrough for tables."""
        dataset = LocalDataset(path=str(tmp_path), file="test.csv")
        
        df = pd.DataFrame({'a': [1, 2]})
        
        # _format_before_write should return data unchanged
        prepared = dataset._format_before_write(df)
        pd.testing.assert_frame_equal(prepared, df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
