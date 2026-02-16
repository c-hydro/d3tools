"""
Comprehensive tests for PlainTextMixin functionality.

Tests cover:
- Dataset instantiation with .txt files
- Read operations (local, memory)
- Write operations (create, append, overwrite)
- Metadata handling
- Edge cases (empty files, special characters, large files)
"""
import pytest

from d3tools.data import LocalDataset, MemoryDataset


class TestPlainTextInstantiation:
    """Test that .txt files correctly instantiate with PlainTextMixin."""
    
    def test_local_dataset_txt_instantiation(self, tmp_path):
        """Test LocalDataset with .txt file gets PlainTextMixin."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Hello World\n")
        
        dataset = LocalDataset(path=str(tmp_path), file="test.txt")
        
        # Check that the dataset has PlainTextMixin in its class hierarchy
        assert 'PlainTextMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'txt'
    
    def test_memory_dataset_txt_instantiation(self):
        """Test MemoryDataset with .txt extension gets PlainTextMixin."""
        dataset = MemoryDataset(key_pattern="data.txt")
        
        assert 'PlainTextMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'txt'
    
    def test_local_dataset_with_key_pattern(self, tmp_path):
        """Test LocalDataset with .txt in key_pattern."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Test content\n")
        
        dataset = LocalDataset(
            path=str(tmp_path),
            key_pattern="test.txt"
        )
        
        assert 'PlainTextMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'txt'


class TestPlainTextRead:
    """Test reading plain text files."""
    
    def test_read_simple_text(self, tmp_path):
        """Test reading a simple text file."""
        txt_file = tmp_path / "test.txt"
        content = "Line 1\nLine 2\nLine 3\n"
        txt_file.write_text(content)
        
        dataset = LocalDataset(path=str(tmp_path), file="test.txt")
        data = dataset.get_data()
        
        assert data == ["Line 1\n", "Line 2\n", "Line 3\n"]
    
    def test_read_empty_file(self, tmp_path):
        """Test reading an empty text file."""
        txt_file = tmp_path / "empty.txt"
        txt_file.write_text("")
        
        dataset = LocalDataset(path=str(tmp_path), file="empty.txt")
        data = dataset.get_data()
        
        assert data == []
    
    def test_read_single_line_no_newline(self, tmp_path):
        """Test reading a file with single line without trailing newline."""
        txt_file = tmp_path / "single.txt"
        txt_file.write_text("Single line without newline")
        
        dataset = LocalDataset(path=str(tmp_path), file="single.txt")
        data = dataset.get_data()
        
        assert data == ["Single line without newline"]
    
    def test_read_unicode_content(self, tmp_path):
        """Test reading text with unicode characters."""
        txt_file = tmp_path / "unicode.txt"
        content = "Hello 世界\nBonjour 🌍\nΓειά σου κόσμε\n"
        txt_file.write_text(content, encoding='utf-8')
        
        dataset = LocalDataset(path=str(tmp_path), file="unicode.txt")
        data = dataset.get_data()
        
        assert data == ["Hello 世界\n", "Bonjour 🌍\n", "Γειά σου κόσμε\n"]
    
    def test_read_with_special_characters(self, tmp_path):
        """Test reading text with special characters."""
        txt_file = tmp_path / "special.txt"
        content = "Tab\tseparated\nQuotes \"test\"\nBackslash \\\n"
        txt_file.write_text(content)
        
        dataset = LocalDataset(path=str(tmp_path), file="special.txt")
        data = dataset.get_data()
        
        assert data == ["Tab\tseparated\n", "Quotes \"test\"\n", "Backslash \\\n"]
    
    def test_read_nonexistent_file(self, tmp_path):
        """Test reading a file that doesn't exist raises appropriate error."""
        dataset = LocalDataset(path=str(tmp_path), file="nonexistent.txt")
        
        with pytest.raises(FileNotFoundError):
            dataset.get_data()

class TestPlainTextWrite:
    """Test writing plain text files."""
    
    def test_write_simple_text(self, tmp_path):
        """Test writing simple text data."""
        dataset = LocalDataset(path=str(tmp_path), file="output.txt")
        
        data = "Line 1\nLine 2\nLine 3\n"
        dataset.write_data(data)
        
        # Verify file was written correctly
        txt_file = tmp_path / "output.txt"
        content = txt_file.read_text()
        assert content == "Line 1\nLine 2\nLine 3\n"
    
    def test_write_overwrite(self, tmp_path):
        """Test that write overwrites existing content by default."""
        txt_file = tmp_path / "overwrite.txt"
        txt_file.write_text("Old content\n")
        
        dataset = LocalDataset(path=str(tmp_path), file="overwrite.txt")
        dataset.write_data("New content\n")
        
        content = txt_file.read_text()
        assert content == "New content\n"
        assert "Old content" not in content
    
    def test_write_append_mode(self, tmp_path):
        """Test writing in append mode."""
        txt_file = tmp_path / "append.txt"
        txt_file.write_text("First line\n")
        
        dataset = LocalDataset(path=str(tmp_path), file="append.txt")
        dataset.write_data("Second line\n", append=True)
        
        content = txt_file.read_text()
        assert content == "First line\nSecond line\n"
    
    def test_write_creates_directory(self, tmp_path):
        """Test that write creates parent directories if needed."""
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="file.txt")
        
        dataset.write_data("Test content\n")
        
        nested_file = nested_path / "file.txt"
        assert nested_file.exists()
        assert nested_file.read_text() == "Test content\n"
    
    def test_write_empty_list(self, tmp_path):
        """Test writing an empty list creates an empty file."""
        dataset = LocalDataset(path=str(tmp_path), file="empty.txt")
        
        dataset.write_data("")
        
        txt_file = tmp_path / "empty.txt"
        assert txt_file.exists()
        assert txt_file.read_text() == ""
    
    def test_write_unicode(self, tmp_path):
        """Test writing unicode content."""
        dataset = LocalDataset(path=str(tmp_path), file="unicode.txt")
        
        data = ["Hello 世界\n", "Bonjour 🌍\n"]
        dataset.write_data("".join(data))
        
        txt_file = tmp_path / "unicode.txt"
        content = txt_file.read_text(encoding='utf-8')
        assert content == "Hello 世界\nBonjour 🌍\n"


class TestPlainTextMemoryDataset:
    """Test PlainTextMixin with MemoryDataset."""
    
    def test_memory_write_and_read(self):
        """Test writing and reading from MemoryDataset."""
        dataset = MemoryDataset(key_pattern="memory.txt")
        
        data = "Line 1\n Line 2\n"
        dataset.write_data(data)
        
        read_data = dataset.get_data()
        assert read_data == data
    
    def test_memory_overwrite(self):
        """Test overwrite behavior in MemoryDataset."""
        dataset = MemoryDataset(key_pattern="memory.txt")
        
        dataset.write_data("Old\n")
        dataset.write_data("New\n")
        
        read_data = dataset.get_data()
        assert read_data == "New\n"

    def test_memory_append_txt_not_working(self):
        """Test append mode in MemoryDataset (TXT).
        This shouldn't work since MemoryDataset doesn't support append mode,
        but we want to ensure it doesn't break and ignores the append flag."""
        dataset = MemoryDataset(key_pattern="memory.txt")
        
        dataset.write_data("Old\n")
        dataset.write_data("New\n", append=True)  # This should not actually append but overwrite
        
        read_data = dataset.get_data()
        assert read_data == "New\n"

class TestPlainTextRoundTrip:
    """Test round-trip operations (write then read)."""
    
    def test_roundtrip_preserves_content(self, tmp_path):
        dataset = LocalDataset(path=str(tmp_path), file="roundtrip.txt")
        
        original_data = [
            "Line with spaces   \n",
            "\tIndented line\n",
            "Line with \"quotes\" and 'apostrophes'\n",
            "Special chars: @#$%^&*()\n",
            "\n",  # Empty line
            "Final line"
        ]
        
        dataset.write_data("".join(original_data))
        read_data = dataset.get_data()
        
        assert read_data == original_data
    
    def test_roundtrip_large_file(self, tmp_path):
        """Test round-trip with a larger file."""
        dataset = LocalDataset(path=str(tmp_path), file="large.txt")
        
        # Create 10,000 lines
        original_data = [f"Line {i}\n" for i in range(10000)]
        
        dataset.write_data("".join(original_data))
        read_data = dataset.get_data()
        
        assert len(read_data) == 10000
        assert read_data == original_data

class TestPlainTextEdgeCases:
    """Test edge cases and error handling."""
    
    def test_very_long_lines(self, tmp_path):
        """Test handling very long lines."""
     
        dataset = LocalDataset(path=str(tmp_path), file="long.txt")
        
        long_line = "x" * 100000 + "\n"
        data = [long_line, "short\n"]
        
        dataset.write_data("".join(data))
        read_data = dataset.get_data()
        
        assert read_data[0] == long_line
        assert len(read_data[0]) == 100001
    
    def test_mixed_line_endings(self, tmp_path):
        """Test handling mixed line endings."""
        txt_file = tmp_path / "mixed.txt"
        # Write with mixed line endings manually
        txt_file.write_bytes(b"Unix\nWindows\r\nMac\rEnd")
        
        dataset = LocalDataset(path=str(tmp_path), file="mixed.txt")
        data = dataset.get_data()
        
        # Python's universal newline mode should handle this
        assert len(data) > 0
    
    def test_binary_content_raises_error(self, tmp_path):
        """Test that binary content in .txt raises appropriate error."""
        txt_file = tmp_path / "binary.txt"
        txt_file.write_bytes(b'\x00\x01\x02\x03\xFF\xFE')
        
        dataset = LocalDataset(path=str(tmp_path), file="binary.txt")
        
        with pytest.raises(UnicodeDecodeError):
            dataset.get_data()
    
    def test_write_non_string_data(self, tmp_path):
        """Test behavior when writing non-string data."""
        dataset = LocalDataset(path=str(tmp_path), file="string.txt")
        with pytest.raises(TypeError):
            dataset.write_data(12345)

class TestPlainTextFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties doesn't break initialization."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("test")
        
        dataset = LocalDataset(key_pattern=str(txt_file))
        
        # Should initialize without errors
        assert dataset is not None
        assert hasattr(dataset, 'format')
    
    def test_format_after_read_passthrough(self, tmp_path):
        """Test _format_after_read is a passthrough for plain text."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Line 1\nLine 2\n")
        
        dataset = LocalDataset(key_pattern=str(txt_file))
        raw_data = ["Line 1\n", "Line 2\n"]
        
        # _format_after_read should return data unchanged
        processed = dataset._format_after_read(raw_data)
        assert processed == raw_data
    
    def test_format_before_write_passthrough(self, tmp_path):
        """Test _format_before_write is a passthrough for plain text."""
        dataset = LocalDataset(path=str(tmp_path), file="test.txt")
        data = "Line 1\nLine 2\n"
        # _format_before_write should return data unchanged
        prepared = dataset._format_before_write(
            data, 
            time=None, 
            time_format="", 
            metadata={}
        )
        assert prepared == data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
