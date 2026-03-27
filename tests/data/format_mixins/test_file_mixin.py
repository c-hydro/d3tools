"""
Comprehensive tests for FileMixin functionality.

Tests cover:
- Dataset instantiation with file-based formats (PNG, PDF)
- Read operations (returns file path)
- Write operations (moves/copies files)
- Edge cases
"""
import pytest
from pathlib import Path
import os

from d3tools.data import LocalDataset, MemoryDataset


class TestFileInstantiation:
    """Test that file formats correctly instantiate with FileMixin."""
    
    def test_local_dataset_png_instantiation(self, tmp_path):
        """Test LocalDataset with .png file gets FileMixin."""
        png_file = tmp_path / "test.png"
        png_file.write_bytes(b'\x89PNG\r\n\x1a\n')  # PNG header
        
        dataset = LocalDataset(path=str(tmp_path), file="test.png")
        
        assert 'FileMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'file'
    
    def test_local_dataset_pdf_instantiation(self, tmp_path):
        """Test LocalDataset with .pdf file gets FileMixin."""
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b'%PDF-1.4')  # PDF header
        
        dataset = LocalDataset(path=str(tmp_path), file="test.pdf")
        
        assert 'FileMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'file'
    
    def test_local_dataset_no_extension_instantiation(self, tmp_path):
        """Test LocalDataset with no extension gets FileMixin."""
        file = tmp_path / "testfile"
        file.write_text("content")
        
        dataset = LocalDataset(path=str(tmp_path), file="testfile")
        
        assert 'FileMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'file'
    
    def test_memory_dataset_png_instantiation(self):
        """Test MemoryDataset with .png extension gets FileMixin."""
        dataset = MemoryDataset(key_pattern="image.png")
        
        assert 'FileMixin' in [cls.__name__ for cls in dataset.__class__.__mro__]
        assert dataset.format == 'file'


class TestFileRead:
    """Test reading file-based formats."""
    
    def test_read_png_returns_path(self, tmp_path):
        """Test reading PNG file returns the file path."""
        png_file = tmp_path / "test.png"
        png_file.write_bytes(b'\x89PNG\r\n\x1a\n')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.png")
        result = dataset.get_data()
        
        # Read should return the path itself
        assert result == str(png_file)
    
    def test_read_pdf_returns_path(self, tmp_path):
        """Test reading PDF file returns the file path."""
        pdf_file = tmp_path / "document.pdf"
        pdf_file.write_bytes(b'%PDF-1.4')
        
        dataset = LocalDataset(path=str(tmp_path), file="document.pdf")
        result = dataset.get_data()
        
        assert result == str(pdf_file)
    
    def test_read_no_extension_returns_path(self, tmp_path):
        """Test reading file without extension returns the file path."""
        file = tmp_path / "somefile"
        file.write_text("binary data")
        
        dataset = LocalDataset(path=str(tmp_path), file="somefile")
        result = dataset.get_data()
        
        assert result == str(file)
    
    def test_read_nonexistent_file(self, tmp_path):
        """Test reading nonexistent file results in error."""
        dataset = LocalDataset(path=str(tmp_path), file="missing.png")
        
        # This should raise an error since the file doesn't exist
        with pytest.raises(FileNotFoundError):
            dataset.get_data()
            

class TestFileWrite:
    """Test writing file-based formats (file moving/copying)."""
    
    def test_write_moves_png_file(self, tmp_path):
        """Test writing PNG moves the file to destination."""
        source_file = tmp_path / "source.png"
        source_file.write_bytes(b'\x89PNG\r\n\x1a\n' + b'image data')
        
        dest_path = tmp_path / "output"
        dataset = LocalDataset(path=str(dest_path), file="dest.png")
        
        dataset.write_data(str(source_file))
        
        dest_file = dest_path / "dest.png"
        assert dest_file.exists()
        assert dest_file.read_bytes() == b'\x89PNG\r\n\x1a\n' + b'image data'
        # Source should be moved (renamed), not copied
        assert not source_file.exists()
    
    def test_write_moves_pdf_file(self, tmp_path):
        """Test writing PDF moves the file to destination."""
        source_file = tmp_path / "source.pdf"
        source_file.write_bytes(b'%PDF-1.4\nPDF content')
        
        dest_path = tmp_path / "output"
        dataset = LocalDataset(path=str(dest_path), file="dest.pdf")
        
        dataset.write_data(str(source_file))
        
        dest_file = dest_path / "dest.pdf"
        assert dest_file.exists()
        assert dest_file.read_bytes() == b'%PDF-1.4\nPDF content'
        assert not source_file.exists()
    
    def test_write_creates_directory(self, tmp_path):
        """Test that write creates parent directories."""
        source_file = tmp_path / "source.png"
        source_file.write_bytes(b'image')
        
        nested_path = tmp_path / "subdir" / "nested"
        dataset = LocalDataset(path=str(nested_path), file="image.png")
        
        dataset.write_data(str(source_file))
        
        dest_file = nested_path / "image.png"
        assert dest_file.exists()
    
    def test_write_overwrites_existing_file(self, tmp_path):
        """Test that write overwrites existing file."""
        source_file = tmp_path / "new.png"
        source_file.write_bytes(b'new content')
        
        dest_file = tmp_path / "output" / "dest.png"
        dest_file.parent.mkdir(parents=True)
        dest_file.write_bytes(b'old content')
        
        dataset = LocalDataset(path=str(tmp_path / "output"), file="dest.png")
        dataset.write_data(str(source_file))
        
        assert dest_file.read_bytes() == b'new content'
    
    def test_write_file_without_extension(self, tmp_path):
        """Test writing file without extension."""
        source_file = tmp_path / "source"
        source_file.write_bytes(b'binary data')
        
        dataset = LocalDataset(path=str(tmp_path / "output"), file="dest")
        dataset.write_data(str(source_file))
        
        dest_file = tmp_path / "output" / "dest"
        assert dest_file.exists()
        assert dest_file.read_bytes() == b'binary data'


class TestFileMemoryDataset:
    """Test FileMixin with MemoryDataset."""
    
    def test_memory_write_and_read_png(self, tmp_path):
        """Test writing and reading from MemoryDataset (PNG)."""
        # Create a temporary source file
        source_file = tmp_path / "source.png"
        source_file.write_bytes(b'PNG data')
        
        dataset = MemoryDataset(key_pattern="memory.png")
        
        # Write stores path in memory
        dataset.write_data(str(source_file))
        
        # Read returns the stored path
        result = dataset.get_data()
        assert result == str(source_file)


class TestFileRoundTrip:
    """Test round-trip operations."""
    
    def test_roundtrip_read_write_png(self, tmp_path):
        """Test reading a PNG and writing to new location."""
        original_file = tmp_path / "original.png"
        original_file.write_bytes(b'\x89PNG\r\n\x1a\n' + b'original image data')
        
        # Read from original location
        dataset1 = LocalDataset(path=str(tmp_path), file="original.png")
        path = dataset1.get_data()
        
        # Write to new location
        dest_path = tmp_path / "copies"
        dataset2 = LocalDataset(path=str(dest_path), file="copy.png")
        dataset2.write_data(path)
        
        # Verify copy was created
        copy_file = dest_path / "copy.png"
        assert copy_file.exists()
        # Note: original is moved, not copied
        assert not original_file.exists()
    
    def test_roundtrip_preserves_binary_content(self, tmp_path):
        """Test that binary content is preserved exactly."""
        # Create file with specific binary content
        original = tmp_path / "binary.pdf"
        binary_content = bytes([i % 256 for i in range(1000)])
        original.write_bytes(binary_content)
        
        dataset = LocalDataset(path=str(tmp_path), file="binary.pdf")
        path = dataset.get_data()
        
        # Move to new location
        new_location = tmp_path / "moved"
        dataset2 = LocalDataset(path=str(new_location), file="binary.pdf")
        dataset2.write_data(path)
        
        # Verify binary content matches
        moved_file = new_location / "binary.pdf"
        assert moved_file.read_bytes() == binary_content


class TestFileEdgeCases:
    """Test edge cases and error handling."""
    
    def test_large_binary_file(self, tmp_path):
        """Test handling large binary files."""
        # Create 10MB file
        large_file = tmp_path / "large.png"
        large_content = b'x' * (10 * 1024 * 1024)
        large_file.write_bytes(large_content)
        
        dataset = LocalDataset(path=str(tmp_path), file="large.png")
        path = dataset.get_data()
        
        # Move it
        dest_path = tmp_path / "moved"
        dataset2 = LocalDataset(path=str(dest_path), file="large.png")
        dataset2.write_data(path)
        
        moved_file = dest_path / "large.png"
        assert moved_file.exists()
        assert moved_file.stat().st_size == 10 * 1024 * 1024
    
    def test_special_characters_in_filename(self, tmp_path):
        """Test handling filenames with special characters."""
        source = tmp_path / "file with spaces.png"
        source.write_bytes(b'data')
        
        dataset = LocalDataset(path=str(tmp_path / "dest"), file="output file.png")
        dataset.write_data(str(source))
        
        dest = tmp_path / "dest" / "output file.png"
        assert dest.exists()
    
    def test_write_same_filename_different_directory(self, tmp_path):
        """Test moving file to different directory with same name."""
        source = tmp_path / "src" / "file.png"
        source.parent.mkdir()
        source.write_bytes(b'content')
        
        dataset = LocalDataset(path=str(tmp_path / "dst"), file="file.png")
        dataset.write_data(str(source))
        
        dest = tmp_path / "dst" / "file.png"
        assert dest.exists()
        assert not source.exists()


class TestFileFormatMethods:
    """Test format-specific methods directly."""
    
    def test_init_format_properties(self, tmp_path):
        """Test _init_format_properties doesn't break initialization."""
        png_file = tmp_path / "test.png"
        png_file.write_bytes(b'PNG')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.png")
        
        assert dataset is not None
        assert hasattr(dataset, 'format')
    
    def test_format_after_read_passthrough(self, tmp_path):
        """Test _format_after_read is a passthrough for files."""
        png_file = tmp_path / "test.png"
        png_file.write_bytes(b'PNG')
        
        dataset = LocalDataset(path=str(tmp_path), file="test.png")
        
        # _format_after_read should return data unchanged
        test_path = "/some/path"
        processed = dataset._format_after_read(test_path)
        assert processed == test_path
    
    def test_format_before_write_passthrough(self, tmp_path):
        """Test _format_before_write is a passthrough for files."""
        dataset = LocalDataset(path=str(tmp_path), file="test.png")
        
        test_path = "/some/path"
        
        # _format_before_write should return data unchanged
        prepared = dataset._format_before_write(test_path)
        assert prepared == test_path


class TestFileSupportedFormats:
    """Test all supported file formats."""
    
    def test_png_format_detected(self, tmp_path):
        """Test .png extension is detected as 'file' format."""
        dataset = LocalDataset(path=str(tmp_path), file="image.png")
        assert dataset.format == 'file'
    
    def test_pdf_format_detected(self, tmp_path):
        """Test .pdf extension is detected as 'file' format."""
        dataset = LocalDataset(path=str(tmp_path), file="document.pdf")
        assert dataset.format == 'file'
    
    def test_no_extension_format_detected(self, tmp_path):
        """Test no extension is detected as 'file' format."""
        dataset = LocalDataset(path=str(tmp_path), file="README")
        assert dataset.format == 'file'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
