"""
Tests for config/parsers.py

Tests the centralized configuration parsing logic.
"""
import pytest
from unittest import mock
import os
import types
import sys

from d3tools.config.parsers import (
    dataset_from_config,
    _manager_from_config,
    workflow_section_from_config,
)
from d3tools.data import Dataset,LocalDataset
from d3tools.thumbnails import DatasetThumbnailManager
from d3tools.logging import DatasetLogManager
from d3tools.timestepping import Day, Dekad
from d3tools.errors import WorkflowEngineImportError


class TestDatasetFromConfig:
    """Test dataset_from_config function."""
    
    def test_basic_config_parsing(self):
        """Test parsing basic dataset config without managers."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif'
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset, LocalDataset)
        assert dataset.dir == '/data'
        assert dataset.file == 'output.tif'
    
    def test_merges_with_defaults(self):
        """Test that defaults are properly merged."""
        config = {
            'file': 'output.tif'
        }
        defaults = {
            'type': 'local',
            'path': '/default/path'
        }
        
        dataset = dataset_from_config(config, defaults)
        
        assert dataset.dir == '/default/path'
        assert dataset.file == 'output.tif'
    
    def test_config_overrides_defaults(self):
        """Test that config values override defaults."""
        config = {
            'type': 'local',
            'path': '/custom/path',
            'file': 'output.tif'
        }
        defaults = {
            'type': 'local',
            'path': '/default/path'
        }
        
        dataset = dataset_from_config(config, defaults)
        
        assert dataset.dir == '/custom/path'
    
    def test_parses_thumbnail_dict(self):
        """Test parsing thumbnail config dict into manager."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parses_log_string(self):
        """Test parsing log config string into manager."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config)
        
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)
    
    def test_parses_both_managers(self):
        """Test parsing both thumbnail and log managers."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            },
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
        assert isinstance(dataset.log, DatasetLogManager)
    
    def test_accepts_already_parsed_managers(self):
        """Test that already-parsed managers are passed through."""
        thumb = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/logs', file='output.txt')
        )
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': thumb,
            'log': log
        }
        
        dataset = dataset_from_config(config)
        
        # Should be the same objects
        assert dataset.thumbnail is thumb
        assert dataset.log is log
    
    def test_handles_none_managers(self):
        """Test that None managers are handled correctly."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': None,
            'log': None
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.thumbnail is None
        assert dataset.log is None
    
    def test_invalid_thumbnail_returns_none(self):
        """Test that invalid thumbnail config returns None."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'invalid': 'config'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.thumbnail is None
    
    def test_empty_log_returns_none(self):
        """Test that empty log config returns None."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': {}
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.log is None
    
    def test_preserves_other_options(self):
        """Test that other dataset options are preserved."""
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'timestep': 't'
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.timestep == Dekad 

class TestParseManagerConfig:
    """Test _parse_manager_config helper function."""
    
    def test_parse_thumbnail_dict(self):
        """Test parsing thumbnail config dict."""
        config = {
            'colors': '/path/colors.json',
            'destination': '/path/thumb.png'
        }
        
        def mock_factory(path):
            return LocalDataset(path=os.path.dirname(path), file=os.path.basename(path))
        
        manager = _manager_from_config(config, 'thumbnail', mock_factory)
        
        assert isinstance(manager, DatasetThumbnailManager)
    
    def test_parse_log_string(self):
        """Test parsing log config string."""
        config = '/logs/output.txt'
        
        def mock_factory(path):
            return LocalDataset(path=os.path.dirname(path), file=os.path.basename(path))
        
        manager = _manager_from_config(config, 'log', mock_factory)
        
        assert isinstance(manager, DatasetLogManager)
    
    def test_returns_already_parsed_manager(self):
        """Test that already-parsed managers are returned as-is."""
        existing_manager = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        
        manager = _manager_from_config(existing_manager, 'thumbnail', lambda x: x)
        
        assert manager is existing_manager
    
    def test_returns_none_for_none(self):
        """Test that None returns None."""
        manager = _manager_from_config(None, 'thumbnail', lambda x: x)
        
        assert manager is None
    
    def test_raises_for_invalid_type(self):
        """Test that invalid manager type raises error."""
        with pytest.raises(ValueError, match="Unknown manager type"):
            _manager_from_config({}, 'invalid_type', lambda x: x)


class TestParserIntegration:
    """Test integration with Dataset.from_options()."""
    
    def test_dataset_from_options_uses_parser(self):
        """Test that Dataset.from_options() uses the parser."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should have parsed thumbnail
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parser_is_reusable(self):
        """Test that parser can be used directly, separate from Dataset."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': '/logs/output.txt'
        }
        
        # Use parser directly
        dataset = dataset_from_config(config)
        
        # Should return a fully constructed Dataset
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)


class TestParserForExternalUse:
    """Test that parser is suitable for use in door/dryes/dam."""
    
    def test_parser_can_be_imported_separately(self):
        """Test that parser can be imported and used independently."""
        # This simulates how door/dryes/dam would use it
        from d3tools.config.parsers import dataset_from_config
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/colors.json',
                'destination': '/thumbs/output.png'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parser_with_custom_defaults(self):
        """Test parser with custom defaults (as door/dryes/dam might use)."""
        # door/dryes/dam might have their own default configurations
        custom_defaults = {
            'type': 's3',
            'bucket_name': 'my-bucket',
            'timestep': 'day'
        }
        
        config = {
            'key_pattern': 'data/output.tif',
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config, custom_defaults)
        
        assert isinstance(dataset, Dataset)  # Will be RemoteDataset subclass
        assert dataset.bucket_name == 'my-bucket'
        assert dataset.timestep == Day
        assert dataset.key_pattern == 'data/output.tif'
        assert isinstance(dataset.log, DatasetLogManager)


class TestWorkflowSectionFromConfig:
    """Test workflow_section_from_config dispatch and error handling."""

    def test_build_false_returns_raw_payload(self):
        """Ensure parse passthrough mode returns the section payload unchanged."""
        section_options = {"source": "ERA5"}
        parsed = workflow_section_from_config("door", section_options, build_object=False)
        assert parsed is section_options

    def test_unknown_engine_raises(self):
        """Ensure unsupported engine keywords fail with a clear error."""
        with pytest.raises(ValueError, match="Unknown workflow section engine"):
            workflow_section_from_config("unknown", {}, build_object=True)

    def test_door_calls_downloader_from_options(self, monkeypatch):
        """Ensure door sections are delegated to Downloader.from_options with the same payload."""
        downloader_builder = mock.Mock(return_value={"built": "door"})
        fake_door = types.SimpleNamespace(
            Downloader=types.SimpleNamespace(from_options=downloader_builder)
        )
        monkeypatch.setitem(sys.modules, "door", fake_door)

        section_options = {"source": "ERA5"}
        result = workflow_section_from_config("door", section_options, build_object=True)

        downloader_builder.assert_called_once_with(section_options)
        assert result == {"built": "door"}

    def test_dam_calls_workflow_from_options(self, monkeypatch):
        """Ensure dam sections are delegated to DAMWorkflow.from_options with the same payload."""
        workflow_builder = mock.Mock(return_value={"built": "dam"})
        fake_dam = types.SimpleNamespace(
            DAMWorkflow=types.SimpleNamespace(from_options=workflow_builder)
        )
        monkeypatch.setitem(sys.modules, "dam", fake_dam)

        section_options = {"input": "x"}
        result = workflow_section_from_config("dam", section_options, build_object=True)

        workflow_builder.assert_called_once_with(section_options)
        assert result == {"built": "dam"}

    def test_dryes_calls_index_from_options_with_kwargs(self, monkeypatch):
        """Ensure dryes sections are delegated as keyword arguments to DRYESIndex.from_options."""
        index_builder = mock.Mock(return_value={"built": "dryes"})
        fake_dryes = types.SimpleNamespace(
            DRYESIndex=types.SimpleNamespace(from_options=index_builder)
        )
        monkeypatch.setitem(sys.modules, "dryes", fake_dryes)

        section_options = {
            "index_options": {"index_name": "spi"},
            "io_options": {"data": "x"},
            "run_options": {},
        }
        result = workflow_section_from_config("dryes", section_options, build_object=True)

        index_builder.assert_called_once_with(**section_options)
        assert result == {"built": "dryes"}

    def test_dryes_rejects_non_mapping_payload(self):
        """Ensure dryes section parsing rejects non-dict payloads before dispatch."""
        with pytest.raises(TypeError, match="DRYES section options must be a mapping"):
            workflow_section_from_config("dryes", "bad", build_object=True)

    def test_import_error_non_strict_falls_back_to_raw_payload(self, monkeypatch):
        """Ensure non-strict mode falls back to raw options when engine import fails."""
        from d3tools.config import parsers

        monkeypatch.setitem(
            parsers._WORKFLOW_ENGINE_BUILDERS,
            "door",
            mock.Mock(side_effect=ModuleNotFoundError("door")),
        )
        section_options = {"source": "ERA5"}

        parsed = workflow_section_from_config(
            "door",
            section_options,
            build_object=True,
            strict_imports=False,
        )

        assert parsed is section_options

    def test_import_error_strict_raises_workflow_engine_import_error(self, monkeypatch):
        """Ensure strict mode wraps import failures in WorkflowEngineImportError."""
        from d3tools.config import parsers

        monkeypatch.setitem(
            parsers._WORKFLOW_ENGINE_BUILDERS,
            "door",
            mock.Mock(side_effect=ModuleNotFoundError("door")),
        )

        with pytest.raises(WorkflowEngineImportError) as exc_info:
            workflow_section_from_config(
                "door",
                {"source": "ERA5"},
                build_object=True,
                strict_imports=True,
            )

        assert exc_info.value.engine == "door"
        assert isinstance(exc_info.value.original_error, ImportError)

    def test_non_import_errors_are_not_silenced(self, monkeypatch):
        """Ensure runtime validation errors from builders are propagated unchanged."""
        from d3tools.config import parsers

        monkeypatch.setitem(
            parsers._WORKFLOW_ENGINE_BUILDERS,
            "door",
            mock.Mock(side_effect=ValueError("invalid section payload")),
        )

        with pytest.raises(ValueError, match="invalid section payload"):
            workflow_section_from_config("door", {"source": "ERA5"}, build_object=True)
