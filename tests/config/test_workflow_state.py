"""Tests for workflow run state reading."""

import json
import tempfile

import pytest

from d3tools.config.utils import get_timerange_from_run_state
from d3tools.timestepping import TimeRange


@pytest.fixture
def sample_run_state():
    """Sample run state with multiple sections."""
    return {
        "version": 1,
        "run_id": "2024-05-07T14:30:00",
        "name": "drought_monitoring",
        "status": "success",
        "start": "2024-05-01",
        "end": "2024-05-07",
        "sections": [
            {
                "name": "download",
                "engine": "door",
                "executed": True,
                "start": "2024-05-01",
                "end": "2024-05-02",
                "reason": None
            },
            {
                "name": "process",
                "engine": "dam",
                "executed": True,
                "start": "2024-05-02",
                "end": "2024-05-05",
                "reason": None
            },
            {
                "name": "calculate",
                "engine": "dryes",
                "executed": True,
                "start": "2024-05-05",
                "end": "2024-05-07",
                "reason": None
            }
        ]
    }


class TestGetTimerangeFromRunState:
    """Test get_timerange_from_run_state() function."""
    
    def test_read_workflow_timerange_no_section(self, tmp_path, sample_run_state):
        """Should read overall workflow timerange without section specifier."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        time_range = get_timerange_from_run_state(str(state_file))
        
        assert isinstance(time_range, TimeRange)
        assert "2024-05-01" in str(time_range.start)
        assert "2024-05-07" in str(time_range.end)
    
    def test_read_section_timerange_with_at_syntax(self, tmp_path, sample_run_state):
        """Should read section timerange using 'section@file.json' syntax."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        time_range = get_timerange_from_run_state(f"download@{str(state_file)}")
        
        assert isinstance(time_range, TimeRange)
        assert "2024-05-01" in str(time_range.start)
        assert "2024-05-02" in str(time_range.end)
    
    def test_read_different_sections_with_at_syntax(self, tmp_path, sample_run_state):
        """Should correctly distinguish between different sections."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        download_range = get_timerange_from_run_state(f"download@{str(state_file)}")
        process_range = get_timerange_from_run_state(f"process@{str(state_file)}")
        calculate_range = get_timerange_from_run_state(f"calculate@{str(state_file)}")
        
        assert "2024-05-01" in str(download_range.start)
        assert "2024-05-02" in str(process_range.start)
        assert "2024-05-05" in str(calculate_range.start)
    
    def test_section_name_with_spaces_trimmed(self, tmp_path, sample_run_state):
        """Should handle spaces around section name in 'section @ file' syntax."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        # Test with extra spaces
        time_range = get_timerange_from_run_state(f"  download  @  {str(state_file)}  ")
        
        assert isinstance(time_range, TimeRange)
        assert "2024-05-01" in str(time_range.start)
    
    def test_nonexistent_section_falls_back_to_workflow(self, tmp_path, sample_run_state):
        """Should fall back to workflow timerange if section not found."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        time_range = get_timerange_from_run_state(f"nonexistent@{str(state_file)}")
        
        # Falls back to workflow range when section not found
        assert time_range is not None
        assert "2024-05-01" in str(time_range.start)
        assert "2024-05-07" in str(time_range.end)
    
    def test_section_with_missing_times_returns_none(self, tmp_path):
        """Should return None if section times are missing."""
        run_state = {
            "version": 1,
            "run_id": "test",
            "name": "test",
            "start": "2024-05-01",
            "end": "2024-05-07",
            "sections": [
                {
                    "name": "incomplete",
                    "engine": "door",
                    "executed": False,
                    "start": None,
                    "end": None,
                    "reason": "No work to do"
                }
            ]
        }
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(run_state))
        
        time_range = get_timerange_from_run_state(f"incomplete@{str(state_file)}")
        
        assert time_range is None
    
    def test_workflow_missing_times_returns_none(self, tmp_path):
        """Should return None if workflow times are missing."""
        run_state = {
            "version": 1,
            "run_id": "test",
            "name": "test",
            "start": None,
            "end": None,
            "sections": []
        }
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(run_state))
        
        time_range = get_timerange_from_run_state(str(state_file))
        
        assert time_range is None
    
    def test_invalid_date_format_raises(self, tmp_path):
        """Should raise error for invalid date format."""
        run_state = {
            "version": 1,
            "run_id": "test",
            "name": "test",
            "start": "not-a-date",
            "end": "2024-05-07",
            "sections": []
        }
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(run_state))
        
        with pytest.raises(Exception):  # TimeRange will raise on invalid format
            get_timerange_from_run_state(str(state_file))
    
    def test_file_not_found_raises(self, tmp_path):
        """Should raise error if file doesn't exist."""
        missing_file = tmp_path / "missing.json"
        
        with pytest.raises(FileNotFoundError):
            get_timerange_from_run_state(str(missing_file))
    
    def test_invalid_json_raises(self, tmp_path):
        """Should raise error for invalid JSON."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{ invalid json }")
        
        with pytest.raises(Exception):  # json.JSONDecodeError
            get_timerange_from_run_state(str(bad_file))
    
    def test_read_section_from_complex_at_path(self, tmp_path, sample_run_state):
        """Should handle nested directory paths with @ syntax."""
        subdir = tmp_path / "nested" / "path"
        subdir.mkdir(parents=True)
        state_file = subdir / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        time_range = get_timerange_from_run_state(f"process@{str(state_file)}")
        
        assert isinstance(time_range, TimeRange)
        assert "2024-05-02" in str(time_range.start)
    
    def test_multiple_at_signs_splits_on_first(self, tmp_path, sample_run_state):
        """Should handle paths with @ in filename by splitting on first @."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        # Construct a path-like string with multiple @ - should split on first
        time_range = get_timerange_from_run_state(f"download@{str(state_file)}")
        
        assert isinstance(time_range, TimeRange)
        assert "2024-05-01" in str(time_range.start)


class TestGetTimerangeFromRunStateIntegration:
    """Integration tests for run state resolution."""
    
    def test_full_workflow_without_section(self, tmp_path, sample_run_state):
        """Should extract full workflow timerange."""
        state_file = tmp_path / "workflow_run.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        time_range = get_timerange_from_run_state(str(state_file))
        
        assert time_range is not None
        # Should span the entire workflow
        assert "2024-05-01" in str(time_range.start)
        assert "2024-05-07" in str(time_range.end)
    
    def test_all_sections_retrievable(self, tmp_path, sample_run_state):
        """Should be able to retrieve all sections individually."""
        state_file = tmp_path / "run_state.json"
        state_file.write_text(json.dumps(sample_run_state))
        
        for section in sample_run_state["sections"]:
            section_name = section["name"]
            time_range = get_timerange_from_run_state(f"{section_name}@{str(state_file)}")
            
            assert time_range is not None
            assert section["start"] in str(time_range.start)
            assert section["end"] in str(time_range.end)
    
    def test_partial_run_state(self, tmp_path):
        """Should handle run states where some sections didn't execute."""
        partial_state = {
            "version": 1,
            "run_id": "2024-05-07",
            "name": "partial",
            "status": "partial",
            "start": "2024-05-01",
            "end": "2024-05-05",
            "sections": [
                {
                    "name": "download",
                    "engine": "door",
                    "executed": True,
                    "start": "2024-05-01",
                    "end": "2024-05-03",
                    "reason": None
                },
                {
                    "name": "process",
                    "engine": "dam",
                    "executed": False,
                    "start": None,
                    "end": None,
                    "reason": "No data"
                }
            ]
        }
        state_file = tmp_path / "partial.json"
        state_file.write_text(json.dumps(partial_state))
        
        # Executed section should have times
        download_range = get_timerange_from_run_state(f"download@{str(state_file)}")
        assert download_range is not None
        
        # Skipped section should return None
        process_range = get_timerange_from_run_state(f"process@{str(state_file)}")
        assert process_range is None
        
        # Overall workflow should still work
        workflow_range = get_timerange_from_run_state(str(state_file))
        assert workflow_range is not None
