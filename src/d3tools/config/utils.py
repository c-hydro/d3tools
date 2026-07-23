import json
from typing import Optional, Any

from .parsers import parse_times_from_run_option

from ..timestepping import TimeRange

def merge_dicts(*dicts) -> dict:
    """
    Merge any number of dictionaries recursively.
    """
    merged_dict = {}

    for d in dicts:
        for key, value in d.items():
            if isinstance(value, dict) and key in merged_dict:
                merged_dict[key] = merge_dicts(merged_dict[key], value)
            else:
                merged_dict[key] = value

    return merged_dict

def load_jsons(*json_objects) -> dict:
    """
    Merge any number of JSON objects recursively.
    """
    dicts = []

    for json_obj in json_objects:
        ext = json_obj.split('.')[-1]
        if ext != 'json':
            raise ValueError(f"Unsupported file format: {ext}. Config files should be in JSON format.")
        with open(json_obj, 'r') as file:
            dicts.append(json.load(file))

    merged_dict = merge_dicts(*dicts)
    return merged_dict

def get_timerange_from_run_state(
    times_from_run_option: Any
    ) -> Optional[TimeRange]:
    
    """
    Read a section or workflow time range from a persisted run-state Dataset.
    
    Args:
        times_from_run_option: Specification for the run-state file and optional section.
            Can be either:
            - A string: "section_name@file.json" or just "file.json"
            - A dictionary with "file" and optional "section" keys
            Both formats are parsed via parse_times_from_run_option().
        
    Returns:
        TimeRange for the specified section (if provided), or the workflow-level
        timerange (if no section specified). Returns None if times are missing.
        
    Example:
        time_range = get_timerange_from_run_state("download@prior_state.json")
        time_range = get_timerange_from_run_state({"file": dataset_obj, "section": "download"})
    """
    
    file, section_name = parse_times_from_run_option(times_from_run_option)
    json_data = file.get_data(as_is = True)
    start_str = json_data.get("start")
    end_str = json_data.get("end")

    if section_name:
        for section in json_data.get("sections", []):
            if section.get("name") == section_name:
                start_str = section.get("start")
                end_str = section.get("end")
                break
    
    if not start_str or not end_str:
        return None

    else:
        return TimeRange(start_str, end_str)
