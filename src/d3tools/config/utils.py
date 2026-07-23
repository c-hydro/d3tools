import json
from typing import Optional

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
    run_state_file: str,
) -> Optional[TimeRange]:
    
    """
    Read a section or workflow time range from a persisted run-state file.
    
    Args:
        run_state_file: JSON run-state reference.
            Supports either:
            - "section_name@section_run_state.json"
            - "section_run_state.json"
        
    Returns:
        TimeRange for the section, or None if not found or times missing
        
    Example:
        time_range = get_timerange_from_run_state(
            "download@prior_workflow_state.json"
        )
    """

    if "@" in run_state_file:
        section_name, run_file = run_state_file.split("@", 1)
        section_name = section_name.strip()
        run_file = run_file.strip()
    else:
        section_name = None
        run_file = run_state_file.strip()
    
    json_data = load_jsons(run_file)
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
