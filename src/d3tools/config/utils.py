import json
from typing import Optional, Any

from .parsers import parse_times_from_run_option

from ..timestepping import TimeRange

def merge_dicts(*dicts) -> dict:
    """Recursively merge dictionaries, with later values overwriting earlier ones.

    Args:
        *dicts: Dictionaries to merge in order. Nested mapping values are merged
            recursively; non-mapping values from later dictionaries replace the
            earlier value at the same key.

    Returns:
        A new dictionary containing the merged configuration.
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
    """Load one or more JSON files and merge them into a single config dict.

    Args:
        *json_objects: Paths to JSON files. Each file is read in order and
            merged using :func:`merge_dicts`.

    Returns:
        A single dictionary containing the merged JSON content.

    Raises:
        ValueError: If any input path does not end in ``.json``.
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
    """Read a saved workflow or section time range from a run-state dataset.

    Args:
        times_from_run_option: Either a string such as ``"section@file.json"`` or
            ``"file.json"``, or a dict containing ``file`` and optional
            ``section`` keys.

    Returns:
        The matching ``TimeRange`` from the persisted run state, or ``None``
        when the state has no usable start/end values.

    Example::
        get_timerange_from_run_state("download@prior_state.json")
        get_timerange_from_run_state({"file": dataset_obj, "section": "download"})
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
