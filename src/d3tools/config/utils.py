import json

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