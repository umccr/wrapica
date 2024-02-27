#!/usr/bin/env python
import re
from pathlib import Path
from typing import Any, Union, Dict, List


def recursively_build_open_api_body_from_libica_item(libica_item: Any) -> Union[Dict, Any]:
    if not isinstance(libica_item, object) or isinstance(libica_item, str):
        return libica_item
    open_api_body_dict = {}
    for key, value in libica_item._data_store.items():
        if isinstance(value, List):
            output_value = [
                recursively_build_open_api_body_from_libica_item(value_item)
                for value_item in value
            ]
        elif isinstance(value, object) and hasattr(value, "_data_store"):
            output_value = recursively_build_open_api_body_from_libica_item(value)
        else:
            output_value = value
        open_api_body_dict[libica_item.attribute_map.get(key)] = output_value
    return open_api_body_dict


def fill_placeholder_path(
    output_path: Path,
    placeholder_dict: Dict
) -> Path:
    """
    Fill the analysisOutput or folder path with a placeholder.

    Supported
    :param output_path:
    :param placeholder_dict:
    :return:
    """
    for key_regex, replacement_value in placeholder_dict.items():
        if not re.match(key_regex, str(output_path)):
            continue
        output_path = Path(re.sub(key_regex, replacement_value, str(output_path)))

    return output_path


