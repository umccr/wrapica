#!/usr/bin/env python
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
