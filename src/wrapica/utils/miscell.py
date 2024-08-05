#!/usr/bin/env python3

"""
Functions that don't quite do anywhere else
"""
# Standard imports
import re
from typing import Dict, Any, Union, List, Type
from urllib.parse import urlparse
from uuid import UUID


def camel_to_snake_case(camel_case: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def snake_to_camel_case(snake_case: str) -> str:
    return ''.join(word.title() for word in snake_case.split('_'))


def to_lower_camel_case(snake_str):
    # We capitalize the first letter of each component except the first one
    # with the 'capitalize' method and join them together.
    camel_string = snake_to_camel_case(snake_str)
    return snake_str[0].lower() + camel_string[1:]


def sanitise_dict_keys(input_dict: Dict) -> Dict:
    output_dict = {}
    key: str
    value: Any
    for key, value in input_dict.items():
        output_dict[camel_to_snake_case(key)] = value
    return output_dict


def is_uuid_format(project_id: str) -> bool:
    try:
        _ = UUID(project_id, version=4)
        return True
    except ValueError:
        return False


def is_uri_format(uri_str: str) -> bool:
    """
    Determine if the string is a valid URI
    :param uri_str:
    :return:
    """
    try:
        url_obj = urlparse(uri_str)
        if url_obj.scheme and url_obj.netloc:
            return True
        else:
            return False
    except ValueError:
        return False


def build_curl_body_from_libica_item(libica_item: Any) -> Union[Dict, Any]:
    """
    Useful for pipeline retention, i.e what was actually submitted to the orchestration engine
    :param libica_item:
    :return:
    """
    if not isinstance(libica_item, object) or isinstance(libica_item, str):
        return libica_item
    open_api_body_dict = {}
    for key, value in libica_item._data_store.items():
        if isinstance(value, List):
            output_value = [
                build_curl_body_from_libica_item(value_item)
                for value_item in value
            ]
        elif isinstance(value, object) and hasattr(value, "_data_store"):
            output_value = build_curl_body_from_libica_item(value)
        else:
            output_value = value
        open_api_body_dict[libica_item.attribute_map.get(key)] = output_value
    return open_api_body_dict


def is_str_type_representation(value: str, type: Type) -> bool:
    """
    Check if the string 'value' can be represented as the type 'type'

    :param value:
    :param type:
    :return:
    """

    try:
        type(value)
        return True
    except ValueError:
        return False


def nextflow_parameter_to_str(parameter: Any) -> str:
    """
    Coerce the parameter to a string representation of the type

    This is easy for ints + floats, for Boolean types we make sure the string is lowercase

    :param parameter:
    :param type:
    :return:
    """
    if type(parameter) == bool:
        return str(parameter).lower()
    return str(parameter)
