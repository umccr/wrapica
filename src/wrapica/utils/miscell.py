#!/usr/bin/env python3

"""
Functions that don't quite do anywhere else
"""
# Standard imports
import re
from typing import Dict, Any, Type, Union
from urllib.parse import urlparse
from uuid import UUID

from pydantic import UUID4


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


def is_uuid_format(
        uuid_obj_or_str: Union[UUID4, str]
) -> bool:
    # First check if it's already a UUID4 object
    if isinstance(uuid_obj_or_str, UUID):
        return True
    # Next check if it's a valid UUID4 string
    try:
        _ = UUID(uuid_obj_or_str, version=4)
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


def is_str_type_representation(value: str, type_: Type) -> bool:
    """
    Check if the string 'value' can be represented as the type 'type'

    :param value:
    :param type_:
    :return:
    """

    try:
        type_(value)
        return True
    except ValueError:
        return False


def nextflow_parameter_to_str(parameter: Any) -> str:
    """
    Coerce the parameter to a string representation of the type

    This is easy for ints + floats, for Boolean types we make sure the string is lowercase

    :param parameter:
    :return:
    """
    if type(parameter) == bool:
        return str(parameter).lower()
    return str(parameter)


def coerce_to_uuid4_obj(
        uuid_str: Union[UUID4, str]
) -> UUID4:
    """
    Coerce a string to a UUID4 object

    :param uuid_str:
    :return:
    """
    if isinstance(uuid_str, UUID):
        return uuid_str
    elif isinstance(uuid_str, str):
        return UUID4(uuid_str)
    raise TypeError(
        f"Expected input type to be str or UUID4, got {type(uuid_str)}"
    )
