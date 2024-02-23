#!/usr/bin/env python3

"""
Put miscellaneous utils here
"""

import re
from pathlib import Path
from typing import Dict


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


