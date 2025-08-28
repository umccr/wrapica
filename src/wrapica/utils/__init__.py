#!/usr/bin/env python
import re
from pathlib import Path
from typing import Dict, Tuple
from urllib.parse import urlparse


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


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Return the bucket and key / prefix from an S3 URI.
    :param s3_uri:
    :return:
    """
    url_obj = urlparse(s3_uri)

    return url_obj.netloc, url_obj.path.lstrip('/')
