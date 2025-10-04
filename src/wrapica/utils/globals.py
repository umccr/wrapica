#!/usr/bin/env python

"""
List of globals to use for icav2 cli plugins
"""
import re
from ..literals import AnalysisStorageSizeType, UriType,  DataType

DEFAULT_ICAV2_BASE_URL = "https://ica.illumina.com/ica/rest"

ICAV2_CONFIG_FILE_SERVER_URL_KEY = "server-url"
ICAV2_SESSION_FILE_ACCESS_TOKEN_KEY = "access-token"
ICAV2_SESSION_FILE_PROJECT_ID_KEY = "project-id"

ICAV2_CONFIG_FILE_PATH = "{HOME}/.icav2/config.yaml"
ICAV2_SESSION_FILE_PATH = "{HOME}/.icav2/.session.{server_url_prefix}.yaml"

ICAV2_ACCESS_TOKEN_AUDIENCE = "ica"

LIBICAV2_DEFAULT_PAGE_SIZE = 1000

ICAV2_MAX_STEP_CHARACTERS = 23

ICAV2_CLI_PLUGINS_HOME_ENV_VAR = "ICAV2_CLI_PLUGINS_HOME"

ICAV2_DEFAULT_ANALYSIS_STORAGE_SIZE: AnalysisStorageSizeType = "Small"

PARAMS_XML_FILE_NAME = "params.xml"

BLANK_PARAMS_XML_V2_FILE_CONTENTS = [
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
    '<pd:pipeline xmlns:pd="xsd://www.illumina.com/ica/cp/pipelinedefinition" code="" version="1.0">',
    '    <pd:dataInputs/>',
    '    <pd:steps/>',
    '</pd:pipeline>'
]

GITHUB_RELEASE_DESCRIPTION_REGEX_MATCH = re.compile(
    r"GitHub\sRelease\sURL:\s(.*)"
)

GITHUB_RELEASE_REPO_TAG_REGEX_MATCH = re.compile(
    r"/(.*)/releases/tag/(.*)"
)

# Is the string a REGEX STRING?
# See 'matching characters' in https://docs.python.org/3/howto/regex.html
IS_REGEX_MATCH = re.compile('.*[%s].*' % re.escape(r'.^$*+?{}[]\|()'))

NEXTFLOW_TASK_POD_MAPPING = {
    "single": "standard-small",
    "low": "standard-medium",
    "medium": "standard-large",
    "high": "himem-medium",
    "high_memory": "himem-large"
}

NEXTFLOW_TASK_POD_MAPPING_RETRY = {
    "single": "standard-medium",
    "low": "standard-large",
    "medium": "standard-xlarge",
    "high": "himem-large",
    "high_memory": "himem-large"
}

NEXTFLOW_PROCESS_LABEL_RE_OBJ = re.compile(r"withLabel:process_(\w+) \{")

DEFAULT_NEXTFLOW_VERSION = "24.10.2"

# Other literals

# URI Types
ICAV2_URI_SCHEME: UriType = "icav2"
S3_URI_SCHEME: UriType = "s3"

# Data Types
FILE_DATA_TYPE: DataType = "FILE"
FOLDER_DATA_TYPE: DataType = "FOLDER"

# URI REGEX OBJ
URI_REGEX_OBJ = re.compile(rf'(?:{ICAV2_URI_SCHEME}|{S3_URI_SCHEME})://[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+')
