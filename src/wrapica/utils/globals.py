#!/usr/bin/env python

"""
List of globals to use for icav2 cli plugins
"""
import re
from ..enums import AnalysisStorageSize

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

ICAV2_DEFAULT_ANALYSIS_STORAGE_SIZE = AnalysisStorageSize.SMALL

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
    "high": "standard-xlarge",
    "high_memory": "himem-large"
}

NEXTFLOW_PROCESS_LABEL_RE_OBJ = re.compile(r"withLabel:process_(\w+) \{")

# NEXTFLOW_VERSION = "24.04.2"
NEXTFLOW_VERSION = "22.04.3"
NEXTFLOW_VERSION_UUID = "b1585d18-f88c-4ca0-8d47-34f6c01eb6f3"
