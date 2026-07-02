#!/usr/bin/env python3

"""
Project Pipeline Helpers
"""

# Standard imports
import typing
from io import StringIO
from typing import Tuple, Dict, Optional, Union, List, cast, Any
from urllib.parse import urlparse
import uuid
from pathlib import Path
from zipfile import ZipFile
import boto3
import pandas as pd
import re
from botocore.exceptions import ClientError
from cwl_utils.parser import load_document_by_uri
from tempfile import NamedTemporaryFile, TemporaryDirectory
from binaryornot.check import is_binary

# Libica imports
from libica.openapi.v3 import (
    ApiClient,
    ApiException,
    ProjectAnalysisStorageApi,
    ProjectPipelineV4,
    PipelineConfigurationParameter,
    PipelineResources,
    AnalysisS3DataDetails,
    CwlAnalysisWithJsonInput,
)
from libica.openapi.v3.api.project_pipeline_api import ProjectPipelineApi
from libica.openapi.v3.api.project_analysis_api import ProjectAnalysisApi

# V3 imports
from libica.openapi.v3.models import (
    AnalysisInputDataMount,
    AnalysisInputExternalData,
    AnalysisV4,
    InputParameter,
    InputParameterList,
    PipelineConfigurationParameterList,
    PipelineFile,
    Project,
    ProjectData,
    ProjectPipeline,
    CreateCwlWithJsonInputAnalysis,
    CreateNextflowWithCustomInputAnalysis,
    AnalysisStorageV3,
    AnalysisStorageV4,
)
from pydantic import UUID4

# Local imports
from ...utils import parse_s3_uri
from ...utils.logger import get_logger
from ...utils.configuration import get_icav2_configuration
from ...utils.cwl_typing_helpers import WorkflowInputParameterType, WorkflowType
from ...utils.globals import (
    BLANK_PARAMS_XML_V2_FILE_CONTENTS,
    FOLDER_DATA_TYPE,
    ICAV2_URI_SCHEME,
    S3_URI_SCHEME,
    URI_REGEX_OBJ,
    ICAV2_CONFIG_NEXTFLOW_PATH
)
from ...literals import DataType, PipelineStatusType, AnalysisStorageSizeType, ResourceType
from ...utils.miscell import is_uuid_format, is_uri_format, coerce_to_uuid4_obj
from ...utils.nextflow_helpers import (
    get_default_nextflow_pipeline_version_id,
    include_icav2_config_into_nextflow_config,
    get_default_icav2_config_content,
)
from .. import (
    CES_DATA_ABS_PATH,
    SAMPLESHEET_WITH_PLACEHOLDERS_NAME,
    SAMPLESHEET_WITH_ABS_PATHS_NAME,
    SAMPLESHEET_DIR_NAME,
)

if typing.TYPE_CHECKING:
    # Import type hints for IDE only, not at runtime
    # Prevents circular imports
    from .. import (
        ICAv2PipelineAnalysisTags,
    )
    from ..classes.cwl_analysis import ICAv2CWLPipelineAnalysis
    from mypy_boto3_s3 import S3Client


# Literals
DEFAULT_ANALYSIS_STORAGE_SIZE: AnalysisStorageSizeType = "Small"


# Types
AnalysisStorageType = Union[AnalysisStorageV3, AnalysisStorageV4]
PipelineType = Union[ProjectPipeline, ProjectPipelineV4]


# Logging
logger = get_logger()


def get_project_pipeline_obj(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
) -> ProjectPipelineV4:
    """
    Return the project pipeline object for a given project and pipeline ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve from the project

    :return: The project pipeline object matching the given IDs
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises ValueError: If the pipeline cannot be found in the project

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_obj

        project_pipeline_obj = get_project_pipeline_obj(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(f"Pipeline ID: {project_pipeline_obj.pipeline.id}, Code: {project_pipeline_obj.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my-pipeline
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        # Force the API client to send back the v4 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        api_instance = ProjectPipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a pipeline.
        api_response: ProjectPipelineV4 = api_instance.get_project_pipeline(
            project_id=str(project_id),
            pipeline_id=str(pipeline_id)
        )
    except ApiException as e:
        raise ValueError("Exception when calling ProjectPipelineApi->get_project_pipeline: %s\n" % e)

    return api_response


def get_project_pipeline_obj_from_pipeline_code(
        project_id: Union[UUID4, str],
        pipeline_code: str
) -> ProjectPipeline:
    """
    Return the project pipeline object matching a given pipeline code.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The pipeline code string to look up in the project

    :return: The project pipeline object matching the given code
    :rtype: `ProjectPipeline <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipeline/>`_

    :raises ValueError: If the pipeline code cannot be found in the project

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_obj_from_pipeline_code

        project_pipeline_obj = get_project_pipeline_obj_from_pipeline_code(
            project_id="project-123",
            pipeline_code="my_pipeline_code"
        )

        print(f"Pipeline ID: {project_pipeline_obj.pipeline.id}, Code: {project_pipeline_obj.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_pipeline_code
    """
    try:
        return next(
            filter(
                lambda project_pipeline_iter: project_pipeline_iter.pipeline.code == pipeline_code,
                list_project_pipelines(project_id)
            )
        )
    except StopIteration:
        logger.error(f"Could not find pipeline '{pipeline_code}' in project {project_id}")
        raise ValueError


def get_project_pipeline_id_from_pipeline_code(
        project_id: Union[UUID4, str],
        pipeline_code: str
) -> str:
    """
    Return the pipeline ID for a given pipeline code within a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The pipeline code string to look up

    :return: The pipeline ID as a string
    :rtype: str

    :raises ValueError: If the pipeline code cannot be found in the project

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_id_from_pipeline_code

        pipeline_id = get_project_pipeline_id_from_pipeline_code(
            project_id="project-123",
            pipeline_code="my_pipeline_code"
        )

        print(pipeline_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """
    return str(get_project_pipeline_obj_from_pipeline_code(project_id, pipeline_code).pipeline.id)


def get_default_analysis_storage_obj_from_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> AnalysisStorageType:
    """
    Return the default analysis storage object for a given project pipeline.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve storage configuration from

    :return: The default analysis storage object for the pipeline
    :rtype: `AnalysisStorageV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStorageV4/>`_

    :raises ValueError: If the pipeline cannot be found in the project
    :raises ApiException: If the API call to retrieve the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_default_analysis_storage_obj_from_project_pipeline

        analysis_storage_obj = get_default_analysis_storage_obj_from_project_pipeline(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(f"Storage ID: {analysis_storage_obj.id}")
        # Storage ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """

    # Get the project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Return the analysis storage
    return project_pipeline_obj.pipeline.analysis_storage


def get_default_analysis_storage_id_from_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> str:
    """
    Return the default analysis storage ID for a given project pipeline.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve storage information from

    :return: The analysis storage ID as a string
    :rtype: str

    :raises ValueError: If the pipeline cannot be found in the project
    :raises ApiException: If the API call to retrieve the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_default_analysis_storage_id_from_project_pipeline

        analysis_storage_id = get_default_analysis_storage_id_from_project_pipeline(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(analysis_storage_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """

    # Get the project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Return the analysis storage id
    return str(project_pipeline_obj.pipeline.analysis_storage.id)


def get_project_pipeline_description_from_pipeline_id(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> str:
    """
    Return the description of a project pipeline given its project and pipeline IDs.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve the description from

    :return: The pipeline description text string
    :rtype: str

    :raises ValueError: If the pipeline cannot be found in the project
    :raises ApiException: If the API call to retrieve the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_description_from_pipeline_id

        pipeline_description = get_project_pipeline_description_from_pipeline_id(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(pipeline_description)
        # My CWL workflow for alignment
    """
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    return project_pipeline_obj.pipeline.description


def coerce_pipeline_id_or_code_to_project_pipeline_obj(pipeline_id_or_code: str) -> PipelineType:
    """
    Coerce a pipeline ID or code string to a project pipeline object.

    :param pipeline_id_or_code: The pipeline identifier as a UUID string or pipeline code

    :return: The resolved project pipeline object
    :rtype: `ProjectPipeline <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipeline/>`_

    :raises ValueError: If the pipeline cannot be found in the current project
    :raises ApiException: If the API call to retrieve the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import coerce_pipeline_id_or_code_to_project_pipeline_obj

        project_pipeline_obj = coerce_pipeline_id_or_code_to_project_pipeline_obj(
            pipeline_id_or_code="my_pipeline_code"
        )

        print(f"Pipeline ID: {project_pipeline_obj.pipeline.id}, Code: {project_pipeline_obj.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_pipeline_code
    """
    from ...project import get_project_id

    project_id = get_project_id()

    if is_uuid_format(pipeline_id_or_code):
        return get_project_pipeline_obj(project_id, pipeline_id_or_code)
    return get_project_pipeline_obj_from_pipeline_code(project_id, pipeline_id_or_code)


def get_analysis_storage_from_analysis_storage_id(
        project_id: Union[UUID4, str],
        analysis_storage_id: Union[UUID4, str]
) -> AnalysisStorageType:
    """
    Return the analysis storage object matching a given storage ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param analysis_storage_id: The analysis storage identifier to look up

    :return: The analysis storage object matching the given ID
    :rtype: `AnalysisStorageV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStorageV4/>`_

    :raises ValueError: If the analysis storage ID cannot be found
    :raises ApiException: If the API call to retrieve storage options fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_analysis_storage_from_analysis_storage_id

        analysis_storage_obj = get_analysis_storage_from_analysis_storage_id(
            project_id="project-123",
            analysis_storage_id="storage-456"
        )

        print(f"Storage ID: {analysis_storage_obj.id}")
        # Storage ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force the API client to send back the v4 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        # Create an instance of the API class
        api_instance = ProjectAnalysisStorageApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve the list of analysis storage options.
        api_response = api_instance.get_project_analysis_storage_options(
            project_id=str(project_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisStorageApi->get_project_analysis_storage_options: %s\n" % e)
        raise ApiException

    try:
        return next(
            filter(
                lambda storage_obj_iter: str(storage_obj_iter.id) == str(analysis_storage_id),
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find analysis storage id {analysis_storage_id}")
        raise ValueError


def get_analysis_storage_from_analysis_storage_size(
        project_id: Union[UUID4, str],
        analysis_storage_size: AnalysisStorageSizeType
) -> AnalysisStorageType:
    """
    Return the analysis storage object matching a given storage size name.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param analysis_storage_size: The analysis storage size name to look up

    :return: The analysis storage object matching the given size
    :rtype: `AnalysisStorageV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStorageV4/>`_

    :raises ValueError: If the analysis storage size cannot be found
    :raises ApiException: If the API call to retrieve storage options fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_analysis_storage_from_analysis_storage_size

        analysis_storage_obj = get_analysis_storage_from_analysis_storage_size(
            project_id="project-123",
            analysis_storage_size="Small"
        )

        print(f"Storage ID: {analysis_storage_obj.id}")
        # Storage ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force the API client to send back the v4 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        # Create an instance of the API class
        api_instance = ProjectAnalysisStorageApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve the list of analysis storage options.
        api_response = api_instance.get_project_analysis_storage_options(
            project_id=str(project_id)
        )
    except ApiException as e:
        logger.error("Exception when calling AnalysisStorageApi->get_analysis_storage_options: %s\n" % e)
        raise ApiException

    try:
        return next(
            filter(
                lambda storage_obj_iter: storage_obj_iter.name == analysis_storage_size,
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find analysis storage size {analysis_storage_size}")
        raise ValueError


def get_analysis_storage_id_from_analysis_storage_size(
        project_id: Union[UUID4, str],
        analysis_storage_size: AnalysisStorageSizeType
) -> str:
    """
    Return the analysis storage ID for a given storage size name.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param analysis_storage_size: The analysis storage size name to look up

    :return: The analysis storage ID as a string
    :rtype: str

    :raises ValueError: If the analysis storage size cannot be found
    :raises ApiException: If the API call to retrieve storage options fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_analysis_storage_id_from_analysis_storage_size

        analysis_storage_id = get_analysis_storage_id_from_analysis_storage_size(
            project_id="project-123",
            analysis_storage_size="Small"
        )

        print(analysis_storage_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """
    return str(get_analysis_storage_from_analysis_storage_size(project_id, analysis_storage_size).id)


def coerce_analysis_storage_id_or_size_to_analysis_storage(
        project_id: Union[UUID4, str],
        analysis_storage_id_or_size: Union[
            str, AnalysisStorageSizeType
        ]
) -> AnalysisStorageType:
    """
    Coerce an analysis storage ID or size name to an analysis storage object.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param analysis_storage_id_or_size: The analysis storage identifier as a UUID string
        or a size name such as "Small", "Medium", or "Large"

    :return: The resolved analysis storage object
    :rtype: `AnalysisStorageV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStorageV4/>`_

    :raises ValueError: If the analysis storage cannot be resolved
    :raises ApiException: If the API call to retrieve storage options fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import coerce_analysis_storage_id_or_size_to_analysis_storage

        analysis_storage = coerce_analysis_storage_id_or_size_to_analysis_storage(
            project_id="project-123",
            analysis_storage_id_or_size="Small"
        )

        print(f"Storage ID: {analysis_storage.id}")
        # Storage ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    if is_uuid_format(analysis_storage_id_or_size):
        return get_analysis_storage_from_analysis_storage_id(project_id, analysis_storage_id_or_size)
    return get_analysis_storage_from_analysis_storage_size(
        project_id,
        cast(AnalysisStorageSizeType, analysis_storage_id_or_size)
    )


def create_cwl_input_json_analysis_obj(
        user_reference: str,
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
        analysis_input_dict: Dict,
        analysis_storage_id: Optional[Union[UUID4, str]] = None,
        analysis_storage_size: Optional[AnalysisStorageSizeType] = None,
        # Output parameters
        analysis_output_uri: Optional[str] = None,
        # Meta parameters
        tags: Optional['ICAv2PipelineAnalysisTags'] = None,
        # CWL Specific parameters
        cwltool_overrides: Optional[Dict] = None
) -> 'ICAv2CWLPipelineAnalysis':
    """
    Create a CWL pipeline analysis object ready for launch from an input JSON dictionary.

    :param user_reference: The user reference string to label the analysis
    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to launch in the project
    :param analysis_input_dict: The CWL input JSON as a dictionary with location URIs
    :param analysis_storage_id: The analysis storage identifier to use. Defaults to None,
        in which case the default storage for the pipeline is used
    :param analysis_storage_size: The analysis storage size name to use. Defaults to None,
        in which case the default storage for the pipeline is used
    :param analysis_output_uri: The output URI for analysis results. Defaults to None,
        in which case the default output location is used
    :param tags: The pipeline analysis tags object for metadata. Defaults to None,
        in which case no tags are applied
    :param cwltool_overrides: The cwltool overrides dictionary for resource hints. Defaults to None,
        in which case no overrides are applied

    :return: The CWL pipeline analysis object ready for launch
    :rtype: ICAv2CWLPipelineAnalysis

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import create_cwl_input_json_analysis_obj
        cwl_analysis = create_cwl_input_json_analysis_obj(
            user_reference="my-analysis-run",
            project_id="project-123",
            pipeline_id="pipeline-456",
            analysis_input_dict={
                "input_file": {
                    "class": "File",
                    "location": "icav2://project-123/data/file.txt"
                }
            }
        )
        print(f"User Reference: {cwl_analysis.user_reference}")
        # User Reference: my-analysis-run
    """
    # Import classes locally to prevent circular imports
    from ..classes.cwl_analysis import ICAv2CWLPipelineAnalysis, ICAv2CwlAnalysisJsonInput

    # Generate inputs object
    cwl_input_obj = ICAv2CwlAnalysisJsonInput(
        input_json=analysis_input_dict
    )

    # Generate cwl analysis object
    cwl_analysis = ICAv2CWLPipelineAnalysis(
        user_reference=user_reference,
        project_id=project_id,
        pipeline_id=pipeline_id,
        # By calling the cwl input object, we return a deferenced CwlAnalysisJsonInput
        analysis_input=cast(CwlAnalysisWithJsonInput, cwl_input_obj()),
        analysis_storage_id=analysis_storage_id,
        analysis_storage_size=analysis_storage_size,
        analysis_output_uri=analysis_output_uri,
        tags=tags,
        cwltool_overrides=cwltool_overrides
    )

    return cwl_analysis


def launch_cwl_workflow(
        project_id: Union[UUID4, str],
        cwl_analysis: CreateCwlWithJsonInputAnalysis,
        idempotency_key=None
) -> AnalysisV4:
    """
    Launch a CWL workflow analysis in a specific project context.

    :param project_id: The project identifier to launch the workflow in
    :param cwl_analysis: The CWL analysis object containing inputs and configuration
    :param idempotency_key: The idempotency key header for preventing duplicate requests.
        Defaults to None, in which case no idempotency key is sent

    :return: The launched analysis object with its ID and status
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ApiException: If the API call to create the CWL analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import launch_cwl_workflow
        from wrapica.libica_models import CreateCwlWithJsonInputAnalysis

        analysis = launch_cwl_workflow(
            project_id="project-123",
            cwl_analysis=cwl_analysis_obj
        )

        print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: REQUESTED
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force default accept header to v4
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    # Collect kwargs
    analysis_kwargs = {
        "idempotency_key": idempotency_key
    }

    # Reduce analysis_kwargs to only those that are not None
    analysis_kwargs = {k: v for k, v in analysis_kwargs.items() if v is not None}

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a CWL pipeline.
        api_response: AnalysisV4 = api_instance.create_cwl_analysis_with_json_input(
            project_id=str(project_id),
            create_cwl_with_json_input_analysis=cwl_analysis,
            **analysis_kwargs
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->create_cwl_json_analysis: %s\n" % e)
        raise ApiException

    return api_response


def launch_nextflow_workflow(
        project_id: Union[UUID4, str],
        nextflow_analysis: CreateNextflowWithCustomInputAnalysis,
        idempotency_key=None
) -> AnalysisV4:
    """
    Launch a Nextflow workflow analysis in a specific project context.

    :param project_id: The project identifier to launch the workflow in
    :param nextflow_analysis: The Nextflow analysis object containing inputs and configuration
    :param idempotency_key: The idempotency key header for preventing duplicate requests.
        Defaults to None, in which case no idempotency key is sent

    :return: The launched analysis object with its ID and status
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ApiException: If the API call to create the Nextflow analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import launch_nextflow_workflow
        from wrapica.libica_models import CreateNextflowWithCustomInputAnalysis

        analysis = launch_nextflow_workflow(
            project_id="project-123",
            nextflow_analysis=nextflow_analysis_obj
        )

        print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: REQUESTED
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force default headers to v4
        api_client.set_default_header(
            header_name="Content-Type",
            header_value="application/vnd.illumina.v4+json"
        )
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    # Collect kwargs
    analysis_kwargs = {
        "idempotency_key": idempotency_key
    }

    # Reduce analysis_kwargs to only those that are not None
    analysis_kwargs = dict(filter(
        lambda kv: kv[1] is not None,
        analysis_kwargs.items()
    ))

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a Nextflow pipeline.
        api_response: AnalysisV4 = api_instance.create_nextflow_analysis_with_custom_input(
            project_id=str(project_id),
            create_nextflow_with_custom_input_analysis=nextflow_analysis,
            **analysis_kwargs
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->create_nextflow_analysis_with_custom_input: %s\n" % e)
        raise ApiException

    return api_response


def get_project_pipeline_input_parameters(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> List[InputParameter]:
    """
    Return the input parameters for a given project pipeline.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve input parameters for

    :return: The list of input parameters for the pipeline
    :rtype: List[`InputParameter <https://umccr.github.io/libica/openapi/v3/docs/InputParameter/>`_]

    :raises ApiException: If the API call to retrieve pipeline input parameters fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_input_parameters

        input_parameters = get_project_pipeline_input_parameters(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(f"Found {len(input_parameters)} parameter(s)")
        # Found 3 parameter(s)
        for param in input_parameters:
            print(f"Parameter: {param.code}")
            # Parameter: my-input-param
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

        # example passing only required values which don't have defaults set
        try:
            # Retrieve input parameters for a project pipeline.
            api_response: InputParameterList = api_instance.get_project_pipeline_input_parameters(
                project_id=str(project_id),
                pipeline_id=str(pipeline_id)
            )
        except ApiException as e:
            logger.error("Exception when calling ProjectPipelineApi->get_project_pipeline_input_parameters: %s\n" % e)
            raise ApiException

    return api_response.items


def get_project_pipeline_configuration_parameters(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> List[PipelineConfigurationParameter]:
    """
    Return the configuration parameters for a given project pipeline.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to retrieve configuration parameters for

    :return: The list of configuration parameters for the pipeline
    :rtype: List[`PipelineConfigurationParameter <https://umccr.github.io/libica/openapi/v3/docs/PipelineConfigurationParameter/>`_]

    :raises ApiException: If the API call to retrieve pipeline configuration parameters fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_configuration_parameters

        configuration_parameters = get_project_pipeline_configuration_parameters(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(f"Found {len(configuration_parameters)} parameter(s)")
        # Found 3 parameter(s)
        for param in configuration_parameters:
            print(f"Parameter: {param.code}")
            # Parameter: my-config-param
    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    try:
        # Retrieve input parameters for a project pipeline.
        api_response: PipelineConfigurationParameterList = api_instance.get_project_pipeline_configuration_parameters(
            project_id=str(project_id),
            pipeline_id=str(pipeline_id),
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->get_project_pipeline_input_parameters: %s\n" % e)
        raise ApiException

    # return each of the items
    return api_response.items


def convert_icav2_uris_to_data_ids_from_cwl_input_json(
        input_obj: Union[str, int, bool, Dict, List]
) -> Tuple[
    # Input Object
    Union[str, Dict, List],
        # Mount List
    List[AnalysisInputDataMount],
        # External Data List
    List[AnalysisInputExternalData]
]:
    """
    Convert ICAv2 URIs to data IDs from a CWL input JSON object.

    .. deprecated:: 2.45.0
        Use :func:`convert_uris_to_data_ids_from_cwl_input_json` instead.

    :param input_obj: The CWL input object containing URIs to convert

    :return: A tuple of the converted input object, mount list, and external data list
    :rtype: Tuple[Union[str, Dict, List], List[`AnalysisInputDataMount <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInputDataMount/>`_], List[`AnalysisInputExternalData <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInputExternalData/>`_]]

    :raises ValueError: If the input object cannot be parsed
    :raises ApiException: If the API call to resolve URIs fails
    """
    DeprecationWarning(
        "This function is deprecated, "
        "please use convert_uris_to_data_ids_from_cwl_input_json instead"
    )
    return convert_uris_to_data_ids_from_cwl_input_json(input_obj)


def convert_uris_to_data_ids_from_cwl_input_json(
        input_obj: Union[str, int, float, bool, Dict, List]
) -> Optional[Tuple[
    # Input Object
    Union[str, int, float, bool, Dict, List],
        # Mount List
    List[AnalysisInputDataMount],
        # External Data List
    List[AnalysisInputExternalData]
]]:
    """
    Convert all URIs in a CWL input JSON to data IDs with mount paths.

    :param input_obj: The CWL input object containing icav2:// or s3:// URIs to resolve

    :return: A tuple of the converted input object, mount list, and external data list
    :rtype: Tuple[Union[str, int, float, bool, Dict, List], List[`AnalysisInputDataMount <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInputDataMount/>`_], List[`AnalysisInputExternalData <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInputExternalData/>`_]]

    :raises ValueError: If the input object type cannot be parsed
    :raises ApiException: If the API call to resolve URIs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import convert_uris_to_data_ids_from_cwl_input_json
        input_obj = {
            "input_file": {
                "class": "File",
                "location": "icav2://project-123/data-path/file.txt"
            }
        }
        input_obj_new, mount_list, external_data_list = convert_uris_to_data_ids_from_cwl_input_json(
            input_obj
        )
        print(f"Mounts: {len(mount_list)}, External: {len(external_data_list)}")
        # Mounts: 1, External: 0
    """
    # Importing from another functions directory should be done locally
    from ...project_data import (
        convert_uri_to_project_data_obj
    )
    from ...storage_credentials import (
        get_storage_credential_id_from_s3_uri,
        get_relative_path_from_credentials_prefix
    )

    # Set default mount list
    input_obj_new_list = []
    mount_list = []
    external_data_list = []

    # Convert basic / immutable types
    if (
            isinstance(input_obj, bool) or
            isinstance(input_obj, int) or
            isinstance(input_obj, str) or
            isinstance(input_obj, float)
    ):
        return input_obj, mount_list, external_data_list

    # Convert dict of list types recursively
    if isinstance(input_obj, List):
        for input_item in input_obj:
            input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_cwl_input_json(
                input_item)
            input_obj_new_list.append(input_obj_new_item)
            mount_list.extend(mount_list_new)
            external_data_list.extend(external_data_list_new)
        return input_obj_new_list, mount_list, external_data_list

    # Convert dict types recursively
    if isinstance(input_obj, Dict):
        if "class" in input_obj.keys() and input_obj["class"] in ["File", "Directory"]:
            # Resolve location
            if (
                    is_uri_format(input_obj.get("location", "")) and
                    urlparse(input_obj.get("location", "")).scheme in [ICAV2_URI_SCHEME, S3_URI_SCHEME]
            ):
                # Check directory has a trailing slash
                if input_obj.get("Directory", None) is not None and not input_obj["location"].endswith("/"):
                    logger.error("Please ensure directories end with a trailing slash!")
                    logger.error(
                        f"Got location '{input_obj.get('location')}' for directory object. "
                        f"Please add a trailing slash and try again")
                    raise ValueError

                # Get the uri object
                uri_str = input_obj.get("location")
                # Check data types match
                if uri_str.endswith("/") and input_obj["class"] == "File":
                    logger.error("Got mismatch on data type and class for input object")
                    logger.error(
                        f"Class of {input_obj.get('location')} is set to file but found directory {uri_str}")
                    raise ValueError
                if not uri_str.endswith("/") and input_obj["class"] == "Directory":
                    logger.error("Got mismatch on data type and class for input object")
                    logger.error(
                        f"Class of {input_obj.get('location')} is set to directory but found file id {uri_str}")

                # Try to get the storage credential id from the uri
                storage_credential_id = get_storage_credential_id_from_s3_uri(cast(str, uri_str))

                # Prioritise the external data configuration where possible
                # Note that this may change in the future
                if storage_credential_id is not None:
                    # Get relative location path
                    mount_path = get_relative_path_from_credentials_prefix(
                        storage_credential_id,
                        cast(str, uri_str),
                    )
                    external_data_list.append(
                        AnalysisInputExternalData(
                            url=cast(str, uri_str),
                            type=S3_URI_SCHEME,
                            mountPath=mount_path,
                            s3Details=AnalysisS3DataDetails(
                                storageCredentialsId=coerce_to_uuid4_obj(storage_credential_id)
                            ),
                            basespaceDetails=None
                        )
                    )
                else:
                    input_obj_new: ProjectData = convert_uri_to_project_data_obj(input_obj.get("location"))
                    data_type: DataType = cast(DataType, input_obj_new.data.details.data_type)
                    owning_project_id: str = str(input_obj_new.data.details.owning_project_id)
                    data_id = input_obj_new.data.id
                    basename = input_obj_new.data.details.name
                    # Set mount path
                    mount_path = str(
                        Path(owning_project_id) /
                        Path(data_id) /
                        Path(basename)
                    )
                    mount_list.append(
                        AnalysisInputDataMount(
                            dataId=data_id,
                            mountPath=mount_path
                        )
                    )
                    # Readd in the folder data type if it exists
                    if data_type == FOLDER_DATA_TYPE:
                        mount_path += "/"

                input_obj["location"] = mount_path
            # Check for presigned urls in location, and check for 'stage' attribute
            elif input_obj.get("location", "").startswith("https://"):
                if not input_obj.get("stage", True):
                    # Pop out input attribute stage object
                    _ = input_obj.pop("stage")
                else:
                    # We stage the presigned url using a uuid for the path
                    mount_path = str(
                        Path("staged") /
                        Path(str(uuid.uuid4())) /
                        Path(urlparse(input_obj.get("location")).path).name
                    )
                    external_data_list.append(
                        AnalysisInputExternalData(
                            url=input_obj.get("location"),
                            type="http",
                            mountPath=mount_path,
                            basespaceDetails=None,
                            s3Details=None,
                        )
                    )
                    input_obj["location"] = mount_path

            # Get secondary Files
            if not len(input_obj.get("secondaryFiles", [])) == 0:
                old_secondary_files = input_obj.get("secondaryFiles", [])
                input_obj["secondaryFiles"] = []
                for input_item in old_secondary_files:
                    input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_cwl_input_json(
                        input_item)
                    input_obj["secondaryFiles"].append(input_obj_new_item)
                    mount_list.extend(mount_list_new)
                    external_data_list.extend(external_data_list_new)

            return input_obj, mount_list, external_data_list
        else:
            input_obj_dict = {}
            for key, value in input_obj.items():
                input_obj_dict[
                    key], mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_cwl_input_json(
                    value)
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
            return input_obj_dict, mount_list, external_data_list

    raise ValueError(f"Could not parse input object, input object is of type {type(input_obj)}")


def convert_uris_to_data_ids_from_str(
        input_str: str,
        inside_samplesheet: bool = False
) -> Tuple[
    str,
    List[AnalysisInputDataMount],
    List[AnalysisInputExternalData]
]:
    """
    Convert uris to data ids from str

    :param input_str: The input string containing one or more uris
    :param inside_samplesheet: Are we currently inside the samplesheet nested list

    :return: The converted input object, mount list and external data list

    :rtype: Tuple[
        Union[str, Dict, List],
        List[AnalysisInputDataMount],
        List[AnalysisInputExternalData]
    ]

    :raises: ValueError, ApiException
    """
    # Local imports
    from ...storage_credentials import (
        get_storage_credential_id_from_s3_uri,
        get_relative_path_from_credentials_prefix
    )
    from ...project_data import convert_uri_to_project_data_obj

    # Initialise lists
    mount_list: List[AnalysisInputDataMount] = []
    external_data_list: List[AnalysisInputExternalData] = []
    new_value = input_str

    uri_match: str
    for uri_match in URI_REGEX_OBJ.findall(input_str):
        # We only need to mount each once but the ways in which we mount are different
        # For folders, we need to first check if we can list all the files since external data mounts only support files
        is_mounted = False

        # Try to get the storage credential id from the uri
        storage_credential_id = get_storage_credential_id_from_s3_uri(cast(str, uri_match))

        if storage_credential_id is not None:
            mount_path = get_relative_path_from_credentials_prefix(
                storage_credential_id,
                cast(str, uri_match),
            )

            if uri_match.endswith("/"):
                # Recursively add ALL files in the directory to the external data list
                # Assuming we can list the directory
                s3_client: 'S3Client' = boto3.client('s3')
                bucket, prefix = parse_s3_uri(cast(str, uri_match))
                continuation_token = None

                while True:
                    try:
                        objects_response = s3_client.list_objects_v2(**dict(filter(
                            lambda kv_iter_: kv_iter_[1] is not None,
                            {
                                "Bucket": bucket,
                                "Prefix": prefix,
                                "ContinuationToken": continuation_token
                            }.items()
                        )))
                    except ClientError as e:
                        logger.warning(
                            f"Could not list objects in S3 URI {uri_match}, "
                            f"wrapica does not have permissions to list-objects in {uri_match}. "
                            f"Falling back to standard mount",
                        )
                        break

                    for s3_object in objects_response.get('Contents', []):
                        file_s3_uri = f"s3://{bucket}/{s3_object['Key']}"
                        file_mount_path = str(
                            Path(mount_path) /
                            Path(s3_object['Key']).relative_to(Path(prefix))
                        )
                        external_data_list.append(
                            AnalysisInputExternalData(
                                url=file_s3_uri,
                                type=S3_URI_SCHEME,
                                mountPath=file_mount_path,
                                s3Details=AnalysisS3DataDetails(
                                    storageCredentialsId=coerce_to_uuid4_obj(storage_credential_id),
                                ),
                                basespaceDetails=None,
                            )
                        )

                    # Is Truncated indicates there are more objects to retrieve
                    if not objects_response.get('IsTruncated', False):
                        break

                    continuation_token = objects_response['NextContinuationToken']
            else:
                external_data_list.append(
                    AnalysisInputExternalData(
                        url=cast(str, uri_match),
                        type=S3_URI_SCHEME,
                        mountPath=str(mount_path),
                        s3Details=AnalysisS3DataDetails(
                            storageCredentialsId=coerce_to_uuid4_obj(storage_credential_id)
                        ),
                        basespaceDetails=None
                    )
                )
            is_mounted = True

        # Otherwise use the standard mount path approach
        if not is_mounted:
            # Get relative location path
            input_obj_new: ProjectData = convert_uri_to_project_data_obj(cast(str, uri_match))
            owning_project_id: str = str(input_obj_new.data.details.owning_project_id)
            data_id = input_obj_new.data.id
            basename = input_obj_new.data.details.name

            # Set mount path
            mount_path = str(
                Path(owning_project_id) /
                Path(data_id) /
                Path(basename)
            )

            # Append the mount list
            mount_list.append(
                AnalysisInputDataMount(
                    dataId=data_id,
                    mountPath=mount_path
                )
            )

            # Add a trailing '/' to mount path if this is a directory
            if input_obj_new.data.details.data_type == FOLDER_DATA_TYPE and not mount_path.endswith("/"):
                mount_path += "/"

            is_mounted = True

        if not is_mounted:
            logger.error(
                f"Could not mount uri {uri_match}, "
                f"either it is not available in ICAv2, "
                f"or it is a folder and we do not have the credentials to "
                f"map the file"
            )
            raise FileNotFoundError(
                f"Could not mount uri {uri_match}, "
                f"either it is not available in ICAv2, "
                f"or it is a folder and we do not have the credentials to "
                f"map the file"
            )

        # Replace the URI in the new value
        # With the mount path
        new_value = re.sub(
            uri_match,
            (
                # We can use the __CES_WORKING_DIR__ placeholder inside the samplesheet
                str(CES_DATA_ABS_PATH.joinpath(mount_path))
                if inside_samplesheet
                else mount_path
            ),
            new_value
        )

    # Return the new value, mount list and external data list
    return new_value, mount_list, external_data_list


def convert_uris_to_data_ids_from_nextflow_input_json(
        input_obj: Union[str, int, bool, Dict[str, Any], List],
        cache_uri: Optional[str] = None,
        is_top_level: bool = True,
        inside_samplesheet: bool = False,
) -> Optional[Tuple[
    Union[str, Dict, List, bool, int, float],
    List[AnalysisInputDataMount],
    List[AnalysisInputExternalData]
]]:
    """
    Similar to the CWL version, however we don't have object types, instead we assume that if any value is a URI,
    we add the data id, mount it and add the mount path to the input json dereferenced dict.

    This has the added component of if there exists the key 'input' in the top level of the input_obj and
    the value is a list of objects, then we will need to convert this to a 'samplesheet.csv', upload to the cache uri
    And then append samplesheet to the list of mount paths.

    :param cache_uri:
    :param input_obj:
    :param is_top_level:

    :return: The converted input object, mount list and external data list
    :rtype: Tuple[
        Union[str, Dict, List],
        List[AnalysisInputDataMount],
        List[AnalysisInputExternalData]
    ]

    :raises: ValueError, ApiException
    """
    # Importing from another functions directory should be done locally
    from ...project_data import (
        get_project_data_obj_from_project_id_and_path,
        coerce_data_id_uri_or_path_to_project_data_obj,
        # Needed for uploading samplesheet
        create_folder_in_project,
        write_icav2_file_contents
    )

    # Set default mount list
    input_obj_new_list: List[Union[Dict, List]] = []
    mount_list: List[AnalysisInputDataMount] = []
    external_data_list: List[AnalysisInputExternalData] = []

    # Convert basic types
    if (
            isinstance(input_obj, bool) or
            isinstance(input_obj, int) or
            isinstance(input_obj, float)
    ):
        return input_obj, mount_list, external_data_list

    if isinstance(input_obj, str):
        new_value, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_str(
            input_obj,
            inside_samplesheet=inside_samplesheet
        )
        mount_list.extend(mount_list_new)
        external_data_list.extend(external_data_list_new)
        return new_value, mount_list, external_data_list

    # Convert dict of list types recursively
    if isinstance(input_obj, List):
        for input_item in input_obj:
            input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_nextflow_input_json(
                input_item,
                cache_uri,
                is_top_level=False,
                inside_samplesheet=inside_samplesheet,
            )
            input_obj_new_list.append(input_obj_new_item)
            mount_list.extend(mount_list_new)
            external_data_list.extend(external_data_list_new)
        return input_obj_new_list, mount_list, external_data_list

    # Convert dict types recursively
    if isinstance(input_obj, Dict):
        new_input_obj = {}

        for key, value in input_obj.items():
            # Check if this is the input key
            if (
                    # Is the top level key named 'samplesheet'
                    is_top_level and key == "samplesheet" and
                    # The value of samplesheet is a list of dicts
                    isinstance(value, List) and all(list(map(lambda val_iter_: isinstance(val_iter_, Dict), value)))
            ):
                # We have the samplesheet!
                # Extend all values to input object lists
                input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_nextflow_input_json(
                    value,
                    is_top_level=False,
                    inside_samplesheet=True,
                )

                # Add in the mounts / external data mounts
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)

                # Write the samplesheet to the cache uri
                if cache_uri is None:
                    logger.error("Cache URI must be provided for samplesheet upload")
                    raise ValueError
                if not cache_uri.endswith("/"):
                    logger.error("Cache URI must end with a trailing slash")
                    raise ValueError("Cache URI must end with a trailing slash")
                # Get cache uri as an icav2 object
                cache_uri_obj = cast(
                    ProjectData,
                    coerce_data_id_uri_or_path_to_project_data_obj(
                        cache_uri,
                        create_data_if_not_found=True
                    )
                )
                # Create the samplesheet folder
                logger.info("Creating samplesheet folder")
                samplesheet_folder_obj = create_folder_in_project(
                    project_id=cache_uri_obj.project_id,
                    folder_path=(Path(cast(str, cache_uri_obj.data.details.path)) / SAMPLESHEET_DIR_NAME)
                )

                # Upload the dereferenced samplesheet
                logger.info("Uploading dereferenced samplesheet to cache uri: %s", cache_uri_obj.data.details.path)
                write_icav2_file_contents(
                    cache_uri_obj.project_id,
                    Path(samplesheet_folder_obj.data.details.path, SAMPLESHEET_WITH_PLACEHOLDERS_NAME),
                    StringIO(pd.DataFrame(input_obj_new_item).to_csv(header=True, index=False)),
                )
                # Add the samplesheet to the mount list
                logger.info("Adding samplesheet directory to the mount list")
                samplesheet_mount_dir_path = Path(str(samplesheet_folder_obj.project_id)) / samplesheet_folder_obj.data.id / SAMPLESHEET_DIR_NAME
                mount_list.append(
                    AnalysisInputDataMount(
                        dataId=get_project_data_obj_from_project_id_and_path(
                            project_id=cache_uri_obj.project_id,
                            data_path=Path(cast(str, samplesheet_folder_obj.data.details.path)),
                            data_type=FOLDER_DATA_TYPE,
                        ).data.id,
                        mountPath=str(samplesheet_mount_dir_path)
                    )
                )

                # The samplesheet is labeled as 'input' for nf-core pipelines
                new_input_obj.update({
                    "input": str(samplesheet_mount_dir_path / SAMPLESHEET_WITH_ABS_PATHS_NAME)
                })

                continue

            if isinstance(value, Dict):
                input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_nextflow_input_json(
                    value,
                    cache_uri,
                    is_top_level=False,
                    inside_samplesheet=inside_samplesheet,
                )
                new_input_obj.update({
                    key: input_obj_new_item
                })
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
                continue

            # If the value is a list, we need to convert the list items
            if isinstance(value, List):
                input_obj_new_item, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_nextflow_input_json(
                    value,
                    is_top_level=False,
                    inside_samplesheet=inside_samplesheet,
                )
                # Update the new input object with the new item list
                new_input_obj.update({
                    key: input_obj_new_item
                })
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
                continue

            if (
                    isinstance(value, str)
            ):
                new_value, mount_list_new, external_data_list_new = convert_uris_to_data_ids_from_str(
                    value,
                    inside_samplesheet=inside_samplesheet,
                )

                # Now update the new input value with the substituted mount path values
                new_input_obj.update({
                    key: new_value
                })
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
                continue

            # Otherwise we assume this is a normal value
            # Like str, int, bool, etc.
            new_input_obj.update({
                key: value
            })

        return new_input_obj, mount_list, external_data_list

    raise ValueError(f"Could not parse input object, input object is of type {type(input_obj)}")


def list_project_pipelines(
        project_id: Union[UUID4, str]
) -> List[ProjectPipeline]:
    """
    List all pipelines available in a given project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string

    :return: The list of project pipeline objects in the project
    :rtype: List[`ProjectPipeline <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipeline/>`_]

    :raises ValueError: If the API call to list project pipelines fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import list_project_pipelines

        pipeline_list = list_project_pipelines(project_id="project-123")

        print(f"Found {len(pipeline_list)} pipeline(s)")
        # Found 3 pipeline(s)
        for pipeline in pipeline_list:
            print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
            # Pipeline ID: abcd1234-..., Code: my-pipeline
    """

    # Get api instance
    with ApiClient(get_icav2_configuration()) as api_client:

        api_instance = ProjectPipelineApi(api_client)

    try:
        api_response = api_instance.get_project_pipelines(project_id=str(project_id))
    except ApiException as e:
        raise ValueError("Exception when calling ProjectPipelineApi->get_project_pipelines: %s\n" % e)

    return api_response.items


def is_pipeline_in_project(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> bool:
    """
    Check whether a pipeline is linked to a given project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to check for in the project

    :return: True if the pipeline exists in the project, False otherwise
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import is_pipeline_in_project

        pipeline_is_in_project = is_pipeline_in_project(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )

        print(pipeline_is_in_project)
        # True
    """

    try:
        _ = next(
            filter(
                lambda project_pipeline_iter: str(project_pipeline_iter.pipeline.id) == str(pipeline_id),
                list_project_pipelines(project_id)
            )
        )
    except StopIteration:
        return False
    return True


def list_projects_with_pipeline(
        pipeline_id: Union[UUID4, str],
        include_hidden_projects: bool
) -> List[Project]:
    """
    Return a list of projects that have a given pipeline linked.

    :param pipeline_id: The pipeline identifier to search for across projects
    :param include_hidden_projects: Whether to include hidden projects in the search

    :return: The list of projects containing the specified pipeline
    :rtype: List[`Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_]

    :raises ValueError: If the pipeline ID is invalid
    :raises ApiException: If the API call to list projects fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import list_projects_with_pipeline

        projects = list_projects_with_pipeline(
            pipeline_id="pipeline-456",
            include_hidden_projects=False
        )

        print(f"Found {len(projects)} project(s)")
        # Found 3 project(s)
        for project in projects:
            print(f"Project ID: {project.id}, Name: {project.name}")
            # Project ID: abcd1234-..., Name: my-project-name
    """
    from ...project import list_projects
    return list(
        filter(
            lambda project_iter: is_pipeline_in_project(project_iter.id, pipeline_id),
            list_projects(include_hidden_projects=include_hidden_projects)
        )
    )


def create_blank_params_xml(output_file_path: Path):
    """
    Create an empty params.xml file with no input parameters defined.

    :param output_file_path: The file system path to write the blank params XML to

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_blank_params_xml

        # Creates an empty params.xml file at the specified path
        create_blank_params_xml(output_file_path=Path("/path/to/params.xml"))
    """
    with open(output_file_path, "w") as params_h:
        for line in BLANK_PARAMS_XML_V2_FILE_CONTENTS:
            params_h.write(line + "\n")


def create_params_xml(inputs: List[WorkflowInputParameterType], output_path: Path):
    """
    Create a params.xml file from a list of workflow input parameters.

    :param inputs: The list of CWL workflow input parameter type objects
    :param output_path: The file system path to write the generated params XML to

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_params_xml

        # Creates a params.xml file from the input parameter definitions
        create_params_xml(inputs=[], output_path=Path("/path/to/params.xml"))
    """
    _ = inputs
    # FIXME - waiting on https://github.com/umccr-illumina/ica_v2/issues/17
    return create_blank_params_xml(output_path)


def release_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
):
    """
    Convert a project pipeline from draft status to released status.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to release within the project

    :raises ValueError: If the pipeline is not in draft status or does not belong to the user
    :raises ApiException: If the API call to release the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import release_project_pipeline

        # Releases the pipeline from draft to released status
        release_project_pipeline(
            project_id="project-123",
            pipeline_id="pipeline-456"
        )
    """
    from ...user import get_user_id_from_configuration

    # Get project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not cast(PipelineStatusType, project_pipeline_obj.pipeline.status) == "DRAFT":
        logger.error("Pipeline is not in draft status, cannot release already released pipeline")
        raise ValueError

    # Check pipeline id belongs to owner
    username = get_user_id_from_configuration()

    if not username == str(project_pipeline_obj.pipeline.owner.id):
        logger.error("This pipeline does not belong to you, you cannot release it")
        raise ValueError

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force headers for : based API call
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Release a pipeline.
        api_instance.release_project_pipeline(project_id=str(project_id), pipeline_id=str(pipeline_id))
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->release_project_pipeline: %s\n" % e)
        raise ApiException


def update_pipeline_file(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
        file_id: Union[UUID4, str],
        file_path: Path,
):
    """
    Update the content of an existing pipeline file on ICAv2.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier containing the file to update
    :param file_id: The pipeline file identifier to update
    :param file_path: The local file path containing the new content

    :raises ValueError: If the pipeline is not in draft status
    :raises ApiException: If the API call to update the file fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import update_pipeline_file

        # Updates the pipeline file content on ICAv2
        update_pipeline_file(
            project_id="project-123",
            pipeline_id="pipeline-456",
            file_id="file-789",
            file_path=Path("/path/to/updated-tool.cwl")
        )
    """
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not cast(PipelineStatusType, project_pipeline_obj.pipeline.status) == "DRAFT":
        logger.error("Pipeline is not in draft status, cannot update a pipeline file if it has been released")
        raise ValueError

    with ApiClient(get_icav2_configuration()) as api_client:
        # Force content type
        api_instance = ProjectPipelineApi(api_client)

    try:
        api_instance.update_project_pipeline_file(
            project_id=str(project_id),
            pipeline_id=str(pipeline_id),
            file_id=coerce_to_uuid4_obj(file_id),
            content=open(f"{file_path}", "rb").read()
        )
    except ApiException as e:
        logger.error("Update pipeline file failed: %s\n" % e)
        raise ApiException("Update pipeline file failed") from e


def delete_pipeline_file(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
        file_id: Union[UUID4, str]
):
    """
    Delete a pipeline file from a draft pipeline on ICAv2.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier containing the file to delete
    :param file_id: The pipeline file identifier to delete

    :raises ValueError: If the pipeline is not in draft status
    :raises ApiException: If the API call to delete the file fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import delete_pipeline_file

        # Deletes the specified file from the draft pipeline
        delete_pipeline_file(
            project_id="project-123",
            pipeline_id="pipeline-456",
            file_id="file-789"
        )
    """
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not cast(PipelineStatusType, project_pipeline_obj.pipeline.status) == "DRAFT":
        logger.error("Pipeline is not in draft status, cannot delete a pipeline file if it has been released")
        raise ValueError

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)
    try:
        # Delete a file for a pipeline.
        api_instance.delete_project_pipeline_file(
            project_id=str(project_id),
            pipeline_id=str(pipeline_id),
            file_id=coerce_to_uuid4_obj(file_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->delete_project_pipeline_file: %s\n" % e)
        raise ApiException("Exception when calling ProjectPipelineApi->delete_project_pipeline_file") from e


def add_pipeline_file(
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
        file_path: Path,
        relative_path: Optional[Path] = None
) -> Optional[PipelineFile]:
    """
    Add a file to a draft pipeline on ICAv2.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_id: The pipeline identifier to add the file to
    :param file_path: The local file path to upload to the pipeline
    :param relative_path: The relative path within the pipeline for the file. Defaults to None,
        in which case the file name is used directly

    :return: The created pipeline file object, or None if the file is empty
    :rtype: Optional[`PipelineFile <https://umccr.github.io/libica/openapi/v3/docs/PipelineFile/>`_]

    :raises ValueError: If the pipeline is not in draft status
    :raises ApiException: If the API call to add the file fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import add_pipeline_file

        pipeline_file = add_pipeline_file(
            project_id="project-123",
            pipeline_id="pipeline-456",
            file_path=Path("/path/to/tool.cwl")
        )

        print(f"File ID: {pipeline_file.id}")
        # File ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not cast(PipelineStatusType, project_pipeline_obj.pipeline.status) == "DRAFT":
        logger.error("Pipeline is not in draft status, cannot add a pipeline file if it has been released")
        raise ValueError

    # Check file is not empty
    if file_path.stat().st_size == 0:
        logger.warning(f"Cannot add an empty file to a pipeline, skipping {relative_path}")
        return None

    if relative_path is None:
        content = open(f"{file_path}", "rb").read()
    else:
        content = (
            str(
                relative_path
            ),
            open(f"{file_path}", "rb").read()
        )

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    try:
        # Create an additional input form file for a pipeline.
        api_response = api_instance.create_project_pipeline_file(
            project_id=str(project_id),
            pipeline_id=str(pipeline_id),
            content=content
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->create_project_pipeline_file: %s\n" % e)
        raise ApiException("Exception when calling ProjectPipelineApi->create_project_pipeline_file") from e

    # Collect the pipeline id from the response json
    return api_response


def create_cwl_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_code: str,
        workflow_path: Path,
        tool_paths: Optional[List[Path]] = None,
        workflow_description: Optional[str] = None,
        params_xml_file: Optional[Path] = None,
        analysis_storage: Optional[AnalysisStorageType] = None,
        workflow_html_documentation: Optional[Path] = None,
        resource_type: Optional[ResourceType] = None
) -> ProjectPipelineV4:
    """
    Create a CWL project pipeline from a workflow file and optional tool files.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The unique code string for the new pipeline
    :param workflow_path: The local path to the CWL workflow file
    :param tool_paths: The list of local paths to CWL tool files. Defaults to None,
        in which case no additional tools are uploaded
    :param workflow_description: The description text for the pipeline. Defaults to None,
        in which case no description is set
    :param params_xml_file: The local path to a params.xml file. Defaults to None,
        in which case a blank params XML is generated
    :param analysis_storage: The analysis storage object to use. Defaults to None,
        in which case the default Small storage is used
    :param workflow_html_documentation: The local path to HTML documentation. Defaults to None,
        in which case no documentation is attached
    :param resource_type: The compute resource type for the pipeline. Defaults to None,
        in which case no specific resource is requested

    :return: The created project pipeline object
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises ApiException: If the API call to create the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_cwl_project_pipeline

        pipeline = create_cwl_project_pipeline(
            project_id="project-123",
            pipeline_code="my_cwl_pipeline",
            workflow_path=Path("/path/to/workflow.cwl")
        )

        print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_cwl_pipeline
    """

    # Add in workflow and
    workflow_path_tuple_bytes = open(workflow_path, 'rb').read()

    # Check analysis storage
    if analysis_storage is None:
        workflow_obj: WorkflowType = load_document_by_uri(workflow_path)

        # Check if hints
        # FIXME - turn into function get_analysis_storage_size_from_workflow_obj
        if hasattr(workflow_obj, 'hints') and workflow_obj.hints is not None:
            try:
                resource_requirement = next(
                    filter(
                        lambda requirement_iter: requirement_iter.class_ == "ResourceRequirement",
                        workflow_obj.hints
                    )
                )

                # Get the analysis storage from the hints
                if hasattr(resource_requirement,
                           'extension_fields') and resource_requirement.extension_fields is not None:
                    analysis_storage_size = resource_requirement.extension_fields.get(
                        'https://platform.illumina.com/rdf/ica/resources/storage', None)
                    if analysis_storage_size is not None:
                        analysis_storage = get_analysis_storage_from_analysis_storage_size(
                            project_id=project_id,
                            analysis_storage_size=cast(AnalysisStorageSizeType, analysis_storage_size)
                        )

            except StopIteration:
                pass

        if analysis_storage is None:
            analysis_storage: AnalysisStorageType = get_analysis_storage_from_analysis_storage_size(
                project_id=project_id,
                analysis_storage_size=DEFAULT_ANALYSIS_STORAGE_SIZE
            )

    # Add params xml file to the file list
    if params_xml_file is None:
        params_xml_temp_file_obj = NamedTemporaryFile(prefix="params", suffix=".xml")
        params_xml_file = Path(params_xml_temp_file_obj.name)
        create_blank_params_xml(output_file_path=cast(Path, params_xml_file))

    params_xml_tuple_bytes = (str(params_xml_file), open(params_xml_file, 'rb').read())

    # Add tool paths to the file list
    if tool_paths is not None:
        tool_tuple_bytes_list = list(map(
            lambda tool_path_iter_: (
                str(
                    # Collect the tool path relative to the workflow path
                    tool_path_iter_.parent.absolute().resolve().relative_to(
                        workflow_path.parent.absolute().resolve()
                    ).joinpath(
                        tool_path_iter_.name
                    )
                ),
                open(tool_path_iter_, 'rb').read()
            ),
            tool_paths
        ))
    else:
        tool_tuple_bytes_list = []

    # Add the html documentation file to the file list
    if workflow_html_documentation is not None:
        html_documentation_tuple_bytes = (str(workflow_html_documentation), open(workflow_html_documentation, 'rb').read())
    else:
        html_documentation_tuple_bytes = None

    # Check if the resource type is set
    if resource_type is not None:
        if resource_type == "f1":
            resources = PipelineResources(
                f1=True,
            )
        elif resource_type == "f2":
            resources = PipelineResources(
                f2=True
            )
        elif resource_type == "gpu":
            resources = PipelineResources(
                gpu=True
            )
        else:
            resources = PipelineResources(
                **{"software-only": True}
            )
    else:
        resources = None

    # Check response
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    try:
        # Create a CWL pipeline within a project.
        api_response = api_instance.create_cwl_pipeline(
            project_id=str(project_id),
            code=pipeline_code,
            description=workflow_description,
            workflow_cwl_file=workflow_path_tuple_bytes,
            parameters_xml_file=params_xml_tuple_bytes,
            analysis_storage_id=coerce_to_uuid4_obj(analysis_storage.id),
            tool_cwl_files=tool_tuple_bytes_list,
            html_documentation=html_documentation_tuple_bytes,
            resources=resources
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->create_cwl_pipeline: %s\n" % e)
        raise ApiException("Exception when calling ProjectPipelineApi->create_cwl_pipeline") from e

    return get_project_pipeline_obj(
        project_id=project_id,
        pipeline_id=api_response.pipeline.id
    )


def create_cwl_workflow_from_zip(
        project_id: Union[UUID4, str],
        pipeline_code: str,
        zip_path: Path,
        analysis_storage: Optional[AnalysisStorageType] = None,
        workflow_description: Optional[str] = None,
        html_documentation_path: Optional[Path] = None,
        resource_type: Optional[ResourceType] = None
) -> ProjectPipelineV4:
    """
    Create a CWL project pipeline from a zip file containing workflow and tools.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The unique code string for the new pipeline
    :param zip_path: The local path to the zip file containing workflow.cwl and tools
    :param analysis_storage: The analysis storage object to use. Defaults to None,
        in which case the default Small storage is used
    :param workflow_description: The description text for the pipeline. Defaults to None,
        in which case the doc attribute from workflow.cwl is used
    :param html_documentation_path: The local path to HTML documentation. Defaults to None,
        in which case no documentation is attached
    :param resource_type: The compute resource type for the pipeline. Defaults to None,
        in which case no specific resource is requested

    :return: The created project pipeline object
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises FileNotFoundError: If the zip file does not contain a workflow.cwl file
    :raises ApiException: If the API call to create the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_cwl_workflow_from_zip

        pipeline = create_cwl_workflow_from_zip(
            project_id="project-123",
            pipeline_code="my_cwl_pipeline",
            zip_path=Path("/path/to/workflow.zip")
        )

        print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_cwl_pipeline
    """
    # Unzip the workflow and tool files
    with (TemporaryDirectory() as temp_dir, ZipFile(zip_path, 'r') as zip_h):
        zip_h.extractall(temp_dir)

        # Get the subdirectory of the zip paths
        zip_dir = Path(temp_dir) / zip_path.stem

        # Get the workflow and tool paths
        workflow_path = Path(zip_dir) / "workflow.cwl"

        # Check the workflow path
        if not workflow_path.is_file():
            logger.error("Cannot create cwl workflow from zip, expected a file named 'workflow.cwl'")
            raise FileNotFoundError

        # Get the tool paths (these might not exist)
        tool_paths = list(
            filter(
                lambda tool_path_iter: (
                        tool_path_iter.is_file() and
                        tool_path_iter.name not in ['workflow.cwl', 'params.xml']
                ),
                Path(zip_dir).rglob("*")
            )
        )

        # Get the params xml file
        params_xml_file = Path(zip_dir) / "params.xml"

        # Check the params xml file
        if not params_xml_file.is_file():
            params_xml_file = None

        # Add workflow description from workflow.cwl
        # If not set
        if workflow_description is None:
            workflow_obj: WorkflowType = load_document_by_uri(workflow_path)
            workflow_description = str(workflow_obj.doc)

        # Create the cwl workflow
        return create_cwl_project_pipeline(
            project_id=project_id,
            pipeline_code=pipeline_code,
            workflow_path=workflow_path,
            tool_paths=tool_paths,
            workflow_description=workflow_description,
            params_xml_file=params_xml_file,
            analysis_storage=analysis_storage,
            workflow_html_documentation=html_documentation_path,
            resource_type=resource_type
        )


def create_nextflow_pipeline_from_zip(
        project_id: Union[UUID4, str],
        pipeline_code: str,
        zip_path: Path,
        workflow_description: str,
        html_documentation_path: Optional[Path] = None,
        resource_type: Optional[ResourceType] = None
) -> ProjectPipelineV4:
    """
    Create a Nextflow project pipeline from a zip file containing workflow files.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The unique code string for the new pipeline
    :param zip_path: The local path to the zip file containing main.nf and config
    :param workflow_description: The description text for the pipeline
    :param html_documentation_path: The local path to HTML documentation. Defaults to None,
        in which case no documentation is attached
    :param resource_type: The compute resource type for the pipeline. Defaults to None,
        in which case no specific resource is requested

    :return: The created project pipeline object
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises ApiException: If the API call to create the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_nextflow_pipeline_from_zip

        pipeline = create_nextflow_pipeline_from_zip(
            project_id="project-123",
            pipeline_code="my_nf_pipeline",
            zip_path=Path("/path/to/pipeline.zip"),
            workflow_description="My Nextflow pipeline"
        )

        print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_nf_pipeline
    """
    # Extract the zip file
    with TemporaryDirectory() as temp_dir:
        zip_h = ZipFile(zip_path, 'r')
        zip_h.extractall(temp_dir)

        # Get the subdirectory of the zip paths
        zip_dir = Path(temp_dir) / zip_path.stem

        # Get the main.nf file
        main_nf_path = zip_dir / "main.nf"

        # Get the nextflow.config file
        config_file = zip_dir / "nextflow.config"

        # Add ICAv2 Config
        with open(zip_dir / ICAV2_CONFIG_NEXTFLOW_PATH, 'w') as config_h:
            config_h.write(get_default_icav2_config_content())
        # Add 'includeConfig' directive to the nextflow.config file
        include_icav2_config_into_nextflow_config(config_file)

        # Get the params xml file
        params_xml_file_path = zip_dir / "params.xml"

        # Collect all other files
        local_workflow_and_module_paths = list(
            filter(
                lambda file_iter: (
                        file_iter.is_file() and not (
                            file_iter == main_nf_path or
                            file_iter == config_file or
                            file_iter == params_xml_file_path
                        )
                        # It's not placed in a subdirectory that is a hidden directory in the top directory
                        and not (
                            file_iter.absolute().resolve().relative_to(zip_dir).parts[0].startswith(".")
                        )
                        # And the file isn't empty
                        and file_iter.stat().st_size > 0
                ),
                zip_dir.rglob("*")
            )
        )

        # Create the nextflow pipeline
        return create_nextflow_project_pipeline(
            project_id=project_id,
            pipeline_code=pipeline_code,
            main_nextflow_file=main_nf_path,
            nextflow_config_file=config_file,
            other_nextflow_files=local_workflow_and_module_paths,
            workflow_description=workflow_description,
            params_xml_file=params_xml_file_path if params_xml_file_path.is_file() else None,
            workflow_html_documentation=html_documentation_path,
            resource_type=resource_type
        )


def create_nextflow_pipeline_from_nf_core_zip(
        project_id: Union[UUID4, str],
        pipeline_code: str,
        zip_path: Path,
        pipeline_revision: str,
        workflow_description: Optional[str] = None,
        html_documentation_path: Optional[Path] = None,
        resource_type: Optional[ResourceType] = None
) -> ProjectPipelineV4:
    """
    Create a Nextflow project pipeline from an nf-core zip download.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The unique code string for the new pipeline
    :param zip_path: The local path to the nf-core zip file
    :param pipeline_revision: The nf-core pipeline revision string used to locate the workflow directory
    :param workflow_description: The description text for the pipeline. Defaults to None,
        in which case no description is set
    :param html_documentation_path: The local path to HTML documentation. Defaults to None,
        in which case no documentation is attached
    :param resource_type: The compute resource type for the pipeline. Defaults to None,
        in which case no specific resource is requested

    :return: The created project pipeline object
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises ApiException: If the API call to create the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_nextflow_pipeline_from_nf_core_zip
        pipeline = create_nextflow_pipeline_from_nf_core_zip(
            project_id="project-123",
            pipeline_code="nfcore_bamtofastq__1_0_0",
            zip_path=Path("/path/to/bamtofastq.zip"),
            pipeline_revision="1.0.0",
            workflow_description="nf-core bamtofastq pipeline"
        )

        print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: nfcore_bamtofastq__1_0_0
    """

    temp_dir = TemporaryDirectory()

    # Unzip the workflow and tool files
    with ZipFile(zip_path, 'r') as zip_h:
        zip_h.extractall(temp_dir.name)

    # We don't need the config dir, just the workflow dir
    workflow_dir = (Path(temp_dir.name) / zip_path.stem / pipeline_revision.replace(".", "_")).absolute().resolve()

    # Get the workflow and tool paths
    main_nf_path = workflow_dir / "main.nf"
    # Check the workflow path
    if not main_nf_path.is_file():
        logger.error("Cannot create nextflow pipeline from nf-core zip, expected a file named 'main.nf'")

    # Collect the configuration file
    config_file = workflow_dir / "nextflow.config"

    # Check if the bin directory exists
    if not (workflow_dir / "bin").is_dir():
        logger.info("Adding in file 'bin/.keepme.txt' to the bin directory")
        # Create the bin directory
        (workflow_dir / "bin").mkdir(parents=True, exist_ok=True)
        # Create a keepme file (cannot be empty)
        with open((workflow_dir / "bin" / ".keepme.txt"), "w") as keepme_h:
            keepme_h.write("# Wrapica Auto-generated empty file\n")

    # Get the tool paths (these might not exist)
    local_workflow_and_module_paths = list(
        filter(
            lambda sub_path: (
                    # Make sure we have a file in the workflow directory
                    sub_path.is_file() and
                    sub_path.is_relative_to(workflow_dir) and
                    # And it's not one of the main.nf or nextflow.config files
                    not (
                            sub_path.absolute().resolve() == (workflow_dir / 'main.nf') or
                            sub_path.absolute().resolve() == (workflow_dir / 'nextflow.config')
                    ) and
                    # It's not placed in a subdirectory that is a hidden directory in the top directory
                    not (
                        sub_path.absolute().resolve().relative_to(workflow_dir).parts[0].startswith(".")
                    ) and
                    # Not a hidden file in the top directory
                    not (
                            sub_path.parent.absolute().resolve() == workflow_dir and
                            sub_path.name.startswith(".")
                    )
                    # Also not a test file
                    and not (
                        sub_path.name.endswith(".test") or
                        sub_path.name.endswith(".test.snap")
                    )
                    # And not a conda environment file or meta file
                    and not (
                        sub_path.name in ["environment.yml", "environment.yaml", "meta.yml", "meta.yaml"]
                    )
            ),
            workflow_dir.rglob("*")
        )
    )

    # Get the nextflow schema input json
    nextflow_schema_input_json_path = workflow_dir / "nextflow_schema.json"

    # Make the params xml file
    params_xml_tmp_file_obj = NamedTemporaryFile(prefix="params", suffix=".xml")
    params_xml_file_path = Path(params_xml_tmp_file_obj.name)

    # Get the params json file
    # FIXME - skipping this for now,
    # Since we use the custom input json option to
    # generate our analysis. It may be useful in the future to
    # grab the nextflow schema json file to prepopulate input yamls in the future
    # write_params_xml_from_nextflow_schema_json(
    #     nextflow_schema_json_path=nextflow_schema_input_json_path,
    #     params_xml_path=params_xml_file_path,
    #     pipeline_name=pipeline_code.split("__")[0],
    #     pipeline_version=pipeline_code.split("__")[1]
    # )
    # Instead we generate a blank params.xml file
    create_blank_params_xml(output_file_path=params_xml_file_path)

    # Now zip everything back up
    new_zip_tmp_dir_obj = TemporaryDirectory()
    new_zip_file_path = Path(new_zip_tmp_dir_obj.name) / (pipeline_code + ".zip")

    with ZipFile(new_zip_file_path, 'w') as zip_h:
        # Add in the main.nf file
        zip_h.write(main_nf_path, arcname=Path(pipeline_code) / main_nf_path.name)
        # Add in the nextflow.config file
        zip_h.write(config_file, arcname=Path(pipeline_code) / config_file.name)
        # Add in the params.xml file
        zip_h.write(params_xml_file_path, arcname=Path(pipeline_code) / params_xml_file_path.name)
        # Add in all the other files
        for tool_path in local_workflow_and_module_paths:
            zip_h.write(tool_path, arcname=Path(pipeline_code) / tool_path.relative_to(workflow_dir))

    # Create the nextflow pipeline
    return create_nextflow_pipeline_from_zip(
        project_id=project_id,
        pipeline_code=pipeline_code,
        zip_path=new_zip_file_path,
        workflow_description=workflow_description,
        html_documentation_path=html_documentation_path,
        resource_type=resource_type
    )


def create_nextflow_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_code: str,
        main_nextflow_file: Path,
        nextflow_config_file: Path,
        other_nextflow_files: List[Path],
        workflow_description: str,
        params_xml_file: Optional[Path] = None,
        analysis_storage: Optional[AnalysisStorageType] = None,
        workflow_html_documentation: Optional[Path] = None,
        resource_type: Optional[ResourceType] = None
) -> ProjectPipelineV4:
    """
    Create a Nextflow project pipeline from individual workflow files.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param pipeline_code: The unique code string for the new pipeline
    :param main_nextflow_file: The local path to the main.nf file
    :param nextflow_config_file: The local path to the nextflow.config file
    :param other_nextflow_files: The list of local paths to additional pipeline files
    :param workflow_description: The description text for the pipeline
    :param params_xml_file: The local path to a params.xml file. Defaults to None,
        in which case a blank params XML is generated
    :param analysis_storage: The analysis storage object to use. Defaults to None,
        in which case the default Small storage is used
    :param workflow_html_documentation: The local path to HTML documentation. Defaults to None,
        in which case no documentation is attached
    :param resource_type: The compute resource type for the pipeline. Defaults to None,
        in which case no specific resource is requested

    :return: The created project pipeline object
    :rtype: `ProjectPipelineV4 <https://umccr.github.io/libica/openapi/v3/docs/ProjectPipelineV4/>`_

    :raises ApiException: If the API call to create the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import create_nextflow_project_pipeline
        pipeline = create_nextflow_project_pipeline(
            project_id="project-123",
            pipeline_code="my_nf_pipeline",
            main_nextflow_file=Path("/path/to/main.nf"),
            nextflow_config_file=Path("/path/to/nextflow.config"),
            other_nextflow_files=[],
            workflow_description="My Nextflow pipeline"
        )

        print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my_nf_pipeline
    """

    # Get nextflow main and config files
    main_nextflow_file_tuple_bytes = ('main.nf', open(main_nextflow_file, 'rb').read())
    nextflow_config_file_tuple_bytes = ('nextflow.config', open(nextflow_config_file, 'rb').read())

    # Check analysis storage
    if analysis_storage is None:
        analysis_storage: AnalysisStorageType = get_analysis_storage_from_analysis_storage_size(
            project_id=project_id,
            analysis_storage_size=DEFAULT_ANALYSIS_STORAGE_SIZE
        )

    # Add params xml file to the file list
    if params_xml_file is None:
        params_xml_temp_file_obj = NamedTemporaryFile(prefix="params", suffix=".xml", delete=False)
        params_xml_file = Path(params_xml_temp_file_obj.name)
        create_blank_params_xml(output_file_path=params_xml_file)

    # Add params xml file to the file list
    params_xml_file_tuple_bytes = ('params.xml', open(params_xml_file, 'rb').read())

    # Add tool paths to the file list
    if other_nextflow_files is not None:
        other_nextflow_files_tuple_bytes_list = list(map(
            lambda nf_file_iter_: (
                str(
                    # Collect the tool path relative to the workflow path
                    nf_file_iter_.parent.absolute().resolve().relative_to(
                        main_nextflow_file.parent.absolute().resolve()
                    ).joinpath(
                        nf_file_iter_.name
                    )
                ),
                open(nf_file_iter_, 'rb').read()
            ),
            list(filter(
                lambda file_iter: not is_binary(str(file_iter)),
                other_nextflow_files
            ))
        ))
    else:
        other_nextflow_files_tuple_bytes_list = []

    # Add the html documentation file to the file list
    if workflow_html_documentation is not None:
        workflow_html_documentation_tuple_bytes = (str(workflow_html_documentation), open(workflow_html_documentation, 'rb').read())
    else:
        workflow_html_documentation_tuple_bytes = None

    # Check if the resource type is set
    if resource_type is not None:
        if resource_type == "f1":
            resources = PipelineResources(
                f1=True
            )
        elif resource_type == "f2":
            resources = PipelineResources(
                f2=True
            )
        elif resource_type == "gpu":
            resources = PipelineResources(
                gpu=True
            )
        else:
            resources = PipelineResources.from_dict(
                {"software-only": True}
            )
    else:
        resources = None


    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    try:
        # Create a Nextflow pipeline within a project.
        api_response = api_instance.create_nextflow_pipeline(
            project_id=str(project_id),
            code=pipeline_code,
            description=workflow_description,
            main_nextflow_file=main_nextflow_file_tuple_bytes,
            parameters_xml_file=params_xml_file_tuple_bytes,
            analysis_storage_id=coerce_to_uuid4_obj(analysis_storage.id),
            pipeline_language_version_id=coerce_to_uuid4_obj(get_default_nextflow_pipeline_version_id()),
            nextflow_config_file=nextflow_config_file_tuple_bytes,
            other_nextflow_files=other_nextflow_files_tuple_bytes_list,
            html_documentation=workflow_html_documentation_tuple_bytes,
            resources=resources
        )
    except Exception as e:
        logger.error("Exception when calling ProjectPipelineApi->create_nextflow_pipeline: %s\n" % e)
        raise ApiException("Exception when calling ProjectPipelineApi->create_nextflow_pipeline") from e

    # Get the pipeline id from the response
    pipeline_id = api_response.pipeline.id

    # Binary files need to be added to the pipeline separately
    binary_files = list(filter(
        lambda file_iter: is_binary(str(file_iter)),
        other_nextflow_files
    ))

    # Add in the binary files
    for binary_file in binary_files:
        logger.info(f"Adding in {binary_file} to pipeline separately")
        add_pipeline_file(
            project_id=project_id,
            pipeline_id=pipeline_id,
            file_path=binary_file,
            relative_path=binary_file.parent.absolute().resolve().relative_to(
                main_nextflow_file.parent.absolute().resolve()
            ).joinpath(
                binary_file.name
            )
        )

    return get_project_pipeline_obj(
        project_id=project_id,
        pipeline_id=pipeline_id
    )