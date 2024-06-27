#!/usr/bin/env python3

"""
Project Pipeline Helpers
"""

# Standard imports
import typing
from typing import Tuple, Dict, Optional, Union, List, BinaryIO
from urllib.parse import urlparse
import uuid
from pathlib import Path
from zipfile import ZipFile

import requests
from cwl_utils.parser import load_document_by_uri
from libica.openapi.v2.model.analysis_storage_v3 import AnalysisStorageV3
from libica.openapi.v2.model.analysis_storage_v4 import AnalysisStorageV4
from requests import HTTPError, Response
from tempfile import NamedTemporaryFile, TemporaryDirectory

# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.analysis_storage_api import AnalysisStorageApi
from libica.openapi.v2.api.entitlement_detail_api import EntitlementDetailApi
from libica.openapi.v2.api.project_analysis_api import ProjectAnalysisApi
from libica.openapi.v2.api.project_pipeline_api import ProjectPipelineApi

from libica.openapi.v2.models import (
    ActivationCodeDetail,
    AnalysisInputDataMount,
    AnalysisInputExternalData,
    AnalysisV3,
    AnalysisV4,
    CreateCwlAnalysis,
    CreateNextflowAnalysis,
    CwlAnalysisJsonInput,
    CwlAnalysisStructuredInput,
    InputParameter,
    InputParameterList,
    NextflowAnalysisInput,
    PipelineConfigurationParameterList,
    PipelineFile,
    Project,
    ProjectData,
    ProjectPipeline,
    SearchMatchingActivationCodesForCwlAnalysis,
    SearchMatchingActivationCodesForNextflowAnalysis
)

# Local imports
from ...utils.logger import get_logger
from ...utils.configuration import get_icav2_configuration
from ...utils.cwl_typing_helpers import WorkflowInputParameterType
from ...utils.globals import BLANK_PARAMS_XML_V2_FILE_CONTENTS, NEXTFLOW_VERSION_UUID

from ...enums import AnalysisStorageSize, WorkflowLanguage, DataType, PipelineStatus
from ...utils.miscell import is_uuid_format
from ...utils.nextflow_helpers import (
    convert_base_config_to_icav2_base_config,
    write_params_xml_from_nextflow_schema_json
)

if typing.TYPE_CHECKING:
    # Import type hints for IDE only, not at runtime
    # Prevents circular imports
    from ..classes.cwl_analysis import ICAv2CWLPipelineAnalysis
    from ..classes.analysis import ICAv2PipelineAnalysisTags

AnalysisType = Union[AnalysisV3, AnalysisV4]
AnalysisStorageType = Union[AnalysisStorageV3, AnalysisStorageV4]

logger = get_logger()


def get_project_pipeline_obj(project_id: str, pipeline_id: str) -> ProjectPipeline:
    """
    Given a project id and pipeline id, return the project pipeline object

    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to retrieve

    :return: The project pipeline object
    :rtype: `ProjectPipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectPipeline/>`_

    :raises: ValueError: If the pipeline cannot be found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_obj

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a pipeline.
        api_response: ProjectPipeline = api_instance.get_project_pipeline(project_id, pipeline_id)
    except ApiException as e:
        raise ValueError("Exception when calling ProjectPipelineApi->get_project_pipeline: %s\n" % e)

    return api_response


def get_project_pipeline_obj_from_pipeline_code(project_id: str, pipeline_code: str) -> ProjectPipeline:
    """
    Given a project id and pipeline code, return the project pipeline object

    :param project_id: The project id that the pipeline exists in
    :param pipeline_code: The pipeline code to retrieve

    :return: The pipeline id
    :rtype: str

    :raises: ValueError: If the pipeline cannot be found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_id_from_pipeline_code

        project_id = "project-123"
        pipeline_code = "pipeline-123"

        pipeline_id = get_project_pipeline_id_from_pipeline_code(project_id, pipeline_code)
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


def get_project_pipeline_id_from_pipeline_code(project_id: str, pipeline_code: str) -> str:
    """
    Given a project pipeline code and project id, return the pipeline id

    :param project_id: The project id that the pipeline exists in
    :param pipeline_code: The pipeline code to retrieve

    :return: The pipeline id
    :rtype: str

    :raises: ValueError: If the pipeline cannot be found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_id_from_pipeline_code

        project_id = "project-123"
        pipeline_code = "pipeline-123"

        pipeline_id = get_project_pipeline_id_from_pipeline_code(project_id, pipeline_code)
    """
    return get_project_pipeline_obj_from_pipeline_code(project_id, pipeline_code).pipeline.id


def get_default_analysis_storage_obj_from_project_pipeline(project_id: str, pipeline_id: str) -> AnalysisStorageType:
    """
    Given a project id and pipeline id, return the default analysis storage object for that pipeline

    :param project_id:
    :param pipeline_id:

    :return: The analysis storage
    :rtype: `AnalysisStorage <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisStorage/>`_

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_default_analysis_storage_id_from_project_pipeline

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        # Use get_project_pipeline_id_from_pipeline_code to get the pipeline id

        analysis_storage_obj = get_default_analysis_storage_obj_from_project_pipeline(project_id, pipeline_id)

    """

    # Get the project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Return the analysis storage
    return project_pipeline_obj.pipeline.analysis_storage_obj


def get_default_analysis_storage_id_from_project_pipeline(project_id: str, pipeline_id: str) -> str:
    """
    Given a project id and pipeline id, return the default analysis storage id for that pipeline

    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to retrieve the analysis storage information from

    :return: The analysis storage id
    :rtype: str

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_default_analysis_storage_id_from_project_pipeline

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        # Use get_project_pipeline_id_from_pipeline_code to get the pipeline id

        analysis_storage_id = get_default_analysis_storage_id_from_project_pipeline(project_id, pipeline_id)
    """

    # Get the project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Return the analysis storage id
    return project_pipeline_obj.pipeline.analysis_storage_obj.id


def get_project_pipeline_description_from_pipeline_id(project_id: str, pipeline_id: str) -> str:
    """
    Get a project pipeline description from a project id and pipeline id

    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to retrieve the description from

    :return: The pipeline description
    :rtype: str

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_project_pipeline_description_from_pipeline_id

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        # Use get_project_pipeline_id_from_pipeline_code to get the pipeline id
        pipeline_description = get_project_pipeline_description_from_pipeline_id(project_id, pipeline_id)
    """
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    return project_pipeline_obj.pipeline.description


def get_analysis_storage_id_from_analysis_storage_size(analysis_storage_size: AnalysisStorageSize) -> str:
    """
    Given an analysis storage size, return the analysis storage id

    :param analysis_storage_size: The analysis storage size to retrieve the id for

    :return: The analysis storage id
    :rtype: str

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import get_analysis_storage_id_from_analysis_storage_size
        from wrapica.enums import AnalysisStorageSize

        analysis_storage_size = AnalysisStorageSize.SMALL

        analysis_storage_id = get_analysis_storage_id_from_analysis_storage_size(analysis_storage_size)
    """
    # Create an instance of the API class
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = AnalysisStorageApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve the list of analysis storage options.
        api_response = api_instance.get_analysis_storage_options()
    except ApiException as e:
        raise ValueError("Exception when calling AnalysisStorageApi->get_analysis_storage_options: %s\n" % e)

    try:
        return next(
            filter(
                lambda x: x.name == analysis_storage_size.value,
                api_response.items
            )
        )
    except StopIteration:
        raise ValueError(f"Could not find analysis storage size {analysis_storage_size} in this region")


def coerce_pipeline_id_or_code_to_project_pipeline_obj(pipeline_id_or_code: str) -> ProjectPipeline:
    """
    Coerce a pipeline id or code to a project pipeline object

    :param pipeline_id_or_code:
    :return: The project pipeline object

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import coerce_pipeline_id_or_code_to_project_pipeline_obj

        pipeline_id_or_code = "pipeline-123"

        project_pipeline_obj = coerce_pipeline_id_or_code_to_project_pipeline_obj(pipeline_id_or_code)
    """
    from ...project import get_project_id

    project_id = get_project_id()

    if is_uuid_format(pipeline_id_or_code):
        return get_project_pipeline_obj(project_id, pipeline_id_or_code)
    return get_project_pipeline_obj_from_pipeline_code(project_id, pipeline_id_or_code)


def get_analysis_storage_from_analysis_storage_id(analysis_storage_id: str) -> AnalysisStorageType:
    """
    Given an analysis storage id, return the analysis storage object
    :param analysis_storage_id:

    :return: The analysis storage object
    :rtype: `AnalysisStorage <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisStorage/>`_

    :raises: ValueError, ApiException
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = AnalysisStorageApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve the list of analysis storage options.
        api_response = api_instance.get_analysis_storage_options()
    except ApiException as e:
        logger.error("Exception when calling AnalysisStorageApi->get_analysis_storage_options: %s\n" % e)
        raise ApiException

    try:
        return next(
            filter(
                lambda storage_obj_iter: storage_obj_iter.id == analysis_storage_id,
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find analysis storage id {analysis_storage_id}")
        raise ValueError


def get_analysis_storage_from_analysis_storage_size(analysis_storage_size: AnalysisStorageSize) -> AnalysisStorageType:
    """
    Given an analysis storage size, return the analysis storage object

    :param analysis_storage_size:
    :return:
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = AnalysisStorageApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve the list of analysis storage options.
        api_response = api_instance.get_analysis_storage_options()
    except ApiException as e:
        logger.error("Exception when calling AnalysisStorageApi->get_analysis_storage_options: %s\n" % e)
        raise ApiException

    try:
        return next(
            filter(
                lambda storage_obj_iter: storage_obj_iter.name == analysis_storage_size.value,
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find analysis storage size {analysis_storage_size.value}")
        raise ValueError


def coerce_analysis_storage_id_or_size_to_analysis_storage(analysis_storage_id_or_size: Union[str, AnalysisStorageSize]) -> AnalysisStorageType:
    """
    Given either an analysis storage id or analysis storage size, return the analysis storage id

    :param analysis_storage_id_or_size:
    :return:
    """
    if isinstance(analysis_storage_id_or_size, AnalysisStorageSize):
        return get_analysis_storage_from_analysis_storage_size(analysis_storage_id_or_size)
    if is_uuid_format(analysis_storage_id_or_size):
        return get_analysis_storage_from_analysis_storage_id(analysis_storage_id_or_size)
    return get_analysis_storage_from_analysis_storage_size(AnalysisStorageSize(analysis_storage_id_or_size))


def get_activation_id(
        project_id: str,
        pipeline_id: str,
        analysis_input: Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput, NextflowAnalysisInput],
        workflow_language: WorkflowLanguage
):
    """
    Get the activation id for a project pipeline analysis input

    :param project_id:
    :param pipeline_id:
    :param analysis_input:
    :param workflow_language:
    :return:
    """
    if workflow_language.value == "CWL":
        return get_best_matching_entitlement_detail_for_cwl_analysis(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input=analysis_input
        ).id

    elif workflow_language.value == "NEXTFLOW":
        return get_best_matching_entitlement_detail_for_nextflow_analysis(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input=analysis_input
        ).id
    else:
        raise ValueError(f"Workflow language {workflow_language.value} not supported")


def get_best_matching_entitlement_detail_for_cwl_analysis(
        project_id: str,
        pipeline_id: str,
        analysis_input: Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput]
) -> ActivationCodeDetail:
    """
    Given a project id, pipeline id and an analysis input object
    Return the best fitting activation ID

    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to retrieve the best matching activation code detail for
    :param analysis_input: The analysis input object

    :return: The best matching activation code detail for the CWL pipeline
    :rtype: `ActivationCodeDetail <https://umccr-illumina.github.io/libica/openapi/v2/docs/ActivationCodeDetail/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import (
            get_best_matching_entitlement_detail_for_cwl_analysis,
            CwlAnalysisJsonInput
        )

        project_id = "project-123"
        pipeline_id = "pipeline-123"
        analysis_input = CwlAnalysisJsonInput(
            input_json={
                "input": "json"
            }
        )

        best_matching_activation_code_detail = get_best_matching_entitlement_detail_for_cwl_analysis(
            project_id, pipeline_id, analysis_input
        )

        print(best_matching_activation_code_detail.id)
        # Output: "activation-123"

    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = EntitlementDetailApi(api_client)
        search_matching_activation_codes_for_cwl_analysis = SearchMatchingActivationCodesForCwlAnalysis(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input=analysis_input,
        )  # SearchMatchingActivationCodesForCwlAnalysis |  (optional)

        # example passing only required values which don't have defaults set
        # and optional values
        try:
            # Search the best matching activation code detail for Cwl pipeline.
            api_response = api_instance.find_best_matching_activation_code_for_cwl(
                search_matching_activation_codes_for_cwl_analysis=search_matching_activation_codes_for_cwl_analysis
            )
        except ApiException as e:
            raise ValueError(
                "Exception when calling EntitlementDetailApi->find_best_matching_activation_code_for_cwl: %s\n" % e
            )

    return api_response


def get_best_matching_entitlement_detail_for_nextflow_analysis(
        project_id: str,
        pipeline_id: str,
        analysis_input: NextflowAnalysisInput
) -> ActivationCodeDetail:
    """
    Given a project id, pipeline id and an analysis input object
    Return the best fitting activation ID

    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to retrieve the best matching activation code detail for
    :param analysis_input: The analysis input object

    :return: The best matching activation code detail for the Nextflow pipeline
    :rtype: `ActivationCodeDetail <https://umccr-illumina.github.io/libica/openapi/v2/docs/ActivationCodeDetail/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import (
            get_best_matching_entitlement_detail_for_nextflow_analysis,
            NextflowAnalysisInput
        )

        project_id = "project-123"
        pipeline_id = "pipeline-123"
        analysis_input = NextflowAnalysisInput(
            input_json={
                "input": "json"
            }
        )

        best_matching_activation_code_detail = get_best_matching_entitlement_detail_for_nextflow_analysis(
            project_id, pipeline_id, analysis_input
        )

        print(best_matching_activation_code_detail.id)
        # Output: "activation-123"
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = EntitlementDetailApi(api_client)
        search_matching_activation_codes_for_nextflow_analysis = SearchMatchingActivationCodesForNextflowAnalysis(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input=analysis_input
        )  # SearchMatchingActivationCodesForNextflowAnalysis |  (optional)

        # example passing only required values which don't have defaults set
        # and optional values
        try:
            # Search the best matching activation code detail for Cwl pipeline.
            api_response = api_instance.find_best_matching_activation_codes_for_nextflow(
                search_matching_activation_codes_for_nextflow_analysis=search_matching_activation_codes_for_nextflow_analysis
            )
        except ApiException as e:
            raise ValueError(
                "Exception when calling EntitlementDetailApi->find_best_matching_activation_code_for_nextflow: %s\n" % e)

    return api_response


def create_cwl_input_json_analysis_obj(
        user_reference: str,
        project_id: str,
        pipeline_id: str,
        analysis_input_dict: Dict,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        # Output parameters
        analysis_output_uri: Optional[str] = None,
        # Meta parameters
        tags: Optional['ICAv2PipelineAnalysisTags'] = None,
        # CWL Specific parameters
        cwltool_overrides: Optional[Dict] = None
) -> 'ICAv2CWLPipelineAnalysis':
    """
    Given a pipeline id (optional - can be in the ICAv2EngineParameters
    An input json where the location attributes point to icav2 uris
    Generate a CreateCwlAnalysis object ready for launch

    :param user_reference: The user reference to use for the analysis
    :param project_id: The project id that the pipeline exists in
    :param pipeline_id: The pipeline id to launch
    :param analysis_input_dict: The analysis input dictionary
    :param analysis_storage_id: The analysis storage id to use
    :param analysis_storage_size: The analysis storage size to use
    :param activation_id: The activation id to use
    :param analysis_output_uri: The analysis output uri to use
    :param tags: The tags to use
    :param cwltool_overrides: The cwltool overrides to use

    :return: The CWL analysis object
    :rtype: `ICAv2CWLPipelineAnalysis <wrapica.project_pipelines.ICAv2CWLPipelineAnalysis>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import (
            # Functions
            create_cwl_input_json_analysis_obj,
            # Classes
            ICAv2PipelineAnalysisTags
        )

        user_reference = "user-123"
        project_id = "project-123"
        pipeline_id = "pipeline-123"
        analysis_input_dict = {
            "my_input_parameter": {
              "class": "File",
              "location": "icav2://project-123/data-path/file.txt"
            }
        }
        analysis_storage_id = "analysis-storage-123"
        analysis_storage_size = AnalysisStorageSize.SMALL
        activation_id = "activation-123"
        analysis_output_uri = "icav2://project-123/output-path"
        tags = ICAv2PipelineAnalysisTags(
            technical_tags=[
              "my_technical_tag",
            ]
            user_tags=[
              "user='John'",
              "billing='ExpensiveGroup'"
            ]
        )

        cwl_pipeline_analysis = create_cwl_input_json_analysis_obj(
            user_reference=user_reference,
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input_dict=analysis_input_dict,
            analysis_storage_id=analysis_storage_id,
            analysis_storage_size=analysis_storage_size,
            activation_id=activation_id,
            analysis_output_uri=analysis_output_uri,
            tags=tags
        )

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
        analysis_input=cwl_input_obj(),
        analysis_storage_id=analysis_storage_id,
        analysis_storage_size=analysis_storage_size,
        activation_id=activation_id,
        analysis_output_uri=analysis_output_uri,
        tags=tags,
        cwltool_overrides=cwltool_overrides
    )

    return cwl_analysis


def launch_cwl_workflow(project_id: str, cwl_analysis: CreateCwlAnalysis, idempotency_key=None) -> AnalysisType:
    """
    Launch a CWL Workflow in a specific project context

    :param project_id: The project id to launch the CWL workflow in
    :param cwl_analysis: The CWL analysis object to launch
    :param idempotency_key: The Idempotency-Key header can be used to prevent duplicate requests and support retries.

    :return: the analysis ID along with the deconstructed json used for submission to the end point
    :rtype: `Analysis <https://umccr-illumina.github.io/libica/openapi/v2/docs/Analysis/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import (
            # Functions
            launch_cwl_workflow,
            # Wrapica classes
            ICAv2CWLPipelineAnalysis,
        )

        from wrapica.libica_models import CreateCwlAnalysis, Analysis

        # Initialise an ICAv2CWLPipeline Analysis object
        cwl_analysis = ICAv2CWLPipelineAnalysis(
            user_reference="user-123",
            project_id="project-123",
            pipeline_id="pipeline-123",
            analysis_input={
                "input": "json"
            }
        )

        # Generate the inputs and analysis object
        cwl_analysis.check_engine_parameters()
        cwl_analysis.create_analysis()

        # Launch the analysis pipeline
        analysis = launch_cwl_workflow(project_id, cwl_analysis.analysis)

        # Alternatively, just call cwl_analysis and it will launch the pipeline.
        # analysis = cwl_analysis()

        # Save the analysis
        cwl_analysis.save_analysis(Path("/path/to/analysis.json"))

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

        # override endpoint settings response type to the version we want i.e. AnalysisV3 or AnalysisV4
        endpoint_settings = api_instance.create_cwl_analysis_endpoint.settings
        endpoint_settings['response_type'] = (AnalysisV4,)

    # Collect kwargs
    analysis_kwargs = {
        "idempotency_key": idempotency_key
    }

    # Reduce analysis_kwargs to only those that are not None
    analysis_kwargs = {k: v for k, v in analysis_kwargs.items() if v is not None}

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a CWL pipeline.
        api_response: AnalysisType = api_instance.create_cwl_analysis(
            project_id,
            cwl_analysis,
            **analysis_kwargs
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->create_cwl_analysis: %s\n" % e)
        raise ApiException

    return api_response


def launch_nextflow_workflow(project_id: str, nextflow_analysis: CreateNextflowAnalysis) -> AnalysisType:
    """
    Launch a Nextflow Workflow in a specific project context

    :param project_id: The project id to launch the Nextflow workflow in
    :param nextflow_analysis: The Nextflow analysis object to launch

    :return: the analysis ID along with the deconstructed json used for submission to the end point
    :rtype: `Analysis <https://umccr-illumina.github.io/libica/openapi/v2/docs/Analysis/>`_

    :Examples:

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_pipelines import (
            # Functions
            launch_nextflow_workflow,
            # Wrapica classes
            ICAv2NextflowPipelineAnalysis,
        )

        from wrapica.libica_models import CreateNextflowAnalysis, Analysis

        # Initialise an ICAv2CWLPipeline Analysis object
        nextflow_analysis = ICAv2NextflowPipelineAnalysis(
            user_reference="user-123",
            project_id="project-123",
            pipeline_id="pipeline-123",
            analysis_input={
                "my_input_parameter": "icav2://path/to/data",
                "my_config_parameter": "value"
            }
        )

        # Generate the inputs and analysis object
        nextflow_analysis.check_engine_parameters()
        nextflow_analysis.create_analysis()

        # Launch the analysis pipeline
        analysis = launch_nextflow_workflow(project_id, nextflow_analysis.analysis)

        # Alternatively, just call cwl_analysis and it will launch the pipeline.
        # analysis = nextflow_analysis()

        # Save the analysis
        nextflow_analysis.save_analysis(Path("/path/to/analysis.json"))

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

        # override endpoint settings response type to the version we want i.e. AnalysisV3 or Analysis
        endpoint_settings = api_instance.create_nextflow_analysis_endpoint.settings
        endpoint_settings['response_type'] = (AnalysisV4,)

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a CWL pipeline.
        api_response: AnalysisV4 = api_instance.create_nextflow_analysis(
            project_id,
            nextflow_analysis
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->create_nextflow_analysis: %s\n" % e)
        raise ApiException

    return api_response


def get_project_pipeline_input_parameters(
        project_id: str,
        pipeline_id: str
) -> List[InputParameter]:
    """
    Get project pipeline input parameters, needed for structured input validation

    :param project_id:
    :param pipeline_id:

    :return: The input parameters for the project pipeline
    :rtype: List[`InputParameter <https://umccr-illumina.github.io/libica/openapi/v2/docs/InputParameter/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import (
            # Functions
            get_project_pipeline_input_parameters
        )
        from wrapica.libica_models import InputParameter

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        input_parameters: List[InputParameter] = get_project_pipeline_input_parameters(project_id, pipeline_id)

        for input_parameter in input_parameters:
            print(input_parameter.code)
            print(input_parameter.required)
            print(input_parameter.multi_value)

        # Output:
        # input_parameter_1
        # false
        # true
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

        # example passing only required values which don't have defaults set
        try:
            # Retrieve input parameters for a project pipeline.
            api_response: InputParameterList = api_instance.get_project_pipeline_input_parameters(
                project_id=project_id,
                pipeline_id=pipeline_id
            )
        except ApiException as e:
            logger.error("Exception when calling ProjectPipelineApi->get_project_pipeline_input_parameters: %s\n" % e)
            raise ApiException

    return api_response.items


def get_project_pipeline_configuration_parameters(
        project_id: str,
        pipeline_id: str
) -> List[Dict]:  # -> List[PipelineConfigurationParameter]:
    """
    Given a pipeline and project id, return the configuration parameters for the pipeline

    :param project_id:
    :param pipeline_id:

    :return: The configuration parameters for the project pipeline
    :rtype: List[`PipelineConfigurationParameter <https://umccr-illumina.github.io/libica/openapi/v2/docs/PipelineConfigurationParameter/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import (
            get_project_pipeline_configuration_parameters
        )
        from wrapica.libica_models import PipelineConfigurationParameter

        project_id = "project-123"
        pipeline_id = "pipeline-123"

        configuration_parameters: List[PipelineConfigurationParameter] = (
            get_project_pipeline_configuration_parameters(project_id, pipeline_id)
        )

        for configuration_parameter in configuration_parameters:
            print(configuration_parameter.code)
            print(configuration_parameter.required)
            print(configuration_parameter.multi_value)
            print(configuration_parameter.type)

        # Output:
        # configuration_parameter_1
        # false
        # true
        # boolean
    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    try:
        # Retrieve input parameters for a project pipeline.
        api_response: PipelineConfigurationParameterList = api_instance.get_project_pipeline_configuration_parameters(
            project_id=project_id,
            pipeline_id=pipeline_id,
            _check_return_type=False  # We return a list of dicts because of this
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
    From a cwl input json, convert all the icav2 uris to data ids

    :param input_obj: The CWL input object to convert

    :return: The converted input object, mount list and external data list
    :rtype: Tuple[
        Union[str, Dict, List],
        List[AnalysisInputDataMount],
        List[AnalysisInputExternalData]
    ]

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project_pipelines import convert_icav2_uris_to_data_ids_from_cwl_input_json

        input_obj = {
            "input_file": {
                "class": "File",
                "location": "icav2://project-123/data-path/file.txt"
            }
        }

        input_obj_new, mount_list, external_data_list = convert_icav2_uris_to_data_ids_from_cwl_input_json(
            input_obj
        )

        print(input_obj_new)
        # Output: {
        #   "input_file": {
        #     "class": "File",
        #     "location": "path/to/mount/file.txt"
        #   }
        # }

        print(mount_list)
        # Output: [
        #   AnalysisInputDataMount(
        #     data_id="fil.1234567890",
        #     mount_path="path/to/mount/file.txt"
        #   )

        print(external_data_list)
        # Output: []

    """
    # Importing from another functions directory should be done locally
    from ...project_data import (
        convert_icav2_uri_to_project_data_obj,
        presign_cwl_directory,
        create_download_url,
        get_project_data_obj_by_id,
        presign_cwl_directory_with_external_data_mounts
    )

    # Set default mount list
    input_obj_new_list = []
    mount_list = []
    external_data_list = []

    # Convert basic types
    if isinstance(input_obj, bool) or isinstance(input_obj, int) or isinstance(input_obj, str):
        return input_obj, mount_list, external_data_list

    # Convert dict of list types recursively
    if isinstance(input_obj, List):
        for input_item in input_obj:
            input_obj_new_item, mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(
                input_item)
            input_obj_new_list.append(input_obj_new_item)
            mount_list.extend(mount_list_new)
            external_data_list.extend(external_data_list_new)
        return input_obj_new_list, mount_list, external_data_list

    # Convert dict types recursively
    if isinstance(input_obj, Dict):
        if "class" in input_obj.keys() and input_obj["class"] in ["File", "Directory"]:
            # Resolve location
            if input_obj.get("location", "").startswith("icav2://"):
                # Check directory has a trailing slash
                if input_obj.get("Directory", None) is not None and not input_obj["location"].endswith("/"):
                    logger.error("Please ensure directories end with a trailing slash!")
                    logger.error(
                        f"Got location '{input_obj.get('location')}' for directory object. Please add a trailing slash and try again")
                    raise ValueError
                # Get relative location path
                input_obj_new: ProjectData = convert_icav2_uri_to_project_data_obj(input_obj.get("location"))
                data_type: DataType = DataType(input_obj_new.data.details.data_type)  # One of FILE | FOLDER
                owning_project_id: str = input_obj_new.data.details.owning_project_id
                data_id = input_obj_new.data.id
                basename = input_obj_new.data.details.name

                # TODO functionalise this section, may need this for cross-tenant data collection later
                # Check presign,
                presign_list = list(
                    filter(
                        lambda x: x == "presign=true",
                        urlparse(input_obj.get("location")).query.split("&")
                    )
                )
                if len(presign_list) > 0:
                    is_presign = True
                else:
                    is_presign = False

                # Check stage
                stage_list = list(
                    filter(
                        lambda x: x == "stage=false",
                        urlparse(input_obj.get("location")).query.split("&")
                    )
                )

                if len(stage_list) > 0:
                    # We must generate a presigned url for this file for the location attribute
                    is_stage = False
                    is_presign = True
                else:
                    is_stage = True

                if is_stage:
                    # Set mount path
                    mount_path = str(
                        Path(owning_project_id) /
                        Path(data_id) /
                        Path(basename)
                    )
                else:
                    # We don't want to add a mount path to this file.
                    mount_path = None

                # Check data types match
                if data_type == DataType.FOLDER and input_obj["class"] == "File":
                    logger.error("Got mismatch on data type and class for input object")
                    logger.error(
                        f"Class of {input_obj.get('location')} is set to file but found directory id {data_id} instead")
                    raise ValueError
                if data_type == DataType.FILE and input_obj["class"] == "Directory":
                    logger.error("Got mismatch on data type and class for input object")
                    logger.error(
                        f"Class of {input_obj.get('location')} is set to directory but found file id {data_id} instead")

                # Set file to presigned url
                # File cannot be set to 'stage' without also being a presigned url
                if is_presign and not is_stage:
                    # No mounting
                    if data_type == DataType.FILE:
                        input_obj["location"] = create_download_url(owning_project_id, data_id)
                    elif data_type == DataType.FOLDER:
                        # We need a basename for the directory now, not a location
                        input_obj["basename"] = get_project_data_obj_by_id(owning_project_id, data_id).data.details.name
                        _ = input_obj.pop("location")
                        input_obj["listing"] = presign_cwl_directory(
                            owning_project_id, data_id
                        )
                elif is_presign and is_stage:
                    if data_type == DataType.FILE:
                        input_obj["location"] = mount_path
                        external_data_list.append(
                            AnalysisInputExternalData(
                                url=create_download_url(owning_project_id, data_id),
                                type="http",
                                mount_path=input_obj.get("location")
                            )
                        )
                    elif data_type == DataType.FOLDER:
                        input_obj["basename"] = get_project_data_obj_by_id(owning_project_id, data_id).data.details.name
                        # We need a basename for the directory, not a location
                        _ = input_obj.pop("location")
                        external_data_list_new, input_obj["listing"] = presign_cwl_directory_with_external_data_mounts(
                            owning_project_id, data_id
                        )
                        external_data_list.extend(
                            external_data_list_new
                        )

                else:
                    mount_list.append(
                        AnalysisInputDataMount(
                            data_id=data_id,
                            mount_path=mount_path
                        )
                    )
                    input_obj["location"] = mount_path
            # Check for presigned urls in location, and check for 'stage' attribute
            elif input_obj.get("location", "").startswith("https://"):
                if not input_obj.get("stage", True):
                    # Pop out input attribute stage object
                    _ = input_obj.pop("stage")
                else:
                    # Get / set basename
                    if input_obj.get("basename", None) is None:
                        input_obj["basename"] = Path(urlparse(input_obj.get("location")).path).name

                    # We stage the presigned url using a uuid for the path
                    mount_path = str(
                        Path("staged") /
                        Path(str(uuid.uuid4())) /
                        Path(urlparse(input_obj.get("location")).path).name
                    )
                    input_obj["location"] = mount_path
                    external_data_list.append(
                        AnalysisInputExternalData(
                            url=input_obj.get("location"),
                            type="http",
                            mount_path=mount_path
                        )
                    )

            # Get secondary Files
            if not len(input_obj.get("secondaryFiles", [])) == 0:
                old_secondary_files = input_obj.get("secondaryFiles", [])
                input_obj["secondaryFiles"] = []
                for input_item in old_secondary_files:
                    input_obj_new_item, mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(
                        input_item)
                    input_obj["secondaryFiles"].append(input_obj_new_item)
                    mount_list.extend(mount_list_new)
                    external_data_list.extend(external_data_list_new)

            return input_obj, mount_list, external_data_list
        else:
            input_obj_dict = {}
            for key, value in input_obj.items():
                input_obj_dict[
                    key], mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(
                    value)
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
            return input_obj_dict, mount_list, external_data_list


def list_project_pipelines(
        project_id: str
) -> List[ProjectPipeline]:
    """
    List pipelines in project

    :param project_id: List all pipelines avialable to this project

    :return: The list of pipelines
    :rtype: List[`ProjectPipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectPipeline/>`_]

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        import json
        from wrapica.project_pipelines import list_pipelines_in_project

        # Get list of pipelines in project
        project_id = "project-123"
        pipeline_list = list_pipelines_in_project(project_id)

        print(
          json.dumps(
            map(
                lambda: pipeline_iter: {
                    "id": pipeline_iter.id,
                    "code": pipeline_iter.code,
                },
                pipeline_list
            )
          )
        )
    """

    # Get api instance
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectPipelineApi(api_client)

    try:
        api_response = api_instance.get_project_pipelines(project_id)
    except ApiException as e:
        raise ValueError("Exception when calling ProjectPipelineApi->get_project_pipelines: %s\n" % e)

    return api_response.items


def is_pipeline_in_project(
        project_id: str,
        pipeline_id: str
) -> bool:
    """
    Check if a pipeline is in a project

    :param project_id:  The project id to check
    :param pipeline_id: The pipeline id to check

    :return: True if the pipeline is in the project, False otherwise
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import is_pipeline_in_project

        # Check if pipeline is in project
        project_id = "project-123"
        pipeline_id = "pipeline-123"
        pipeline_is_in_project = is_pipeline_in_project(project_id, pipeline_id)
    """

    try:
        _ = next(
            filter(
                lambda project_pipeline_iter: project_pipeline_iter.pipeline.id == pipeline_id,
                list_project_pipelines(project_id)
            )
        )
    except StopIteration:
        return False
    return True


def list_projects_with_pipeline(
    pipeline_id: str,
    include_hidden_projects: bool
) -> List[Project]:
    """
    Given a pipeline id, return a list of projects that the pipeline is linked to

    :param pipeline_id: The pipeline id to check
    :param include_hidden_projects: Include hidden projects in the list

    :return: The list of projects
    :rtype: :rtype: List[`Project <https://umccr-illumina.github.io/libica/openapi/v2/docs/Project/>`_]
    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project_pipelines import list_projects_with_pipeline

        pipeline_id = "pipeline-123"
        project_id = next(list_projects_with_pipeline(pipeline_id, include_hidden_projects=False)).id

        project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)
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
    Create a params.xml file with no inputs

    :param output_file_path: The output file path we wish to write the file to

    :return: None

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import create_blank_params_xml

        # Create blank params file
        output_file_path = Path("/path/to/params.xml")
        create_blank_params_xml(output_file_path)
    """
    with open(output_file_path, "w") as params_h:
        for line in BLANK_PARAMS_XML_V2_FILE_CONTENTS:
            params_h.write(line + "\n")


def create_params_xml(inputs: List[WorkflowInputParameterType], output_path: Path):
    """
    From the inputs, create a params xml file

    :param inputs:
    :param output_path:

    :return:

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import create_params_xml

        # Create params xml file
        output_path = Path("/path/to/params.xml")
        create_params_xml([], output_path)
    """
    _ = inputs
    # FIXME - waiting on https://github.com/umccr-illumina/ica_v2/issues/17
    return create_blank_params_xml(output_path)


def release_project_pipeline(project_id: str, pipeline_id: str):
    """
    Convert a project pipeline from a draft status to a released status

    :param project_id:
    :param pipeline_id:

    :raises ValueError: If the pipeline is not in draft status, or if the pipeline does not belong to the user
    :raises ApiException: If the API call fails
    """
    from ...user import get_user_id_from_configuration

    # Get project pipeline object
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not PipelineStatus(project_pipeline_obj.pipeline.status) == PipelineStatus.DRAFT:
        logger.error("Pipeline is not in draft status, cannot release already released pipeline")
        raise ValueError

    # Check pipeline id belongs to owner
    username = get_user_id_from_configuration()

    if not username == project_pipeline_obj.pipeline.owner_id:
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
        api_instance.release_project_pipeline(project_id, pipeline_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->release_project_pipeline: %s\n" % e)
        raise ApiException


def update_pipeline_file(project_id: str, pipeline_id: str, file_id: str, file_path: Path):
    """
    Update the pipeline file on icav2

    :param project_id:
    :param pipeline_id:
    :param file_id:
    :param file_path:

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import update_pipeline_file

        # Set vars
        project_id = "project-123"
        pipeline_id = "pipeline-123"

        # Find pipefile where the file name is 'tabix-tool.cwl'
        file_id = next(
            filter(
                lambda file_iter: file_iter.name == "tabix-tool.cwl",
                get_pipeline_files(project_id, pipeline_id)
            )
        ).id

        tool_file_with_new_content = Path("/path/to/tabix-tool.cwl")

        update_pipeline_file(project_id, pipeline_id, file_id, file_path_with_new_content)

    """
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not PipelineStatus(project_pipeline_obj.pipeline.status) == PipelineStatus.DRAFT:
        logger.error("Pipeline is not in draft status, cannot update a pipeline file if it has been released")
        raise ValueError

    configuration = get_icav2_configuration()

    headers = {
        "Accept": "application/vnd.illumina.v3+json",
        "Authorization": f"Bearer {configuration.access_token}",
        # requests won"t add a boundary if this header is set when you pass files=
        # "Content-Type": "multipart/form-data",
    }

    files = {
        "content": open(f"{file_path}", "rb"),
    }

    try:
        response = requests.put(
            f"{configuration.host}/api/projects/{project_id}/pipelines/{pipeline_id}/files/{file_id}/content",
            headers=headers,
            files=files,
        )
        response.raise_for_status()
    except HTTPError as err:
        logger.error(f"Failed to update file {err}")
        raise ApiException

    # # Enter a context with an instance of the API client
    # with ApiClient(get_icav2_configuration()) as api_client:
    #     # Create an instance of the API class
    #     api_instance = ProjectPipelineApi(api_client)
    #
    # # Set content handler
    # content_h: IOBase
    # with open(file_name, 'rb') as content_h:
    #     # example passing only required values which don't have defaults set
    #     try:
    #         # Update the contents of a file for a pipeline.
    #         api_instance.update_pipeline_file(project_id, pipeline_id, file_id, content_h)
    #     except ApiException as e:
    #         logger.error("Exception when calling ProjectPipelineApi->update_pipeline_file: %s\n" % e)
    #         raise ApiException


def delete_pipeline_file(project_id: str, pipeline_id: str, file_id: str):
    """
    Delete the pipeline file on icav2

    :param project_id:
    :param pipeline_id:
    :param file_id:

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import update_pipeline_file

        # Set vars
        project_id = "project-123"
        pipeline_id = "pipeline-123"

        # Find pipefile where the file name is 'tabix-tool.cwl'
        # And delete it
        file_id = next(
            filter(
                lambda file_iter: file_iter.name == "tabix-tool.cwl",
                get_pipeline_files(project_id, pipeline_id)
            )
        ).id

        delete_pipeline_file(project_id, pipeline_id, file_id)
    """
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not PipelineStatus(project_pipeline_obj.pipeline.status) == PipelineStatus.DRAFT:
        logger.error("Pipeline is not in draft status, cannot delete a pipeline file if it has been released")
        raise ValueError

    # FIXME
    configuration = get_icav2_configuration()

    import requests

    headers = {
        "Accept": "application/vnd.illumina.v3+json",
        "Authorization": f"Bearer {configuration.access_token}",
        # requests won"t add a boundary if this header is set when you pass files=
        # "Content-Type": "multipart/form-data",
    }

    try:
        response = requests.delete(
            f"{configuration.host}/api/projects/{project_id}/pipelines/{pipeline_id}/files/{file_id}",
            headers=headers,
        )
        response.raise_for_status()
    except HTTPError as err:
        logger.error(f"Failed to update file {err}")
        raise ApiException

    # # Enter a context with an instance of the API client
    # with ApiClient(get_icav2_configuration()) as api_client:
    #     # Create an instance of the API class
    #     api_instance = ProjectPipelineApi(api_client)
    #
    #     # example passing only required values which don't have defaults set
    #     try:
    #         # Delete a file for a pipeline.
    #         api_instance.delete_pipeline_file(project_id, pipeline_id, file_id)
    #     except ApiException as e:
    #         print("Exception when calling ProjectPipelineApi->delete_pipeline_file: %s\n" % e)


def add_pipeline_file(project_id: str, pipeline_id: str, file_path: Path) -> PipelineFile:
    """
    Add a pipeline file to a pipeline on icav2

    :param project_id: The project id to add the file to
    :param pipeline_id: The pipeline id to add the file to
    :param file_path: The file path to add to the pipeline

    :return: The pipeline file object
    :rtype: `PipelineFile <https://umccr-illumina.github.io/libica/openapi/v2/docs/PipelineFile/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.project_pipelines import add_pipeline_file

        # Add pipeline file
        project_id = "project-123"
        pipeline_id = "pipeline-123"
        file_path = Path("/path/to/file.txt")
        pipeline_file = add_pipeline_file(project_id, pipeline_id, file_path)
    """
    # FIXME - next release these endpoints should be available in libica
    # First confirm pipeline is in draft mode
    project_pipeline_obj = get_project_pipeline_obj(project_id, pipeline_id)

    # Confirm pipeline is in draft status
    if not PipelineStatus(project_pipeline_obj.pipeline.status) == PipelineStatus.DRAFT:
        logger.error("Pipeline is not in draft status, cannot add a pipeline file if it has been released")
        raise ValueError

    configuration = get_icav2_configuration()

    headers = {
        "Accept": "application/vnd.illumina.v3+json",
        "Authorization": f"Bearer {configuration.access_token}",
        # requests won"t add a boundary if this header is set when you pass files=
        # "Content-Type": "multipart/form-data",
    }

    files = {
        "content": open(f"{file_path}", "rb"),
    }

    try:
        response = requests.post(
            f"{configuration.host}/api/projects/{project_id}/pipelines/{pipeline_id}/files",
            headers=headers,
            files=files,
        )
        response.raise_for_status()
    except HTTPError as err:
        logger.error(f"Failed to update file {err}")
        raise ApiException

    # Collect the pipeline id from the response json
    return PipelineFile(**response.json())

    # # Enter a context with an instance of the API client
    # with ApiClient(get_icav2_configuration()) as api_client:
    #     # Create an instance of the API class
    #     api_instance = ProjectPipelineApi(api_client)
    #
    # # Set content handler
    # content_h: IOBase
    # with open(file_name, 'rb') as content_h:
    #     # example passing only required values which don't have defaults set
    #     try:
    #         # Update the contents of a file for a pipeline.
    #         api_instance.add_pipeline_file(project_id, pipeline_id, content_h)
    #     except ApiException as e:
    #         logger.error("Exception when calling ProjectPipelineApi->update_pipeline_file: %s\n" % e)
    #         raise ApiException


def create_cwl_project_pipeline(
    project_id: str,
    pipeline_code: str,
    workflow_path: Path,
    tool_paths: Optional[List[Path]] = None,
    workflow_description: Optional[str] = None,
    params_xml_file: Optional[Path] = None,
    analysis_storage: Optional[AnalysisStorageType] = None,
    workflow_html_documentation: Optional[Path] = None,
):
    """
    Create a CWL project pipeline from a workflow path and tool paths

    :param project_id:
    :param pipeline_code:
    :param workflow_path:
    :param tool_paths:
    :param workflow_description:
    :param params_xml_file:
    :param analysis_storage:
    :param workflow_html_documentation:
    :return:
    """
    # Get configuration
    configuration = get_icav2_configuration()

    # Initialise the file list with the workflow path
    file_list: List[Union[
        Tuple[str, Tuple[str, BinaryIO]],
        Tuple[str, Tuple[str, BinaryIO], str],
        Tuple[str, Tuple[None, str]]
    ]] = [
        ('workflowCwlFile', ('workflow.cwl', open(workflow_path, 'rb'))),
    ]

    # Check analysis storage
    if analysis_storage is None:
        analysis_storage: AnalysisStorageType = get_analysis_storage_from_analysis_storage_size(AnalysisStorageSize.SMALL)

    # Add params xml file to the file list
    if params_xml_file is None:
        params_xml_temp_file_obj = NamedTemporaryFile(prefix="params", suffix=".xml")
        params_xml_file = Path(params_xml_temp_file_obj.name)
        create_blank_params_xml(output_file_path=params_xml_file)
    file_list.append(
        ('parametersXmlFile', ('params.xml', open(params_xml_file, 'rb')))
    )

    # Add tool paths to the file list
    if tool_paths is not None:
        for tool_path in tool_paths:
            file_list.append(
                (
                    'toolCwlFiles',
                    (
                        str(
                            # Collect the tool path relative to the workflow path
                            tool_path.parent.absolute().resolve().relative_to(
                                workflow_path.parent.absolute().resolve()
                            ).joinpath(
                                tool_path.name
                            )
                        ),
                        open(tool_path, 'rb')
                    )
                )
            )

    # Add the html documentation file to the file list
    if workflow_html_documentation is not None:
        file_list.append(
            ('htmlDocumentation', (str(workflow_html_documentation), open(params_xml_file, 'rb')))
        )

    # Add code, description and analysis storage id to the forms
    file_list.append(('code', (None, pipeline_code)))
    file_list.append(('description', (None, workflow_description)))
    file_list.append(('analysisStorageId', (None, analysis_storage.id)))

    # Check response
    try:
        response = requests.post(
            headers={
                "Authorization": f"Bearer {configuration.access_token}",
                "Accept": "application/vnd.illumina.v3+json"
            },
            url=f"{configuration.host}/api/projects/{project_id}/pipelines:createCwlPipeline",
            files=file_list
        )
        response.raise_for_status()
    except HTTPError as err:
        logger.error(f"Failed to create the CWL pipeline, error: {err}")
        raise ApiException

    # Collect the id from the response json
    pipeline_id = response.json().get("pipeline").get("id")

    # Convert to a ProjectPipeline object
    return get_project_pipeline_obj(project_id=project_id, pipeline_id=pipeline_id)


def create_cwl_workflow_from_zip(
        project_id: str,
        pipeline_code: str,
        zip_path: Path,
        analysis_storage: Optional[AnalysisStorageType] = None,
        workflow_description: Optional[str] = None,
        html_documentation_path: Optional[Path] = None,
) -> ProjectPipeline:
    """
    Create a CWL project pipeline from a zip file containing the workflow and tools

    :param project_id:
    :param pipeline_code:
    :param zip_path:
    :param analysis_storage:
    :param workflow_description:
    :param html_documentation_path:
    :return:
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
            workflow_description = load_document_by_uri(workflow_path).get("doc", None)

        # Create the cwl workflow
        return create_cwl_project_pipeline(
            project_id=project_id,
            pipeline_code=pipeline_code,
            workflow_path=workflow_path,
            tool_paths=tool_paths,
            workflow_description=workflow_description,
            params_xml_file=params_xml_file,
            analysis_storage=analysis_storage,
            workflow_html_documentation=html_documentation_path
        )


def create_nextflow_pipeline_from_zip(
        project_id: str,
        pipeline_code: str,
        zip_path: Path,
        workflow_description: Optional[str] = None,
        html_documentation_path: Optional[Path] = None
) -> ProjectPipeline:
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

        # Get the params xml file
        params_xml_file_path = zip_dir / "params.xml"

        # Collect all other files
        local_workflow_and_module_paths = list(
            filter(
                lambda file_iter: (
                    file_iter.is_file() and
                    not (
                        file_iter == main_nf_path or
                        file_iter == config_file or
                        file_iter == params_xml_file_path
                    )
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
            workflow_html_documentation=html_documentation_path
        )


def create_nextflow_pipeline_from_nf_core_zip(
        project_id: str,
        pipeline_code: str,
        zip_path: Path,
        pipeline_revision: str,
        workflow_description: Optional[str] = None,
        html_documentation_path: Optional[Path] = None,
) -> ProjectPipeline:
    """
    Create a Nextflow project pipeline from a zip file containing the workflow and tools
    This function is designed for a user to generate an nf-core pipeline from a zip file containing the workflow and tools.

    :param pipeline_revision:
    :param project_id:
    :param pipeline_code:
    :param zip_path:
    :param workflow_description:
    :param html_documentation_path:

    :return: The nextflow pipeline
    :rtype: `ProjectPipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectPipeline/>`_

    :Examples:

    .. code-block:: python

        :linenos:

        # Bash Prestep
        # Create a nextflow pipeline from a zip file containing the workflow and tools
        # nf-core download bamtofastq --compress zip --outdir bamtofastq

        # Imports
        from wrapica.project_pipelines import create_nextflow_pipeline_from_zip

        # Set vars
        project_id = "project-123"
        pipeline_code = "pipeline-123"
        zip_path = Path("/path/to/bamtofastq.zip")
        workflow_description = "This is the nf-core pipeline for bamtofastq"

        # Create nextflow pipeline from zip
        nextflow_pipeline = create_nextflow_pipeline_from_zip(
            project_id, pipeline_code, zip_path, workflow_description
        )
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

    # Create the base icav2 configuration file
    base_config_file = Path(workflow_dir) / "conf" / "base.config"
    convert_base_config_to_icav2_base_config(base_config_file)

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
    write_params_xml_from_nextflow_schema_json(
        nextflow_schema_json_path=nextflow_schema_input_json_path,
        params_xml_path=params_xml_file_path,
        pipeline_name=pipeline_code.split("__")[0],
        pipeline_version=pipeline_code.split("__")[1]
    )

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
        html_documentation_path=html_documentation_path
    )


def create_nextflow_project_pipeline(
        project_id: str,
        pipeline_code: str,
        main_nextflow_file: Path,
        nextflow_config_file: Path,
        other_nextflow_files: List[Path],
        workflow_description: Optional[str] = None,
        params_xml_file: Optional[Path] = None,
        analysis_storage: Optional[AnalysisStorageType] = None,
        workflow_html_documentation: Optional[Path] = None,
):
    """
    Create a CWL project pipeline from a workflow path and tool paths

    :param project_id:
    :param pipeline_code:
    :param main_nextflow_file:
    :param nextflow_config_file:
    :param other_nextflow_files:
    :param workflow_description:
    :param params_xml_file:
    :param analysis_storage:
    :param workflow_html_documentation:
    :return:
    """
    # Get configuration
    configuration = get_icav2_configuration()

    # Initialise the file list with the workflow path
    file_list: List[Union[
        Tuple[str, Tuple[str, BinaryIO]],
        Tuple[str, Tuple[str, BinaryIO], str],
        Tuple[str, Tuple[None, str]]
    ]] = [
        ('mainNextflowFile', ('main.nf', open(main_nextflow_file, 'rb'))),
        ('nextflowConfigFile', ('nextflow.config', open(nextflow_config_file, 'rb')))
    ]

    # Check analysis storage
    if analysis_storage is None:
        analysis_storage: AnalysisStorageType = get_analysis_storage_from_analysis_storage_size(AnalysisStorageSize.SMALL)

    # Add params xml file to the file list
    if params_xml_file is None:
        params_xml_temp_file_obj = NamedTemporaryFile(prefix="params", suffix=".xml")
        params_xml_file = Path(params_xml_temp_file_obj.name)
        create_blank_params_xml(output_file_path=params_xml_file)
    file_list.append(
        ('parametersXmlFile', ('params.xml', open(params_xml_file, 'rb'))) #, 'text/xml')
    )

    # Add tool paths to the file list
    if other_nextflow_files is not None:
        for tool_path in other_nextflow_files:
            file_list.append(
                (
                    'otherNextflowFiles',
                    (
                        str(
                            # Collect the tool path relative to the workflow path
                            tool_path.parent.absolute().resolve().relative_to(
                                main_nextflow_file.parent.absolute().resolve()
                            ).joinpath(
                                tool_path.name
                            )
                        ),
                        open(tool_path, 'rb')
                    )
                )
            )

    # Add the html documentation file to the file list
    if workflow_html_documentation is not None:
        file_list.append(
            ('htmlDocumentation', (str(workflow_html_documentation), open(workflow_html_documentation, 'rb'))) #, 'text/html')
        )

    # Add code, description and analysis storage id to the forms
    file_list.append(('code', (None, pipeline_code)))
    file_list.append(('description', (None, workflow_description)))
    file_list.append(('analysisStorageId', (None, analysis_storage.id)))
    file_list.append(('pipelineLanguageVersionId', (None, NEXTFLOW_VERSION_UUID)))

    # Check response
    try:
        response: Response = requests.post(
            headers={
                "Authorization": f"Bearer {configuration.access_token}",
                "Accept": "application/vnd.illumina.v3+json"
            },
            url=f"{configuration.host}/api/projects/{project_id}/pipelines:createNextflowPipeline",
            files=file_list
        )
        response.raise_for_status()
    except HTTPError as err:
        logger.error(f"Failed to create the Nextflow pipeline, error: {err}")
        logger.error(response.json())
        raise ApiException

    # Collect the id from the response json
    pipeline_id = response.json().get("pipeline").get("id")

    # Convert to a ProjectPipeline object
    return get_project_pipeline_obj(project_id=project_id, pipeline_id=pipeline_id)
