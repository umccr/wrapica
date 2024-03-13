#!/usr/bin/env python3

"""
Project Pipeline Helpers
"""

# Standard imports
import typing
from typing import Tuple, Dict, Optional, Union, List
from urllib.parse import urlparse
import uuid
from pathlib import Path

# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.analysis_storage_api import AnalysisStorageApi
from libica.openapi.v2.api.entitlement_detail_api import EntitlementDetailApi
from libica.openapi.v2.api.project_analysis_api import ProjectAnalysisApi
from libica.openapi.v2.api.project_pipeline_api import ProjectPipelineApi
from libica.openapi.v2.model.activation_code_detail import ActivationCodeDetail
from libica.openapi.v2.model.analysis import Analysis
from libica.openapi.v2.model.analysis_input_data_mount import AnalysisInputDataMount
from libica.openapi.v2.model.analysis_input_external_data import AnalysisInputExternalData
from libica.openapi.v2.model.analysis_v3 import AnalysisV3
from libica.openapi.v2.model.create_cwl_analysis import CreateCwlAnalysis
from libica.openapi.v2.model.create_nextflow_analysis import CreateNextflowAnalysis
from libica.openapi.v2.model.cwl_analysis_json_input import CwlAnalysisJsonInput
from libica.openapi.v2.model.cwl_analysis_structured_input import CwlAnalysisStructuredInput
from libica.openapi.v2.model.input_parameter import InputParameter
from libica.openapi.v2.model.input_parameter_list import InputParameterList
from libica.openapi.v2.model.nextflow_analysis_input import NextflowAnalysisInput
from libica.openapi.v2.model.pipeline_configuration_parameter import PipelineConfigurationParameter
from libica.openapi.v2.model.pipeline_configuration_parameter_list import PipelineConfigurationParameterList
from libica.openapi.v2.model.project_data import ProjectData
from libica.openapi.v2.model.project_pipeline import ProjectPipeline
from libica.openapi.v2.model.project_pipeline_list import ProjectPipelineList
from libica.openapi.v2.model.search_matching_activation_codes_for_cwl_analysis import (
    SearchMatchingActivationCodesForCwlAnalysis
)
from libica.openapi.v2.model.search_matching_activation_codes_for_nextflow_analysis import (
    SearchMatchingActivationCodesForNextflowAnalysis
)

# Local imports
from ...utils import recursively_build_open_api_body_from_libica_item
from ...utils.logger import get_logger
from ...utils.configuration import get_icav2_configuration
from ...enums import AnalysisStorageSize, WorkflowLanguage, DataType


if typing.TYPE_CHECKING:
    # Import type hints for IDE only, not at runtime
    # Prevents circular imports
    from ..classes.cwl_analysis import ICAv2CWLPipelineAnalysis
    from ..classes.analysis import ICAv2PipelineAnalysisTags


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
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectPipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a list of project pipelines.
        api_response: ProjectPipelineList = api_instance.get_project_pipelines(project_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->get_project_pipelines: %s\n" % e)
        raise ValueError

    try:
        return next(
            filter(
                lambda project_pipeline_iter: project_pipeline_iter.pipeline.code == pipeline_code,
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find pipeline '{pipeline_code}' in project {project_id}")
        raise ValueError


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
    return project_pipeline_obj.pipeline.analysis_storage.id


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


def get_activation_id(
    project_id: str,
    pipeline_id: str,
    analysis_input: Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput, NextflowAnalysisInput],
    workflow_language: WorkflowLanguage
):

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
    output_parent_folder_id: Optional[str] = None,
    output_parent_folder_path: Optional[str] = None,
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
    :param output_parent_folder_id: The output parent folder id to use
        (can use output_parent_folder_path or analysis_output_uri instead)
    :param output_parent_folder_path: The output parent folder path to use
        (can use output_parent_folder_id or analysis_output_uri instead)
    :param analysis_output_uri: The analysis output uri to use
        (can use output_parent_folder_id or output_parent_folder_path instead)
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
        output_parent_folder_id=output_parent_folder_id,
        output_parent_folder_path=output_parent_folder_path,
        analysis_output_uri=analysis_output_uri,
        tags=tags,
        cwltool_overrides=cwltool_overrides
    )

    return cwl_analysis


def launch_cwl_workflow(project_id: str, cwl_analysis: CreateCwlAnalysis) -> Analysis:
    """
    Launch a CWL Workflow in a specific project context

    :param project_id: The project id to launch the CWL workflow in
    :param cwl_analysis: The CWL analysis object to launch

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
        # Force default headers to v3
        # FIXME https://github.com/umccr-illumina/ica_v2/issues/173
        api_client.set_default_header(
            header_name="Content-Type",
            header_value="application/vnd.illumina.v3+json"
        )
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )

        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

        # override endpoint settings response type to the version we want i.e. AnalysisV3 or Analysis
        endpoint_settings = api_instance.create_cwl_analysis_endpoint.settings
        endpoint_settings['response_type'] = (AnalysisV3,)

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a CWL pipeline.
        api_response: Analysis = api_instance.create_cwl_analysis(
            project_id,
            cwl_analysis
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->create_cwl_analysis: %s\n" % e)
        raise ApiException

    return api_response


def launch_nextflow_workflow(project_id: str, nextflow_analysis: CreateNextflowAnalysis) -> AnalysisV3:
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
        # Force default headers to v3
        # FIXME https://github.com/umccr-illumina/ica_v2/issues/173
        api_client.set_default_header(
            header_name="Content-Type",
            header_value="application/vnd.illumina.v3+json"
        )
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )

        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

        # override endpoint settings response type to the version we want i.e. AnalysisV3 or Analysis
        endpoint_settings = api_instance.create_nextflow_analysis_endpoint.settings
        endpoint_settings['response_type'] = (AnalysisV3,)

    # example passing only required values which don't have defaults set
    try:
        # Create and start an analysis for a CWL pipeline.
        api_response: AnalysisV3 = api_instance.create_nextflow_analysis(
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
            api_response: InputParameterList = api_instance.get_project_pipeline_input_parameters(project_id, pipeline_id)
        except ApiException as e:
            logger.error("Exception when calling ProjectPipelineApi->get_project_pipeline_input_parameters: %s\n" % e)
            raise ApiException

    return api_response.items


def get_project_pipeline_configuration_parameters(
    project_id: str,
    pipeline_id: str
) -> List[PipelineConfigurationParameter]:
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
                project_id, pipeline_id
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
        convert_icav2_uri_to_data_obj,
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
            input_obj_new_item, mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(input_item)
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
                input_obj_new: ProjectData = convert_icav2_uri_to_data_obj(input_obj.get("location"))
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
                        input_obj["basename"] = get_project_data_obj_by_id(owning_project_id, data_id).data.details.name
                        input_obj["listing"] = presign_cwl_directory(
                            owning_project_id, data_id
                        )
                elif is_presign and is_stage:
                    if data_type == DataType.FILE:
                        input_obj["location"] = mount_path
                        external_data_list.append(
                            AnalysisInputExternalData(
                                url=create_download_url(owning_project_id, data_id),
                                location=input_obj.get("location")
                            )
                        )
                    elif data_type == DataType.FOLDER:
                        input_obj["location"] = mount_path
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
                            path=mount_path
                        )
                    )

            # Get secondary Files
            if not len(input_obj.get("secondaryFiles", [])) == 0:
                old_secondary_files = input_obj.get("secondaryFiles", [])
                input_obj["secondaryFiles"] = []
                for input_item in old_secondary_files:
                    input_obj_new_item, mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(input_item)
                    input_obj["secondaryFiles"].append(input_obj_new_item)
                    mount_list.extend(mount_list_new)
                    external_data_list.extend(external_data_list_new)

            return input_obj, mount_list, external_data_list
        else:
            input_obj_dict = {}
            for key, value in input_obj.items():
                input_obj_dict[key], mount_list_new, external_data_list_new = convert_icav2_uris_to_data_ids_from_cwl_input_json(value)
                mount_list.extend(mount_list_new)
                external_data_list.extend(external_data_list_new)
            return input_obj_dict, mount_list, external_data_list
