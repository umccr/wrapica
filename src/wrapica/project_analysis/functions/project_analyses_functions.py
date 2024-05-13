#!/usr/bin/env python

# Standard imports
import json
from io import TextIOWrapper
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, Union, Dict, Any, Optional

# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.project_analysis_api import ProjectAnalysisApi
from libica.openapi.v2.model.analysis_v3 import AnalysisV3
from libica.openapi.v2.model.analysis_v4 import AnalysisV4
from libica.openapi.v2.model.analysis_input import AnalysisInput
from libica.openapi.v2.model.analysis_output import AnalysisOutput
from libica.openapi.v2.model.analysis_output_list import AnalysisOutputList
from libica.openapi.v2.model.analysis_step import AnalysisStep
from libica.openapi.v2.model.analysis_step_logs import AnalysisStepLogs
from libica.openapi.v2.model.cwl_analysis_output_json import CwlAnalysisOutputJson
from libica.openapi.v2.api import project_analysis_api

# Util imports
from ...utils.configuration import get_icav2_configuration
from ...utils.websocket_helpers import write_websocket_to_file, convert_html_to_text
from ...utils.logger import get_logger

Analysis = Union[AnalysisV3, AnalysisV4]

logger = get_logger()


def get_project_analysis_inputs(
    project_id: str,
    analysis_id: str
) -> List[AnalysisInput]:
    """
    Get the analysis inputs for a given analysis

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: List of analysis inputs
    :rtype: List[`AnalysisInput <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisInput>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_project_analysis_inputs

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        workflow_inputs = get_project_analysis_inputs(project_id, analysis_id)

    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

        # example passing only required values which don't have defaults set
        try:
            # Retrieve the outputs of an analysis.
            analysis_input_list: List[AnalysisInput] = api_instance.get_analysis_inputs(
                project_id, analysis_id
            ).items
        except ApiException as e:
            logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
            raise ApiException

    return analysis_input_list


def get_analysis_input_object_from_analysis_code(
    project_id: str,
    analysis_id: str,
    analysis_code: str
) -> AnalysisInput:
    """
    Given an analysis code for an analysis id, collect the analysis input object
    Confirm the input has either analysis or external data attributes

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query
    :param analysis_code: The analysis input code to query

    :return: The analysis input object
    :rtype: `AnalysisInput <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisInput>`_

    :raises: StopIteration, ValueError, ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_input_object_from_analysis_code

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"
        analysis_code = "run_folder"

        run_folder_input_data_id = get_analysis_input_object_from_analysis_code(
            project_id, analysis_id, analysis_code
        ).analysis_data[0].data_id
    """

    # Get analysis inputs
    analysis_input_list: List[AnalysisInput] = get_project_analysis_inputs(
        project_id,
        analysis_id
    )

    # Iterate through inputs to find the one we want
    try:
        input_obj: AnalysisInput = next(
            filter(
                lambda analysis_iter: analysis_iter.code == analysis_code,
                analysis_input_list
            )
        )
    except StopIteration:
        logger.error(f"Could not get {analysis_code} from analysis {analysis_id}")
        raise StopIteration

    if len(input_obj.analysis_data) == 0 and len(input_obj.external_data) == 0:
        logger.error(f"Expected analysis data or external data to be 1 but got {len(input_obj.analysis_data)}")
        raise ValueError

    return input_obj


def get_outputs_object_from_analysis_id(
    project_id: str,
    analysis_id: str
) -> List[AnalysisOutput]:
    """
    Query the outputs object from the analysis id

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: List of analysis outputs
    :rtype: List[`AnalysisOutput <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisOutput>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
    
        :linenos:
        from wrapica.project_analysis import get_outputs_object_from_analysis_id

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        workflow_outputs = get_outputs_object_from_analysis_id(project_id, analysis_id)
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the outputs of an analysis
        api_response: AnalysisOutputList = api_instance.get_analysis_outputs(project_id, analysis_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
        raise ApiException

    return api_response.items


def get_analysis_output_object_from_analysis_code(
    project_id: str,
    analysis_id: str,
    analysis_code: str
) -> AnalysisOutput:
    """
    Given an analysis code for an analysis id, collect the analysis output object

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query
    :param analysis_code: The analysis output code to collect

    :return: The analysis output object
    :rtype: `AnalysisOutput <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisOutput>`_

    :raises: StopIteration, ValueError, ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_output_object_from_analysis_code

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"
        analysis_code = "Output"

        analysis_folder_output_id = get_analysis_output_object_from_analysis_code(
            project_id, analysis_id, analysis_code
        ).data[0].data_id
    """

    analysis_output: List[AnalysisOutput] = get_outputs_object_from_analysis_id(
        project_id,
        analysis_id
    )
    try:
        output_obj: AnalysisOutput = next(
            filter(
                lambda analysis_iter: analysis_iter.code == analysis_code,
                analysis_output
            )
        )
    except StopIteration:
        logger.error(f"Could not get output item from analysis {analysis_id}")
        raise StopIteration

    if len(output_obj.data) == 0:
        logger.error(f"Expected analysis output data to be at least 1 but got {len(output_obj.data)}")
        raise ValueError

    return output_obj


def get_cwl_outputs_json_from_analysis_id(
    project_id: str,
    analysis_id: str
) -> Dict[str, Any]:
    """
    Query the outputs object from the analysis id

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: List of analysis outputs
    :rtype: Dict[str, Any]

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_cwl_outputs_json_from_analysis_id

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        cwl_json_output = get_cwl_outputs_json_from_analysis_id(project_id, analysis_id)
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the outputs of an analysis
        api_response: CwlAnalysisOutputJson = api_instance.get_cwl_output_json(
            project_id, analysis_id
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
        raise ApiException

    return json.loads(api_response.output_json)


def get_analysis_obj(
    project_id: str,
    analysis_id: str
) -> Analysis:
    """
    Get an analysis object given a project id and analysis id

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: The analysis object
    :rtype: `Analysis <https://umccr-illumina.github.io/libica/openapi/v2/docs/Analysis>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_obj

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        analysis = get_analysis_obj(project_id, analysis_id)
    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve an analysis.
        api_response: Analysis = api_instance.get_analysis(project_id, analysis_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis: %s\n" % e)
        raise ApiException

    return api_response


def get_analysis_steps(project_id: str, analysis_id: str, include_technical_steps: bool = False) -> List[AnalysisStep]:
    """
    Get the workflow steps for a given analysis

    :param project_id:
    :param analysis_id:

    :return: List of analysis steps
    :rtype: List[`AnalysisStep <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisStep>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_steps

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        analysis_step_list = get_analysis_steps(project_id, analysis_id)
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the individual steps of an analysis.
        api_response = api_instance.get_analysis_steps(
            project_id,
            analysis_id
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_steps: %s\n" % e)
        raise ApiException

    # Collect all steps
    analysis_steps: List[AnalysisStep] = api_response.items

    if not include_technical_steps:
        analysis_steps = list(
            filter(
                lambda step_iter: step_iter.technical is False,
                analysis_steps
            )
        )

    return analysis_steps


def get_analysis_log_from_analysis_step(analysis_step: AnalysisStep) -> AnalysisStepLogs:
    """
    Get the logs for a given analysis step

    :param analysis_step:

    :return: Get the logs attribute of an analysis step
    :rtype: `AnalysisStepLogs <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisStepLogs>`_

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_steps

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        analysis_step_list = get_analysis_steps(project_id, analysis_id)

        step_logs = list(map(get_analysis_log_from_analysis_step, analysis_step_list))
    """
    return analysis_step.logs


def write_analysis_step_logs(
    project_id: str,
    step_logs: AnalysisStepLogs,
    log_name: str,
    output_path: Union[Path | TextIOWrapper],
    is_cwltool_log: Optional[bool] = False
) -> None:
    """
    Write the analysis step logs to a file

    :param project_id: The project id the analysis was run in
    :param step_logs: The step logs object
    :param log_name: One of stdout or stderr
    :param output_path: The output path to write the file to

    :raises: ApiException, NotADirectoryError

    :return: None
    :rtype: None

    :Examples:

    .. code-block:: python

        :linenos:
        from pathlib import Path
        from wrapica.project_analysis import get_analysis_steps

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        # Get analysis steps and logs
        analysis_step_list = get_analysis_steps(project_id, analysis_id)
        step_logs = list(map(get_analysis_log_from_analysis_step, analysis_step_list))

        # Write first step logs to file
        write_analysis_step_logs(
            project_id=project_id,
            step_logs=step_logs[0],
            log_name="stderr",
            output_path=Path("stderr.log")
        )

    """
    # Local imports
    from ...project_data import read_icav2_file_contents

    # Check if we're getting our log from a stream
    is_stream = False
    log_stream = None
    log_data_id = ""

    # Initialise list of non empty log attributes
    non_empty_log_attrs = []

    # Check attributes of log obj
    for attr in dir(step_logs):
        if attr.startswith('_'):
            continue
        if getattr(step_logs, attr) is None:
            continue
        non_empty_log_attrs.append(attr)

    if log_name == "stdout":
        if hasattr(step_logs, "std_out_stream") and step_logs.std_out_stream is not None:
            is_stream = True
            log_stream = step_logs.std_out_stream
        elif hasattr(step_logs, "std_out_data") and step_logs.std_out_data is not None:
            log_data_id: str = step_logs.std_out_data.id
        else:
            logger.error("Could not get either file output or stream of logs")
            logger.error(f"The available attributes were {', '.join(non_empty_log_attrs)}")
            raise AttributeError
    else:
        if hasattr(step_logs, "std_err_stream") and step_logs.std_err_stream is not None:
            is_stream = True
            log_stream = step_logs.std_err_stream
        elif hasattr(step_logs, "std_err_data") and step_logs.std_err_data is not None:
            log_data_id: str = step_logs.std_err_data.id
        else:
            logger.error("Could not get either file output or stream of logs")
            logger.error(f"The available attributes were {', '.join(non_empty_log_attrs)}")
            raise AttributeError
    if is_stream:
        if is_cwltool_log:
            temp_html_obj = NamedTemporaryFile()
            write_websocket_to_file(
                log_stream,
                output_file=Path(temp_html_obj.name)
            )
            convert_html_to_text(Path(temp_html_obj.name), output_path)
        else:
            write_websocket_to_file(
                log_stream,
                output_file=output_path
            )
    else:
        read_icav2_file_contents(project_id, log_data_id, output_path)


def abort_analysis(
    project_id: str,
    analysis_id: str,
) -> None:
    """
    Abort an analysis

    :param project_id: The project id the analysis was run in
    :param analysis_id: The analysis id to abort

    :raises: ApiException

    :return: None
    :rtype: None

    :Examples:

    .. code-block:: python

        :linenos:
        from pathlib import Path
        from wrapica.project_analysis import abort_analysis

        # Set params
        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id
        project_id = "project_id"
        analysis_id = "analysis_id"
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Abort an analysis.
        api_instance.abort_analysis(project_id, analysis_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->abort_analysis: %s\n" % e)
        raise ApiException
