#!/usr/bin/env python

"""
Project Analysis Functions
"""

# Standard imports
from __future__ import annotations
import json
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, Union, Dict, Any, Optional, cast
import re

# Libica apis
from libica.openapi.v3.api.project_analysis_api import ProjectAnalysisApi
from libica.openapi.v3.api_client import ApiClient
from libica.openapi.v3.exceptions import ApiException
from pydantic import UUID4

# Wrapica imports
from ...literals import (
    ProjectAnalysisStatusType,
    ProjectAnalysisSortParametersType,
    AnalysisTagType,
    AnalysisStepDict,
    ProjectAnalysisStepStatusType,
    AnalysisLogStreamNameType,
)

# Libica models
from libica.openapi.v3.models import (
    AnalysisQueryParameters,
    AnalysisV3,
    AnalysisV4,
    AnalysisInput,
    AnalysisOutput,
    AnalysisOutputList,
    AnalysisStep,
    AnalysisStepLogs,
    CwlAnalysisInputJson,
    CwlAnalysisOutputJson,
)

# Local imports
from ...utils.globals import LIBICAV2_DEFAULT_PAGE_SIZE, IS_REGEX_MATCH
from ...utils.configuration import get_icav2_configuration
from ...utils.miscell import is_uuid_format, coerce_to_uuid4_obj
from ...utils.websocket_helpers import write_websocket_to_file, convert_html_to_text
from ...utils.logger import get_logger

AnalysisType = Union[AnalysisV3, AnalysisV4]

logger = get_logger()


def get_project_analysis_inputs(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
) -> List[AnalysisInput]:
    """
    Get the analysis inputs for a given analysis

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: List of analysis inputs
    :rtype: List[`AnalysisInput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInput>`_]

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
                project_id=str(project_id),
                analysis_id=str(analysis_id)
            ).items
        except ApiException as e:
            logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
            raise ApiException

    return analysis_input_list


def get_analysis_input_object_from_analysis_input_code(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
    analysis_input_code: str
) -> AnalysisInput:
    """
    Given an analysis input code for an analysis id, collect the analysis input object
    Confirm the input has either analysis or external data attributes

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query
    :param analysis_input_code: The analysis input code to query

    :return: The analysis input object
    :rtype: `AnalysisInput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInput>`_

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
        project_id=project_id,
        analysis_id=analysis_id
    )

    # Iterate through inputs to find the one we want
    try:
        input_obj: AnalysisInput = next(
            filter(
                lambda analysis_input_iter: analysis_input_iter.code == analysis_input_code,
                analysis_input_list
            )
        )
    except StopIteration:
        logger.error(f"Could not get {analysis_input_code} from analysis {analysis_id}")
        raise StopIteration

    if len(input_obj.analysis_data) == 0 and len(input_obj.external_data) == 0:
        logger.error(f"Expected analysis data or external data to be 1 but got {len(input_obj.analysis_data)}")
        raise ValueError

    return input_obj


def get_outputs_object_from_analysis_id(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
) -> List[AnalysisOutput]:
    """
    Query the outputs object from the analysis id

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: List of analysis outputs
    :rtype: List[`AnalysisOutput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisOutput>`_]

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
        api_instance = ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the outputs of an analysis
        api_response: AnalysisOutputList = api_instance.get_analysis_outputs(
            project_id=str(project_id),
            analysis_id=str(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
        raise ApiException

    return api_response.items


def get_analysis_output_object_from_analysis_output_code(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
    analysis_output_code: str
) -> AnalysisOutput:
    """
    Given an analysis code for an analysis id, collect the analysis output object

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query
    :param analysis_output_code: The analysis output code to collect

    :return: The analysis output object
    :rtype: `AnalysisOutput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisOutput>`_

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
                lambda analysis_iter: analysis_iter.code == analysis_output_code,
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
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
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
        api_instance = ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the outputs of an analysis
        api_response: CwlAnalysisOutputJson = api_instance.get_cwl_output_json(
            project_id=str(project_id),
            analysis_id=coerce_to_uuid4_obj(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_outputs: %s\n" % e)
        raise ApiException

    return json.loads(api_response.output_json)


def get_analysis_obj_from_analysis_id(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
) -> AnalysisV4:
    """
    Get an analysis object given a project id and analysis id

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis id to query

    :return: The analysis object
    :rtype: `Analysis <https://umccr.github.io/libica/openapi/v3/docs/Analysis>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import get_analysis_obj_from_analysis_id

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        analysis = get_analysis_obj_from_analysis_id(project_id, analysis_id)
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Set as V4 headers
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

    # example passing only required values which don't have defaults set
    try:
        # Retrieve an analysis.
        api_response: AnalysisV4 = api_instance.get_analysis(
            project_id=str(project_id),
            analysis_id=str(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis: %s\n" % e)
        raise ApiException

    return api_response


def get_analysis_steps(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
    include_technical_steps: bool = False
) -> List[AnalysisStep]:
    """
    Get the workflow steps for a given analysis

    :param project_id:
    :param analysis_id:
    :param include_technical_steps:

    :return: List of analysis steps
    :rtype: List[`AnalysisStep <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStep>`_]

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
        api_instance = ProjectAnalysisApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the individual steps of an analysis.
        api_response = api_instance.get_analysis_steps(
            project_id=str(project_id),
            analysis_id=str(analysis_id)
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


def get_analysis_log_from_analysis_step(
    analysis_step: AnalysisStep
) -> AnalysisStepLogs:
    """
    Get the logs for a given analysis step

    :param analysis_step:

    :return: Get the logs attribute of an analysis step
    :rtype: `AnalysisStepLogs <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStepLogs>`_

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
    project_id: Union[UUID4, str],
    step_logs: AnalysisStepLogs,
    log_name: AnalysisLogStreamNameType,
    output_path: Union[Path | TextIOWrapper],
    is_cwltool_log: Optional[bool] = False
) -> None:
    """
    Write the analysis step logs to a file

    :param project_id: The project id the analysis was run in
    :param step_logs: Required, the step logs object
    :param log_name: Required, One of stdout or stderr
    :param output_path: Required, The output path to write the file to
    :param is_cwltool_log: Required, If the log is a cwltool log we convert from html to text format

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

    if log_name == "stderr":
        if hasattr(step_logs, "std_out_stream") and step_logs.std_out_stream is not None:
            is_stream = True
            log_stream = step_logs.std_out_stream
        elif hasattr(step_logs, "std_out_data") and step_logs.std_out_data is not None:
            log_data_id: str = str(step_logs.std_out_data.id)
        else:
            logger.error("Could not get either file output or stream of logs")
            logger.error(f"The available attributes were {', '.join(non_empty_log_attrs)}")
            raise AttributeError
    else:
        if hasattr(step_logs, "std_err_stream") and step_logs.std_err_stream is not None:
            is_stream = True
            log_stream = step_logs.std_err_stream
        elif hasattr(step_logs, "std_err_data") and step_logs.std_err_data is not None:
            log_data_id: str = str(step_logs.std_err_data.id)
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
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
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
        # Force default headers for endpoints with a ':' in the name
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

    # example passing only required values which don't have defaults set
    try:
        # Abort an analysis.
        api_instance.abort_analysis(
            project_id=str(project_id),
            analysis_id=str(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->abort_analysis: %s\n" % e)
        raise ApiException


def list_analyses(
    project_id: Union[UUID4, str],
    pipeline_id: Optional[Union[UUID4, str]] = None,
    user_reference: Optional[str] = None,
    status: Optional[Union[ProjectAnalysisStatusType, List[ProjectAnalysisStatusType]]] = None,
    creation_date_before: Optional[datetime] = None,
    creation_date_after: Optional[datetime] = None,
    modification_date_before: Optional[datetime] = None,
    modification_date_after: Optional[datetime] = None,
    sort: Optional[Union[ProjectAnalysisSortParametersType, List[ProjectAnalysisSortParametersType]]] = None,
    max_items: Optional[int] = None
) -> List[AnalysisV4]:
    """
    List analyses

    :param project_id: The project id
    :param pipeline_id: The pipeline id
    :param user_reference: The full user reference (regex optional)
    :param status: The status of the analysis
    :param creation_date_before: Return only analyses created before this date
    :param creation_date_after: Return only analyses created after this date
    :param modification_date_before: Return only analyses modified before this date
    :param modification_date_after: Return only analyses modified after this date
    :param sort: A parameter, or list of parameters to sort by
    :param max_items:

    :raises: ApiException

    :return: List of analyses
    :rtype: `List[Analysis] <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :Examples:

    .. code-block:: python

        :linenos:
        from pathlib import Path
        from wrapica.project_analysis import list_analyses

        # Set params
        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id
        project_id = "project_id"

        analysis_list = list_analyses(project_id)
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    # Check parameters
    if user_reference is not None and IS_REGEX_MATCH.search(user_reference) is not None:
        user_reference_regex = re.compile(user_reference)
        user_reference = None
    else:
        user_reference_regex = None

    if status is not None and not isinstance(status, list):
        status = [status]

    if sort is not None:
        if not isinstance(sort, list):
            sort = [sort]

        # Complete a comma join of the sort parameters
        sort = ",".join(sort)

    # AnalysisQueryParameters
    analysis_query_parameters = {
        "status": status if status is not None else None,
        "user_reference": user_reference,
    }
    analysis_query_parameters = AnalysisQueryParameters(
        # Filter out None values and parse into query parameters
        **dict(filter(
            lambda kv_iter_: kv_iter_[1] is not None,
            analysis_query_parameters.items()
        ))
    )

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    # Set page parameters
    if max_items is None:
        max_items = 0
    if not max_items == 0 and max_items < LIBICAV2_DEFAULT_PAGE_SIZE:
        page_size = max_items
    else:
        page_size = LIBICAV2_DEFAULT_PAGE_SIZE
    page_token = ""
    # We use page tokens if sort is None, otherwise we use page offsets
    if sort is not None:
        page_offset = 0
    else:
        page_offset = ""

    # Initialise list
    analysis_list: List[AnalysisV4] = []

    # Loop through the pages
    while True:
        # Attempt to collect all analyses
        try:
            api_response = api_instance.search_analyses(
                **dict(
                    filter(
                        lambda x: x[1] is not None,
                        {
                            "project_id": str(project_id),
                            "page_size": str(page_size),
                            "page_offset": str(page_offset),
                            "page_token": page_token,
                            "analysis_query_parameters": analysis_query_parameters,
                            "sort": sort
                        }.items()
                    )
                )
            )
        except ApiException as e:
            raise ValueError("Exception when calling ProjectAnalysisApi->get_project_data_list: %s\n" % e)

        # Extend items list
        analysis_list.extend(api_response.items)

        # Determine page iteration method by if we have a 'sort' parameter
        if sort is not None:
            # Check page offset and page size against total item count
            if page_offset + page_size >= api_response.total_item_count:
                break
            if not max_items == 0 and len(analysis_list) >= max_items:
                break

            # Continuing iteration
            page_offset += page_size
            # Add page size to page offset
            # But check if we're approaching the max_items
            if not max_items == 0 and len(analysis_list) + page_size > max_items:
                page_size = max_items - len(analysis_list)
        else:
            # Check if there is a next page
            if api_response.next_page_token is None or api_response.next_page_token == "":
                break
            page_token = api_response.next_page_token

    # Before we return the list, we filter
    # pipeline_id
    # creation_date_after
    # creation_date_before
    # modification_date_after
    # modification_date_before
    analysis_list = list(
        filter(
            lambda analysis_iter: (
                (
                    pipeline_id is None or
                    str(analysis_iter.pipeline.id) == str(pipeline_id)
                ) and
                (
                    user_reference_regex is None or
                    user_reference_regex.match(analysis_iter.user_reference) is not None
                ) and
                (
                    creation_date_after is None or
                    analysis_iter.creation_date >= creation_date_after
                ) and
                (
                    creation_date_before is None or
                    analysis_iter.creation_date <= creation_date_before
                ) and
                (
                    modification_date_after is None or
                    analysis_iter.modification_date >= modification_date_after
                ) and
                (
                    modification_date_before is None or
                    analysis_iter.modification_date <= modification_date_before
                )
            ),
            analysis_list
        )
    )

    return analysis_list


def get_cwl_analysis_input_json(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
) -> Dict:
    """
    Get the CWL Analysis Input JSON

    :param project_id: The project id the analysis was run in
    :param analysis_id:  The analysis id

    :return: The CWL Analysis Input JSON As a dictionary
    :rtype: Dict

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        import json
        from wrapica.project_analysis import get_cwl_analysis_input_json

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        # Print the input json
        print(
            json.dumps(
                get_cwl_analysis_input_json(project_id, analysis_id),
                indent=4
            )
        )

    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    try:
        # Retrieve the input json of a CWL analysis.
        api_response: CwlAnalysisInputJson = api_instance.get_cwl_input_json(
            project_id=str(project_id),
            analysis_id=coerce_to_uuid4_obj(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_cwl_input_json: %s\n" % e)
        raise ApiException

    return json.loads(api_response.input_json)


def get_cwl_analysis_output_json(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str]
) -> Dict:
    """
    Get the CWL Analysis Input JSON

    :param project_id: The project id the analysis was run in
    :param analysis_id: The analysis id

    :return: The CWL Analysis Output JSON As a dictionary
    :rtype: Dict

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        import json
        from wrapica.project_analysis import get_cwl_analysis_output_json

        # Set params
        project_id = "project_id"
        analysis_id = "analysis_id"

        # Print the output json
        print(
            json.dumps(
                get_cwl_analysis_output_json(project_id, analysis_id),
                indent=4
            )
        )

    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    try:
        # Retrieve the input json of a CWL analysis.
        api_response: CwlAnalysisOutputJson = api_instance.get_cwl_output_json(
            project_id=str(project_id),
            analysis_id=coerce_to_uuid4_obj(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_cwl_output_json: %s\n" % e)
        raise ApiException

    return json.loads(api_response.output_json)


def analysis_step_to_dict(analysis_step: AnalysisStep) -> AnalysisStepDict:
    """
    Convert an analysis step object to a dictionary

    Returns the following attributes
    * name: str -> remove preceding '#' values
    * status: WorkflowStep.Status -> The workflow step status
    * queue_date: Datetime object
    * start_date: Datetime object
    * end_date: Datetime object

    :param analysis_step: The AnalysisStep object
    :return: The analysis step as a dict
    :rtype: Dict
    """
    return {
        "name": analysis_step.name.split("#", 1)[-1],
        "status": cast(ProjectAnalysisStepStatusType, analysis_step.status),
        "queue_date": analysis_step.queue_date if hasattr(analysis_step, "queue_date") else None,
        "start_date": analysis_step.start_date if hasattr(analysis_step, "start_date") else None,
        "end_date": analysis_step.end_date if hasattr(analysis_step, "end_date") else None
    }


def get_analysis_obj_from_user_reference(
    project_id: Union[UUID4, str],
    user_reference: str
) -> AnalysisType:
    """
    Given a user reference, get the analysis object

    Will fail if more than one analysis is found for a given user reference.
    Will also fail if no analysis is found for the user reference.

    :param project_id:
    :param user_reference:
    :return:
    """

    # List analysis filtering on the user reference
    # List analysis filtering on the user reference
    analysis_list = list_analyses(
        project_id=project_id,
        user_reference=user_reference
    )

    if len(analysis_list) == 0:
        logger.error("Could not find analysis id from user reference")
        raise ValueError

    if not len(analysis_list) == 1:
        logger.error(
            f"Got {len(analysis_list)} analyses from user reference, "
            f"cannot coerce user reference to analysis id"
        )
        raise ValueError

    return analysis_list[0]


def coerce_analysis_id_or_user_reference_to_analysis_obj(
    project_id: Union[UUID4, str],
    analysis_id_or_user_reference: Union[Union[UUID4, str], str]
) -> AnalysisType:
    """
    Given either an analysis id or user reference, coerce to an analysis object

    :param project_id:
    :param analysis_id_or_user_reference:
    :return: The analysis object
    :rtype: `Analysis <https://umccr.github.io/libica/openapi/v3/docs/Analysis>`_

    :raises: ValueError

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_analysis import coerce_analysis_id_or_user_reference_to_analysis_obj

        # Set params
        analysis_id_or_user_reference = "analysis_id"

        analysis = coerce_analysis_id_or_user_reference_to_analysis_obj(analysis_id_or_user_reference)
    """

    # For an analysis id
    if is_uuid_format(analysis_id_or_user_reference):
        return get_analysis_obj_from_analysis_id(
            project_id=project_id,
            analysis_id=analysis_id_or_user_reference
        )

    return get_analysis_obj_from_user_reference(
        project_id=project_id,
        user_reference=analysis_id_or_user_reference
    )


def coerce_analysis_id_or_user_reference_to_analysis_id(
    project_id: Union[UUID4, str],
    analysis_id_or_user_reference: str
) -> str:
    """
    Given either an analysis id or user reference, coerce to an analysis id

    :param project_id:
    :param analysis_id_or_user_reference:
    :return:
    """

    if is_uuid_format(analysis_id_or_user_reference):
        return analysis_id_or_user_reference

    return str(
        get_analysis_obj_from_user_reference(
            project_id=project_id,
            user_reference=analysis_id_or_user_reference
        ).id
    )


def update_analysis_obj(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
    analysis_obj: AnalysisType
) -> AnalysisType:
    """
    Given an analysis id, update the analysis object
    :param project_id:
    :param analysis_id:
    :param analysis_obj:
    :return:
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    try:
        # Retrieve the input json of a CWL analysis.
        api_response: AnalysisType = api_instance.update_analysis(
            project_id=str(project_id),
            analysis_id=str(analysis_id),
            analysis_v4=(
                analysis_obj
                if isinstance(analysis_obj, AnalysisV4)
                else get_analysis_obj_from_analysis_id(
                    project_id=project_id,
                    analysis_id=analysis_id
                )
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->update_analysis: %s\n" % e)
        raise e

    return api_response


def add_tag_to_analysis(
    project_id: Union[UUID4, str],
    analysis_id: Union[UUID4, str],
    tag: str,
    tag_type: AnalysisTagType
):
    # Check project id and analysis id are in uuid formats
    if not is_uuid_format(project_id):
        raise ValueError("Project id is not in UUID format")

    if not is_uuid_format(analysis_id):
        raise ValueError("Analysis id is not in UUID format")

    # Get the current analysis object
    analysis_obj: AnalysisType = get_analysis_obj_from_analysis_id(
        project_id=project_id,
        analysis_id=analysis_id
    )

    if tag_type == "user_tag":
        analysis_obj.tags.user_tags.append(tag)
    elif tag_type == "technical_tag":
        analysis_obj.tags.technical_tags.append(tag)
    elif tag_type == "reference_tag":
        analysis_obj.tags.reference_tags.append(tag)
    else:
        raise ValueError("Tag type not recognised")

    # Update the analysis object
    analysis_obj = update_analysis_obj(
        project_id=project_id,
        analysis_id=analysis_id,
        analysis_obj=analysis_obj
    )

    return analysis_obj


