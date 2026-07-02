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
    AnalysisUsageDetails
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
    Return the analysis inputs for a given analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query

    :return: The list of analysis input objects
    :rtype: List[`AnalysisInput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInput/>`_]

    :raises ApiException: If the API call to retrieve analysis inputs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_project_analysis_inputs

        inputs = get_project_analysis_inputs(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(f"Found {len(inputs)} input(s)")
        # Found 3 input(s)
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
    Return the analysis input object matching the given input code.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query
    :param analysis_input_code: The input code to match against analysis inputs

    :return: The analysis input object for the matching code
    :rtype: `AnalysisInput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInput/>`_

    :raises StopIteration: If no input matches the given analysis input code
    :raises ValueError: If the matched input has no analysis data or external data
    :raises ApiException: If the API call to retrieve analysis inputs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_input_object_from_analysis_input_code

        input_obj = get_analysis_input_object_from_analysis_input_code(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678",
            analysis_input_code="run_folder"
        )

        print(f"Input code: {input_obj.code}")
        # Input code: run_folder
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
    Return the list of output objects for a given analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query

    :return: The list of analysis output objects
    :rtype: List[`AnalysisOutput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisOutput/>`_]

    :raises ApiException: If the API call to retrieve analysis outputs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_outputs_object_from_analysis_id

        outputs = get_outputs_object_from_analysis_id(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(f"Found {len(outputs)} output(s)")
        # Found 3 output(s)
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
    Return the analysis output object matching the given output code.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query
    :param analysis_output_code: The output code to match against analysis outputs

    :return: The analysis output object for the matching code
    :rtype: `AnalysisOutput <https://umccr.github.io/libica/openapi/v3/docs/AnalysisOutput/>`_

    :raises StopIteration: If no output matches the given analysis output code
    :raises ValueError: If the matched output has no data items
    :raises ApiException: If the API call to retrieve analysis outputs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_output_object_from_analysis_output_code

        output_obj = get_analysis_output_object_from_analysis_output_code(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678",
            analysis_output_code="Output"
        )

        print(f"Output code: {output_obj.code}")
        # Output code: Output
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
    Return the CWL outputs JSON dictionary for a given analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query

    :return: The parsed CWL outputs JSON as a dictionary
    :rtype: Dict[str, Any]

    :raises ApiException: If the API call to retrieve CWL outputs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_cwl_outputs_json_from_analysis_id

        cwl_outputs = get_cwl_outputs_json_from_analysis_id(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(cwl_outputs)
        # {'output_dir': {'class': 'Directory', 'location': 'icav2://...'}}
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
    Return the analysis object for a given project and analysis ID.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to retrieve

    :return: The analysis object matching the given ID
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ApiException: If the API call to retrieve the analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_obj_from_analysis_id

        analysis = get_analysis_obj_from_analysis_id(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
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
        include_technical_steps: Optional[bool] = False
) -> List[AnalysisStep]:
    """
    Return the workflow steps for a given analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query
    :param include_technical_steps: Whether to include technical steps in the result.
        Defaults to False, in which case only non-technical steps are returned

    :return: The list of analysis step objects
    :rtype: List[`AnalysisStep <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStep/>`_]

    :raises ApiException: If the API call to retrieve analysis steps fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_steps

        steps = get_analysis_steps(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(f"Found {len(steps)} step(s)")
        # Found 3 step(s)
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
    Return the logs object from a given analysis step.

    :param analysis_step: The analysis step object to extract logs from

    :return: The logs attribute of the analysis step
    :rtype: `AnalysisStepLogs <https://umccr.github.io/libica/openapi/v3/docs/AnalysisStepLogs/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import (
            get_analysis_steps,
            get_analysis_log_from_analysis_step
        )

        steps = get_analysis_steps("project-uuid-1234", "analysis-uuid-5678")
        step_logs = get_analysis_log_from_analysis_step(steps[0])

        print(f"Logs: {step_logs}")
        # Logs: AnalysisStepLogs(...)
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
    Write the analysis step logs to a file or stream.

    :param project_id: The project context the analysis was run in
    :param step_logs: The step logs object containing stream or data references
    :param log_name: The log stream name, one of stdout or stderr
    :param output_path: The file path or text stream to write logs to
    :param is_cwltool_log: Whether the log is a cwltool HTML log requiring conversion.
        Defaults to False, in which case the log is written as-is

    :raises ApiException: If the API call to read log data fails
    :raises AttributeError: If neither stream nor data output is available for the log

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_analysis import (
            get_analysis_steps,
            get_analysis_log_from_analysis_step,
            write_analysis_step_logs
        )

        steps = get_analysis_steps("project-uuid-1234", "analysis-uuid-5678")
        step_logs = get_analysis_log_from_analysis_step(steps[0])
        # Writes the stderr log content to the specified output file
        write_analysis_step_logs(
            project_id="project-uuid-1234",
            step_logs=step_logs,
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
    Abort a running analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to abort

    :raises ApiException: If the API call to abort the analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import abort_analysis

        # Aborts the specified analysis
        abort_analysis(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )
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
    Return a filtered list of analyses for a given project.

    :param project_id: The project identifier to list analyses for
    :param pipeline_id: Filter analyses by pipeline identifier.
        Defaults to None, in which case no pipeline filter is applied
    :param user_reference: Filter analyses by user reference string or regex pattern.
        Defaults to None, in which case no user reference filter is applied
    :param status: Filter analyses by status or list of statuses.
        Defaults to None, in which case no status filter is applied
    :param creation_date_before: Return only analyses created before this datetime.
        Defaults to None, in which case no upper creation date bound is applied
    :param creation_date_after: Return only analyses created after this datetime.
        Defaults to None, in which case no lower creation date bound is applied
    :param modification_date_before: Return only analyses modified before this datetime.
        Defaults to None, in which case no upper modification date bound is applied
    :param modification_date_after: Return only analyses modified after this datetime.
        Defaults to None, in which case no lower modification date bound is applied
    :param sort: A sort parameter or list of sort parameters for ordering results.
        Defaults to None, in which case no sorting is applied
    :param max_items: The maximum number of analyses to return.
        Defaults to None, in which case all matching analyses are returned

    :return: The list of analyses matching the specified filters
    :rtype: List[`AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_]

    :raises ApiException: If the API call to search analyses fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import list_analyses

        analyses = list_analyses(
            project_id="project-uuid-1234",
            status="SUCCEEDED"
        )

        print(f"Found {len(analyses)} analysis(es)")
        # Found 3 analysis(es)
        for analysis in analyses:
            print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
            # Analysis ID: abcd1234-..., Status: SUCCEEDED
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
    Return the CWL analysis input JSON as a dictionary.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query

    :return: The parsed CWL analysis input JSON as a dictionary
    :rtype: Dict

    :raises ApiException: If the API call to retrieve the CWL input JSON fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_cwl_analysis_input_json

        input_json = get_cwl_analysis_input_json(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(input_json)
        # {'input_file': {'class': 'File', 'location': 'icav2://...'}}
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
    Return the CWL analysis output JSON as a dictionary.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query

    :return: The parsed CWL analysis output JSON as a dictionary
    :rtype: Dict

    :raises ApiException: If the API call to retrieve the CWL output JSON fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_cwl_analysis_output_json

        output_json = get_cwl_analysis_output_json(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(output_json)
        # {'output_dir': {'class': 'Directory', 'location': 'icav2://...'}}
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
    Convert an analysis step object to a typed dictionary.

    :param analysis_step: The analysis step object to convert

    :return: The analysis step represented as a typed dictionary
    :rtype: Dict

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_steps, analysis_step_to_dict

        steps = get_analysis_steps("project-uuid-1234", "analysis-uuid-5678")
        step_dict = analysis_step_to_dict(steps[0])

        print(step_dict)
        # {'name': 'step-name', 'status': 'DONE', ...}
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
    Return the analysis object matching a given user reference.

    :param project_id: The project context to search analyses in
    :param user_reference: The unique user reference string to match

    :return: The analysis object matching the user reference
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ValueError: If no analysis or multiple analyses match the user reference

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_obj_from_user_reference

        analysis = get_analysis_obj_from_user_reference(
            project_id="project-uuid-1234",
            user_reference="my-unique-analysis-ref"
        )

        print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
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
    Coerce an analysis ID or user reference to an analysis object.

    :param project_id: The project context to resolve the analysis in
    :param analysis_id_or_user_reference: The analysis identifier as a UUID or a user reference string

    :return: The resolved analysis object
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ValueError: If the user reference matches zero or multiple analyses
    :raises ApiException: If the API call to retrieve the analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import coerce_analysis_id_or_user_reference_to_analysis_obj

        analysis = coerce_analysis_id_or_user_reference_to_analysis_obj(
            project_id="project-uuid-1234",
            analysis_id_or_user_reference="my-analysis-ref"
        )

        print(f"Analysis ID: {analysis.id}, Status: {analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
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
    Coerce an analysis ID or user reference to an analysis ID string.

    :param project_id: The project context to resolve the analysis in
    :param analysis_id_or_user_reference: The analysis identifier as a UUID or a user reference string

    :return: The resolved analysis ID as a string
    :rtype: str

    :raises ValueError: If the user reference matches zero or multiple analyses

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import coerce_analysis_id_or_user_reference_to_analysis_id

        analysis_id = coerce_analysis_id_or_user_reference_to_analysis_id(
            project_id="project-uuid-1234",
            analysis_id_or_user_reference="my-analysis-ref"
        )

        print(analysis_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
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
    Update an analysis object with the provided analysis data.

    :param project_id: The project context the analysis belongs to
    :param analysis_id: The analysis identifier to update
    :param analysis_obj: The analysis object containing updated fields

    :return: The updated analysis object returned by the API
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ApiException: If the API call to update the analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import (
            get_analysis_obj_from_analysis_id,
            update_analysis_obj
        )

        analysis = get_analysis_obj_from_analysis_id("project-uuid-1234", "analysis-uuid-5678")
        updated = update_analysis_obj(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678",
            analysis_obj=analysis
        )
        print(f"Analysis ID: {updated.id}, Status: {updated.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
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
    """
    Add a tag to an existing analysis object.

    :param project_id: The project context the analysis belongs to
    :param analysis_id: The analysis identifier to add the tag to
    :param tag: The tag value string to append
    :param tag_type: The tag category, one of user_tag, technical_tag, or reference_tag

    :return: The updated analysis object with the new tag applied
    :rtype: `AnalysisV4 <https://umccr.github.io/libica/openapi/v3/docs/AnalysisV4/>`_

    :raises ValueError: If project_id or analysis_id is not in UUID format, or tag_type is unrecognised
    :raises ApiException: If the API call to update the analysis fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import add_tag_to_analysis

        updated_analysis = add_tag_to_analysis(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678",
            tag="my-custom-tag",
            tag_type="user_tag"
        )

        print(f"Analysis ID: {updated_analysis.id}, Status: {updated_analysis.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
    """
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


def get_analysis_usage(
        project_id: Union[UUID4, str],
        analysis_id: Union[UUID4, str],
) -> AnalysisUsageDetails:
    """
    Return the usage details for a given analysis.

    :param project_id: The project context the analysis was run in
    :param analysis_id: The analysis identifier to query usage for

    :return: The usage details for the specified analysis
    :rtype: `AnalysisUsageDetails <https://umccr.github.io/libica/openapi/v3/docs/AnalysisUsageDetails/>`_

    :raises ApiException: If the API call to retrieve usage details fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_analysis import get_analysis_usage

        usage = get_analysis_usage(
            project_id="project-uuid-1234",
            analysis_id="analysis-uuid-5678"
        )

        print(f"Analysis ID: {usage.analysis_id}, Status: {usage.status}")
        # Analysis ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: SUCCEEDED
    """

    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectAnalysisApi(api_client)

    try:
        # Retrieve the analysis usage details
        api_response: AnalysisUsageDetails = api_instance.get_analysis_usage_details(
            project_id=str(project_id),
            analysis_id=str(analysis_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectAnalysisApi->get_analysis_usage_details: %s\n" % e)
        raise e

    return api_response
