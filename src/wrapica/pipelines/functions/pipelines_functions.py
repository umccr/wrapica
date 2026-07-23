#!/usr/bin/env python3

# Standard library imports
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, List, Union
from zipfile import ZipFile
from cwl_utils.parser import load_document_by_uri
from pydantic import UUID4

# Libica API imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.pipeline_api import PipelineApi

# Libica model imports
from libica.openapi.v3.models import (
    PipelineFile,
    PipelineV3,
    PipelineV4
)

# Local imports
from ...utils.configuration import get_icav2_configuration
from ...utils.cwl_typing_helpers import WorkflowType
from ...utils.logger import get_logger
from ...utils.miscell import is_uuid_format, coerce_to_uuid4_obj

# Logger
logger = get_logger()

# Custom types
PipelineType = Union[PipelineV3, PipelineV4]


def get_pipeline_obj_from_pipeline_id(
    pipeline_id: Union[UUID4, str]
) -> PipelineType:
    """
    Return the pipeline object for a given pipeline ID.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string

    :return: The pipeline object matching the given ID
    :rtype: `Pipeline <https://umccr.github.io/libica/openapi/v3/docs/Pipeline/>`_

    :raises ApiException: If the API call to retrieve the pipeline fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import get_pipeline_obj_from_pipeline_id

        pipeline_obj = get_pipeline_obj_from_pipeline_id(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456"
        )

        print(f"Pipeline ID: {pipeline_obj.id}, Code: {pipeline_obj.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my-pipeline
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force the API client to send back the v4 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v4+json"
        )
        # Create an instance of the API class
        api_instance = PipelineApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve a pipeline.
        api_response: PipelineV4 = api_instance.get_pipeline(pipeline_id=str(pipeline_id))
    except ApiException as e:
        logger.error("Exception when calling PipelineApi->get_pipeline: %s\n" % e)
        raise ApiException

    return api_response


def get_pipeline_obj_from_pipeline_code(
    pipeline_code: str
) -> PipelineType:
    """
    Return the pipeline object matching the given pipeline code.

    :param pipeline_code: The unique code string identifying the pipeline

    :return: The pipeline object whose code matches the input
    :rtype: `Pipeline <https://umccr.github.io/libica/openapi/v3/docs/Pipeline/>`_

    :raises StopIteration: If no pipeline matches the given code

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import get_pipeline_obj_from_pipeline_code

        pipeline_obj = get_pipeline_obj_from_pipeline_code(
            pipeline_code="my-pipeline-code"
        )

        print(f"Pipeline ID: {pipeline_obj.id}, Code: {pipeline_obj.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my-pipeline-code
    """

    try:
        return next(
            filter(
                lambda pipeline_iter: pipeline_iter.code == pipeline_code,
                list_all_pipelines(),
            )
        )
    except StopIteration:
        logger.error(f"Pipeline with code {pipeline_code} not found")
        raise StopIteration


def coerce_pipeline_id_or_code_to_pipeline_obj(pipeline_id_or_code: str) -> PipelineType:
    """
    Coerce a pipeline ID or code string to a pipeline object.

    :param pipeline_id_or_code: The pipeline identifier as a UUID-formatted string or a pipeline code

    :return: The resolved pipeline object for the given identifier
    :rtype: `Pipeline <https://umccr.github.io/libica/openapi/v3/docs/Pipeline/>`_

    :raises ApiException: If the API call to retrieve the pipeline fails
    :raises StopIteration: If the pipeline code does not match any pipeline

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import coerce_pipeline_id_or_code_to_pipeline_obj

        pipeline_obj = coerce_pipeline_id_or_code_to_pipeline_obj(
            "my-pipeline-code"
        )

        print(f"Pipeline ID: {pipeline_obj.id}, Code: {pipeline_obj.code}")
        # Pipeline ID: abcd1234-ab12-ab12-ab12-abcdef123456, Code: my-pipeline-code
    """

    # Check uuid format
    if is_uuid_format(pipeline_id_or_code):
        return get_pipeline_obj_from_pipeline_id(pipeline_id_or_code)
    else:
        return get_pipeline_obj_from_pipeline_code(pipeline_id_or_code)


def coerce_pipeline_id_or_code_to_pipeline_id(pipeline_id_or_code: str) -> Union[UUID4, str]:
    """
    Coerce a pipeline ID or code string to the pipeline ID.

    :param pipeline_id_or_code: The pipeline identifier as a UUID-formatted string or a pipeline code

    :return: The pipeline ID for the given identifier
    :rtype: str

    :raises StopIteration: If the pipeline code does not match any pipeline

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import coerce_pipeline_id_or_code_to_pipeline_id

        pipeline_id = coerce_pipeline_id_or_code_to_pipeline_id(
            "my-pipeline-code"
        )

        print(pipeline_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """

    # Check uuid format
    if is_uuid_format(pipeline_id_or_code):
        return pipeline_id_or_code

    # If not uuid format, assume it is a pipeline code
    return get_pipeline_obj_from_pipeline_code(pipeline_id_or_code).id


def list_all_pipelines() -> List[PipelineType]:
    """
    Return all pipelines available to the user in this tenant.

    :return: The list of all available pipelines
    :rtype: List[`Pipeline <https://umccr.github.io/libica/openapi/v3/docs/Pipeline/>`_]

    :raises ApiException: If the API call to retrieve the pipeline list fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import list_all_pipelines

        pipelines = list_all_pipelines()

        for pipeline in pipelines:
            print(f"Pipeline: {pipeline.code}")
            # Pipeline: my-pipeline
    """

    # Create an instance of the API class
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force the API client to send back the v3 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        api_instance = PipelineApi(api_client)

    # example, this endpoint has no required or optional parameters
    # No page token required
    try:
        # Retrieve a list of pipelines.
        pipelines: List[PipelineV3] = api_instance.get_pipelines().items
    except ApiException as e:
        logger.error("Could not get pipeline list")
        raise ApiException from e

    return pipelines


def download_pipeline_file(
    pipeline_id: Union[UUID4, str],
    file_id: Union[UUID4, str],
    file_path: Optional[Path] = None
) -> Optional[BytesIO]:
    """
    Download the content of a pipeline file by file ID.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string
    :param file_id: The file identifier as a UUID4 object or UUID-formatted string
    :param file_path: The local path to save the file to. Defaults to None, in which case
        the file content is returned as a BytesIO object

    :return: The file content as a BytesIO object, or None if file_path is provided
    :rtype: Optional[BytesIO]

    :raises ApiException: If the API call to download the file content fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import download_pipeline_file

        # Downloads the pipeline file content to the local path
        download_pipeline_file(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456",
            file_id="efgh5678-ef56-ef56-ef56-efghij789012",
            file_path=Path("workflow.cwl")
        )

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import download_pipeline_file

        content = download_pipeline_file(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456",
            file_id="efgh5678-ef56-ef56-ef56-efghij789012"
        )

        print(content)
        # <_io.BytesIO object>
    """
    # Create an instance of the API class
    with ApiClient(get_icav2_configuration()) as api_client:
        # Force the API client to send back the v3 API
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/octet-stream"
        )
        api_instance = PipelineApi(api_client)

    try:
        # Download the contents of a pipeline file.
        api_response = api_instance.download_pipeline_file_content(
            pipeline_id=str(pipeline_id),
            file_id=coerce_to_uuid4_obj(file_id),
        )
    except ApiException as e:
        logger.error("Exception when calling PipelineApi->download_pipeline_file_content: %s\n" % e)
        raise ApiException

    # Write out file
    if file_path is not None:
        # Check parent exists
        assert file_path.parent.is_dir(), f"Parent directory {file_path.parent} does not exist"

        with open(file_path, 'wb') as file_h:
            file_h.write(
                api_response
            )
        return None
    else:
        return BytesIO(api_response)


def list_pipeline_files(
    pipeline_id: Union[UUID4, str]
) -> List[PipelineFile]:
    """
    Return the list of files for a given pipeline.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string

    :return: The list of files belonging to the pipeline
    :rtype: List[`PipelineFile <https://umccr.github.io/libica/openapi/v3/docs/PipelineFile/>`_]

    :raises ApiException: If the API call to retrieve pipeline files fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import list_pipeline_files

        pipeline_files = list_pipeline_files(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456"
        )

        for pf in pipeline_files:
            print(f"File: {pf.name}")
            # File: workflow.cwl
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = PipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve files for a project pipeline.
        api_response = api_instance.get_pipeline_files(pipeline_id=str(pipeline_id))
    except ApiException as e:
        logger.error("Exception when calling PipelineApi->get_pipeline_files: %s\n" % e)
        raise ApiException

    return api_response.items


def download_pipeline_to_directory(
        pipeline_id: Union[UUID4, str],
        output_directory: Path
):
    """
    Download all files of a pipeline to a local directory.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string
    :param output_directory: The local directory path to download pipeline files into

    :raises ApiException: If the API call to retrieve pipeline files fails
    :raises AssertionError: If the parent directory of the output path does not exist

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import download_pipeline_to_directory

        # Downloads all pipeline files to the specified directory
        download_pipeline_to_directory(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456",
            output_directory=Path("my-pipeline")
        )
    """

    # Ensure parent directory exists
    assert output_directory.parent.is_dir(), f"Parent directory {output_directory.parent} does not exist"

    # Create output directory
    output_directory.mkdir(exist_ok=True)

    for pipeline_file in list_pipeline_files(pipeline_id):
        # Get file path
        file_path = output_directory / pipeline_file.name
        # Make sure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # Download file
        download_pipeline_file(pipeline_id, pipeline_file.id, file_path)


def download_pipeline_to_zip(
        pipeline_id: Union[UUID4, str],
        zip_path: Path
):
    """
    Download all files of a pipeline into a zip archive.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string
    :param zip_path: The local file path for the output zip archive

    :raises ApiException: If the API call to retrieve pipeline files fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import download_pipeline_to_zip

        # Downloads all pipeline files into a zip archive
        download_pipeline_to_zip(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456",
            zip_path=Path("my-pipeline.zip")
        )
    """
    # Get the pipeline as an object
    pipeline_obj = get_pipeline_obj_from_pipeline_id(pipeline_id)

    # Get the pipeline code
    pipeline_code = pipeline_obj.code

    # Create the temporary directory
    with TemporaryDirectory() as tmp_dir:
        output_dir = Path(tmp_dir) / pipeline_code

        # Download the pipeline to the directory
        download_pipeline_to_directory(pipeline_id, output_dir)

        # Zip the directory
        tmp_zip_path = output_dir.with_suffix(".zip")

        # Zip the output directory to the zip path
        with ZipFile(tmp_zip_path, 'w') as zip_h:
            for file in output_dir.rglob("*"):
                zip_h.write(file, output_dir.name / file.relative_to(output_dir))

        # Move the zip file to the final location
        tmp_zip_path.rename(zip_path)


def get_cwl_obj_from_pipeline_id(
        pipeline_id: Union[UUID4, str]
) -> WorkflowType:
    """
    Return the parsed CWL workflow object for a given pipeline ID.

    :param pipeline_id: The pipeline identifier as a UUID4 object or UUID-formatted string

    :return: The parsed CWL workflow object from the pipeline files
    :rtype: WorkflowType

    :raises ApiException: If the API call to retrieve pipeline files fails
    :raises FileNotFoundError: If the pipeline does not contain a workflow.cwl file

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.pipelines import get_cwl_obj_from_pipeline_id

        cwl_obj = get_cwl_obj_from_pipeline_id(
            pipeline_id="abcd1234-ab12-ab12-ab12-abcdef123456"
        )

        print(type(cwl_obj).__name__)
        # Workflow
    """

    # Create a temp directory
    pipeline_tmp_dir_obj = TemporaryDirectory(delete=False)
    pipeline_tmp_dir_path = Path(pipeline_tmp_dir_obj.name)

    # Download pipeline to directory
    download_pipeline_to_directory(
        pipeline_id=pipeline_id,
        output_directory=pipeline_tmp_dir_path
    )

    # Get the cwl file
    workflow_file = pipeline_tmp_dir_path / "workflow.cwl"

    # Check the workflow file exists
    if not workflow_file.exists():
        raise FileNotFoundError(f"Expected file 'workflow.cwl' in top directory, but it was not found")

    # Load the document
    return load_document_by_uri(workflow_file)
