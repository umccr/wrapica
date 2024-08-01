#!/usr/bin/env python3
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, List, Union
from zipfile import ZipFile
from cwl_utils.parser import load_document_by_uri

# Libica API imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.pipeline_api import PipelineApi

# Libica model imports
from libica.openapi.v2.models import (
    PipelineFile,
    PipelineV3,
    PipelineV4
)

# Local imports
from ...utils.configuration import get_icav2_configuration
from ...utils.cwl_typing_helpers import WorkflowType
from ...utils.logger import get_logger
from ...utils.miscell import is_uuid_format

PipelineType = Union[PipelineV3, PipelineV4]

# Logger
logger = get_logger()


def get_pipeline_obj_from_pipeline_id(
    pipeline_id: str
) -> PipelineType:
    """
    Get the pipeline object from the pipeline id

    :param pipeline_id:

    :return: The pipeline object
    :rtype: `Pipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/Pipeline/>`_

    :raises ApiException: If the pipeline is not found

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import get_pipeline_obj_from_pipeline_id

        # Use wrapica.pipelines.get_pipeline_obj_from_pipeline_code
        # If you need to convert a pipeline code to a pipeline object

        pipeline_obj: Pipeline = get_pipeline_obj_from_pipeline_id(
            pipeline_id="pipeline_id"
        )
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = PipelineApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve a list of pipelines.
        api_response: PipelineV3 = api_instance.get_pipeline(pipeline_id)
    except ApiException as e:
        logger.error("Exception when calling PipelineApi->get_pipelines: %s\n" % e)
        raise ApiException

    return api_response


def get_pipeline_obj_from_pipeline_code(
    pipeline_code: str
) -> PipelineType:
    """
    Get the pipeline object from the pipeline code

    :param pipeline_code:

    :return: The pipeline object
    :rtype: `Pipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/Pipeline/>`_

    :raises ApiException: If the pipeline is not found

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import get_pipeline_obj_from_pipeline_code

        # Use wrapica.pipelines.get_pipeline_obj_from_pipeline_id
        # If you need to convert a pipeline id to a pipeline object
        # Since get_pipeline_obj_from_pipeline_code will need to list all pipelines first
        # To find a matching pipeline, if a user has a pipeline id, it is recommended to use
        # get_pipeline_obj_from_pipeline_id instead of this function.

        pipeline_obj: Pipeline = get_pipeline_obj_from_pipeline_code(
            pipeline_code="pipeline_code"
        )

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
    Given either a pipeline id or code, check if the input value is uuid4 format,

    If so, assume it is a pipeline id and collect the object. Otherwise, assume it is a pipeline code and
    call get_pipeline_obj_from_pipeline_code to get the pipeline object

    :param pipeline_id_or_code:
    :return: The pipeline object
    :rtype: `Pipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/Pipeline/>`_

    :raises ValueError: If the pipeline cannot be found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import coerce_pipeline_id_or_code_to_pipeline_obj

        pipeline_code = "pipeline-123"

        pipeline_obj = coerce_pipeline_id_or_code_to_pipeline_obj(pipeline_code)

    """

    # Check uuid format
    if is_uuid_format(pipeline_id_or_code):
        return get_pipeline_obj_from_pipeline_id(pipeline_id_or_code)
    else:
        return get_pipeline_obj_from_pipeline_code(pipeline_id_or_code)


def coerce_pipeline_id_or_code_to_pipeline_id(pipeline_id_or_code: str) -> str:
    """
    Given either a pipeline id or code, check if the input value is uuid4 format,
    If so, assume it is a pipeline id. Otherwise, assume it is a pipeline code and
    call get_project_pipeline_id_from_pipeline_code to get the pipeline id

    :param pipeline_id_or_code:    The pipeline id or code to retrieve
    :return: The pipeline id       The pipeline id
    :rtype: str

    :raises: ValueError: If the pipeline cannot be found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_pipelines import coerce_pipeline_id_or_code_to_pipeline_id

        project_id = "project-123"
        pipeline_code = "pipeline-123"

        pipeline_id = coerce_pipeline_id_or_code_to_pipeline_id(project_id, pipeline_code)
    """

    # Check uuid format
    if is_uuid_format(pipeline_id_or_code):
        return pipeline_id_or_code

    # If not uuid format, assume it is a pipeline code
    return get_pipeline_obj_from_pipeline_code(pipeline_id_or_code).id


def list_all_pipelines() -> List[PipelineType]:
    """
    List all pipelines available to a user through the pipelines/ endpoint

    :return: List of pipelines
    :rtype: List[`Pipeline <https://umccr-illumina.github.io/libica/openapi/v2/docs/Pipeline/>`_]

    :raises ApiException: If the pipeline list cannot be retrieved

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import list_all_pipelines

        pipelines: List[Pipeline] = list_all_pipelines()
    """

    # Create an instance of the API class
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = PipelineApi(api_client)

    # example, this endpoint has no required or optional parameters
    # No page token required
    try:
        # Retrieve a list of pipelines.
        pipelines: List[PipelineV3] = api_instance.get_pipelines().items
    except ApiException as e:
        logger.error("Could not get pipeline list")
        raise ApiException

    return pipelines


def download_pipeline_file(
    pipeline_id: str,
    file_id: str,
    file_path: Optional[Path] = None
) -> Optional[BytesIO]:
    """
    Download pipeline file

    :param pipeline_id:  The pipeline id
    :param file_id:  The file id
    :param file_path:  The file path to save the file to (if not set, the file content will be returned)

    :return: The file content if file_path is not set
    :rtype: `BytesIO <https://docs.python.org/3/library/io.html#io.BytesIO>`_

    :raises ApiException: If the file cannot be downloaded

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.pipelines import download_pipeline_file

        download_pipeline_file(
            pipeline_id="pipeline_id",
            file_id="file_id",
            file_path=Path("path/to/file")
        )

    """
    assert file_path.parent.is_dir(), f"Parent directory {file_path.parent} does not exist"

    # example passing only required values which don't have defaults set
    # FIXME - wait until https://github.com/umccr-illumina/libica/issues/137 is resolved
    # try:
    #     # Download the contents of a pipeline file.
    #     api_response: file_type = api_instance.download_pipeline_file_content(
    #         pipeline_id=pipeline_id,
    #         file_id=file_id
    #     )
    # except ApiException as e:
    #     logger.error("Exception when calling PipelineApi->download_pipeline_file_content: %s\n" % e)
    #     raise ApiException
    import requests
    from requests import HTTPError

    headers = {
        "Accept": "application/octet-stream",
        "Authorization": f"Bearer {get_icav2_configuration().access_token}"
    }

    try:
        response = requests.get(
            get_icav2_configuration().host + f"/api/pipelines/{pipeline_id}/files/{file_id}/content",
            headers=headers
        )
        response.raise_for_status()
    except HTTPError:
        logger.error(f"Failed to download pipeline file {file_id} from pipeline {pipeline_id}")
        raise ApiException

    # Write out file
    if file_path is not None:
        # Check parent exists
        assert file_path.parent.is_dir(), f"Parent directory {file_path.parent} does not exist"

        with open(file_path, 'wb') as file_h:
            file_h.write(
                response.content
                # api_response.read()
            )
    else:
        # return BytesIO(api_response.read())
        return BytesIO(response.content)


def list_pipeline_files(
    pipeline_id: str
) -> List[PipelineFile]:
    """
    List pipeline files

    :param pipeline_id:

    :return: List of pipeline files
    :rtype: List[`PipelineFile <https://umccr-illumina.github.io/libica/openapi/v2/docs/PipelineFile/>`_]

    :raises ApiException: If the pipeline files cannot be retrieved

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.pipelines import list_pipeline_files

        # List pipeline files
        pipeline_files: List[PipelineFile] = list_pipeline_files(
            pipeline_id="pipeline_id"
        )
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = PipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve files for a project pipeline.
        api_response = api_instance.get_pipeline_files(pipeline_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectPipelineApi->get_pipeline_files1: %s\n" % e)
        raise ApiException

    return api_response.items


def download_pipeline_to_directory(pipeline_id: str, output_directory: Path):
    """
    Download a pipeline to a directory

    :param pipeline_id
    :param output_directory

    :raises ApiException: If the pipeline files cannot be retrieved
    :raises AssertionError: If the parent directory does not exist

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from pathlib import Path
        from wrapica.pipelines import download_pipeline_to_directory

        # Download pipeline to local directory
        download_pipeline_to_directory(
            pipeline_id="pipeline_id",
            output_directory=Path("path/to/output/directory")
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
        pipeline_id: str,
        zip_path: Path
):
    """
    Given a pipeline id, download the pipeline to a zip file

    :param pipeline_id:
    :param zip_path:
    :return:
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
        pipeline_id: str
) -> WorkflowType:
    """
    Get the pipeline files from a project pipeline
    The arrange the files as they're named to generate the workflow object

    :param pipeline_id:
    :return:
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
