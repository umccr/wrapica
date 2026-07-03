"""Tests for wrapica.project_data module."""
import warnings
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from libica.openapi.v3 import ApiException
from libica.openapi.v3.api.project_data_api import ProjectDataApi

from tests.test_helpers import (
    DUMMY_PROJECT_ID,
    DUMMY_DATA_ID_FILE,
    DUMMY_DATA_ID_FOLDER,
    DUMMY_ICAV2_URI,
    make_paged_response,
)

from wrapica.project_data.functions.project_data_functions import (
    create_data_in_project,
    get_project_data_obj_by_id,
    list_project_data_non_recursively,
    find_project_data_bulk,
    convert_icav2_uri_to_project_data_obj,
    convert_project_data_obj_to_icav2_uri,
    unpack_icav2_uri,
    create_download_url,
    is_folder_id_format,
    is_file_id_format,
    is_data_id_format,
    get_aws_credentials_access_for_project_folder,
    read_icav2_file_contents,
    write_icav2_file_contents,
    delete_project_data,
    presign_cwl_directory,
)

MODULE_PATH = "wrapica.project_data.functions.project_data_functions"


class TestCreateDataInProject:
    """Tests for create_data_in_project."""

    def test_create_data_payload_fields(self, mocker, configuration_fixture):
        """Verify CreateData payload includes name, folderPath, and dataType."""
        mock_response = MagicMock()
        mock_response.data.id = DUMMY_DATA_ID_FILE

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_create = mocker.patch.object(
            ProjectDataApi, "create_data_in_project",
            return_value=mock_response,
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = create_data_in_project(
                project_id=DUMMY_PROJECT_ID,
                parent_folder_path=Path("/output/results"),
                data_name="sample.txt",
                data_type="FILE",
            )

        assert result is mock_response
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["project_id"] == DUMMY_PROJECT_ID
        create_data_arg = call_kwargs["create_data"]
        assert create_data_arg.name == "sample.txt"
        assert create_data_arg.folder_path == "/output/results/"
        assert create_data_arg.data_type == "FILE"


class TestGetProjectDataObjById:
    """Tests for get_project_data_obj_by_id."""

    def test_returns_project_data(self, mocker, configuration_fixture):
        """Verify get_project_data is called with correct args and result returned."""
        mock_data_obj = MagicMock()
        mock_data_obj.data.id = DUMMY_DATA_ID_FILE

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get = mocker.patch.object(
            ProjectDataApi, "get_project_data",
            return_value=mock_data_obj,
        )

        result = get_project_data_obj_by_id(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FILE,
        )

        assert result is mock_data_obj
        mock_get.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FILE,
        )


class TestListProjectDataNonRecursively:
    """Tests for list_project_data_non_recursively."""

    def test_multi_page_pagination(self, mocker, configuration_fixture):
        """Verify multi-page listing aggregates items from all pages using next_page_token."""
        item_a = MagicMock()
        item_b = MagicMock()
        item_c = MagicMock()

        pages = make_paged_response([[item_a, item_b], [item_c]])

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_list = mocker.patch.object(
            ProjectDataApi, "get_project_data_list",
            side_effect=pages,
        )

        result = list_project_data_non_recursively(
            project_id=DUMMY_PROJECT_ID,
            parent_folder_path=Path("/data/input"),
        )

        assert result == [item_a, item_b, item_c]
        assert mock_list.call_count == 2


class TestFindProjectDataBulk:
    """Tests for find_project_data_bulk."""

    def test_starts_with_case_insensitive_mode(self, mocker, configuration_fixture):
        """Verify bulk find uses STARTS_WITH_CASE_INSENSITIVE file_path_match_mode."""
        item_1 = MagicMock()
        item_2 = MagicMock()

        mock_response = MagicMock()
        mock_response.items = [item_1, item_2]
        mock_response.next_page_token = ""

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_list = mocker.patch.object(
            ProjectDataApi, "get_project_data_list",
            return_value=mock_response,
        )

        result = find_project_data_bulk(
            project_id=DUMMY_PROJECT_ID,
            parent_folder_path=Path("/data/input"),
            data_type="FILE",
        )

        assert result == [item_1, item_2]
        mock_list.assert_called_once()
        call_kwargs = mock_list.call_args[1]
        assert call_kwargs["file_path_match_mode"] == "STARTS_WITH_CASE_INSENSITIVE"


class TestConvertIcav2UriToProjectDataObj:
    """Tests for convert_icav2_uri_to_project_data_obj."""

    def test_valid_uri_resolves_to_data_obj(self, mocker, configuration_fixture):
        """Verify a valid icav2://project-id/path URI resolves to a project data obj."""
        mock_data_obj = MagicMock()
        mock_data_obj.data.id = DUMMY_DATA_ID_FILE

        mocker.patch(
            f"{MODULE_PATH}.get_project_data_obj_from_project_id_and_path",
            return_value=mock_data_obj,
        )

        uri = f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"
        result = convert_icav2_uri_to_project_data_obj(uri)

        assert result is mock_data_obj


class TestConvertProjectDataObjToIcav2Uri:
    """Tests for convert_project_data_obj_to_icav2_uri."""

    def test_format_icav2_uri(self, configuration_fixture):
        """Verify output format is icav2://{project_id}{data_path}."""
        mock_data_obj = MagicMock()
        mock_data_obj.project_id = DUMMY_PROJECT_ID
        mock_data_obj.data.details.path = "/path/to/file.txt"
        mock_data_obj.data.details.data_type = "FILE"

        result = convert_project_data_obj_to_icav2_uri(mock_data_obj)

        assert result == f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"


class TestUnpackIcav2Uri:
    """Tests for unpack_icav2_uri."""

    def test_valid_uri_returns_tuple(self, mocker, configuration_fixture):
        """Verify a valid icav2:// URI returns (project_id, path) tuple."""
        mocker.patch(
            f"{MODULE_PATH}.is_uuid_format",
            return_value=True,
        )

        project_id, data_path = unpack_icav2_uri(
            f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"
        )

        assert project_id == DUMMY_PROJECT_ID
        assert data_path == "/path/to/file.txt"

    def test_invalid_scheme_raises_value_error(self, configuration_fixture):
        """Verify a non-icav2/s3 scheme raises ValueError."""
        with pytest.raises(ValueError, match="not recognised"):
            unpack_icav2_uri("ftp://example.com/path/to/file.txt")


class TestCreateDownloadUrl:
    """Tests for create_download_url."""

    def test_returns_url_string(self, mocker, configuration_fixture):
        """Verify the presigned URL string is returned from the API response."""
        mock_download = MagicMock()
        mock_download.url = "https://storage.example.com/file.txt?token=abc123"

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectDataApi, "create_download_url_for_data",
            return_value=mock_download,
        )

        result = create_download_url(
            project_id=DUMMY_PROJECT_ID,
            file_id=DUMMY_DATA_ID_FILE,
        )

        assert result == "https://storage.example.com/file.txt?token=abc123"


class TestIdFormatChecks:
    """Tests for is_folder_id_format, is_file_id_format, is_data_id_format."""

    def test_valid_folder_id(self):
        assert is_folder_id_format(DUMMY_DATA_ID_FOLDER) is True

    def test_invalid_folder_id(self):
        assert is_folder_id_format("fil.1234567890abcdef1234567890abcdef") is False

    def test_valid_file_id(self):
        assert is_file_id_format(DUMMY_DATA_ID_FILE) is True

    def test_invalid_file_id(self):
        assert is_file_id_format("fol.abcdef1234567890abcdef1234567890") is False

    def test_is_data_id_format_file(self):
        assert is_data_id_format(DUMMY_DATA_ID_FILE) is True

    def test_is_data_id_format_folder(self):
        assert is_data_id_format(DUMMY_DATA_ID_FOLDER) is True

    def test_is_data_id_format_invalid(self):
        assert is_data_id_format("xyz.0000000000000000") is False


class TestGetAwsCredentialsAccessForProjectFolder:
    """Tests for get_aws_credentials_access_for_project_folder."""

    def test_returns_aws_temp_credentials(self, mocker, configuration_fixture):
        """Verify AWS temp credentials are returned from the mocked API."""
        mock_aws_creds = MagicMock()
        mock_aws_creds.access_key = "AKIAEXAMPLE"
        mock_aws_creds.secret_key = "secret"
        mock_aws_creds.session_token = "token"
        mock_aws_creds.region = "us-east-1"

        mock_response = MagicMock()
        mock_response.aws_temp_credentials = mock_aws_creds
        mock_response.rclone_temp_credentials = None

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectDataApi, "create_temporary_credentials_for_data",
            return_value=mock_response,
        )

        result = get_aws_credentials_access_for_project_folder(
            project_id=DUMMY_PROJECT_ID,
            folder_id=DUMMY_DATA_ID_FOLDER,
        )

        assert result is mock_aws_creds
        assert result.access_key == "AKIAEXAMPLE"


class TestReadIcav2FileContents:
    """Tests for read_icav2_file_contents."""

    def test_returns_decoded_content(self, mocker, configuration_fixture):
        """Verify file contents are returned as decoded string when output_path is None."""
        mocker.patch(
            f"{MODULE_PATH}.create_download_url",
            return_value="https://storage.example.com/file.txt",
        )

        mock_response = MagicMock()
        mock_response.content = b"Hello, ICAv2!"
        mocker.patch(
            f"{MODULE_PATH}.requests.get",
            return_value=mock_response,
        )

        result = read_icav2_file_contents(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FILE,
        )

        assert result == "Hello, ICAv2!"


class TestWriteIcav2FileContents:
    """Tests for write_icav2_file_contents."""

    def test_uploads_file_contents(self, mocker, configuration_fixture, tmp_path):
        """Verify file is created and contents are PUT to the upload URL."""
        mock_file_obj = MagicMock()
        mock_file_obj.data.id = DUMMY_DATA_ID_FILE

        mocker.patch(
            f"{MODULE_PATH}.create_file_in_project",
            return_value=mock_file_obj,
        )
        mocker.patch(
            f"{MODULE_PATH}.get_project_data_upload_url",
            return_value="https://storage.example.com/upload?token=xyz",
        )
        mock_put = mocker.patch(
            f"{MODULE_PATH}.requests.put",
        )

        # Create a local file to upload
        local_file = tmp_path / "upload.txt"
        local_file.write_text("upload content")

        result = write_icav2_file_contents(
            project_id=DUMMY_PROJECT_ID,
            data_path=Path("/output/upload.txt"),
            file_stream_or_path=local_file,
        )

        assert result == DUMMY_DATA_ID_FILE
        mock_put.assert_called_once()
        call_args = mock_put.call_args
        assert call_args[0][0] == "https://storage.example.com/upload?token=xyz"
        assert call_args[1]["data"] == b"upload content"


class TestDeleteProjectData:
    """Tests for delete_project_data."""

    def test_calls_delete_data_api(self, mocker, configuration_fixture):
        """Verify ProjectDataApi.delete_data is called with correct parameters."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_delete = mocker.patch.object(
            ProjectDataApi, "delete_data",
        )

        delete_project_data(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FILE,
        )

        mock_delete.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FILE,
        )


class TestPresignCwlDirectory:
    """Tests for presign_cwl_directory."""

    def test_presign_files_and_folders(self, mocker, configuration_fixture):
        """Verify CWL directory listing with presigned URLs for files."""
        # Create mock file items
        mock_file = MagicMock()
        mock_file.data.details.data_type = "FILE"
        mock_file.data.id = DUMMY_DATA_ID_FILE
        mock_file.data.details.name = "output.bam"

        mock_folder = MagicMock()
        mock_folder.data.details.data_type = "FOLDER"
        mock_folder.data.id = DUMMY_DATA_ID_FOLDER
        mock_folder.data.details.name = "subfolder"

        # Mock nested folder content (empty)
        mocker.patch(
            f"{MODULE_PATH}.list_project_data_non_recursively",
            side_effect=[[mock_file, mock_folder], []],
        )
        mocker.patch(
            f"{MODULE_PATH}.create_download_url",
            return_value="https://storage.example.com/output.bam?token=abc",
        )

        result = presign_cwl_directory(
            project_id=DUMMY_PROJECT_ID,
            data_id=DUMMY_DATA_ID_FOLDER,
        )

        assert len(result) == 2
        # File entry
        assert result[0]["class"] == "File"
        assert result[0]["basename"] == "output.bam"
        assert result[0]["location"] == "https://storage.example.com/output.bam?token=abc"
        # Directory entry
        assert result[1]["class"] == "Directory"
        assert result[1]["basename"] == "subfolder"
        assert result[1]["listing"] == []
