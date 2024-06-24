#!/usr/bin/env python

"""
An agnostic Analysis Object

Both CWL Analysis and NextFlow analysis will use this object
as their parent class

"""
# Standard imports
import json
from pathlib import Path
from typing import List, Optional, Union, Dict, Any
from urllib.parse import urlparse
from datetime import datetime
from uuid import uuid4

# Libica imports
from libica.openapi.v2.models import (
    AnalysisV3,
    AnalysisV4,
    AnalysisOutputMapping,
    AnalysisTag,
    CreateCwlAnalysis,
    CreateNextflowAnalysis,
    CwlAnalysisJsonInput,
    CwlAnalysisStructuredInput,
    NextflowAnalysisInput,
    ProjectData
)

# Local imports
from ...utils import recursively_build_open_api_body_from_libica_item
from ...enums import AnalysisStorageSize, DataType
from ...utils.miscell import sanitise_dict_keys
from ...utils.logger import get_logger

# Set logger
logger = get_logger()

Analysis = Union[AnalysisV3, AnalysisV4]
CwlAnalysisInput = Union[CwlAnalysisJsonInput, CwlAnalysisStructuredInput]


class ICAv2AnalysisInput:
    """
    Parent class for CWLAnalysisInput, NextflowAnalysisInput
    """
    def __init__(
        self,
        object_type: str,
    ):
        # Set the object type
        self.object_type = object_type

    def __call__(
            self
    ) -> Union[
        CwlAnalysisInput,
        CwlAnalysisJsonInput,
        CwlAnalysisStructuredInput,
        NextflowAnalysisInput
    ]:
        """
        Create the analysis input
        :return:
        """
        self.validate_input()
        return self.create_analysis_input()

    def create_analysis_input(self):
        """
        Implemented in subclass
        :return:
        """
        raise NotImplementedError

    def validate_input(self):
        """
        For structured inputs, one can first import the pipeline id and compare the input values against those
        specified in the pipeline, and compare required against non-required inputs
        For JSON based inputs, one will need to validate against the JSON schema. This is not implemented yet
        as waiting on PR https://github.com/common-workflow-language/cwl-utils/pull/288
        We also want https://github.com/umccr-illumina/ica_v2/issues/162 resolved so we can validate against the top level workflow object.
        :return:
        """
        raise NotImplementedError


class ICAv2PipelineAnalysisTags:
    """
    List of tags
    """
    def __init__(
        self,
        technical_tags: Union[List | Dict],
        user_tags: Union[List | Dict],
        reference_tags: Union[List | Dict]
    ):
        """
        List of tags to use in the pipeline
        :param technical_tags:
        :param user_tags:
        :param reference_tags:
        """

        # Assign
        self.technical_tags = technical_tags
        self.user_tags = user_tags
        self.reference_tags = reference_tags

        self.clean_tags()

    def clean_tags(self):
        # Now clean up
        for tag_type in ["technical_tags", "user_tags", "reference_tags"]:
            if not isinstance(getattr(self, tag_type), (list, dict)):
                raise ValueError(f"{tag_type} must be a list or a dictionary")
            # For each key-value pair in the dictionary, convert it to a string split by '='
            if isinstance(getattr(self, tag_type), dict):
                setattr(
                    self,
                    tag_type,
                    list(
                        map(
                            lambda tag_iter: f"{tag_iter[0]}={tag_iter[1]}",
                            getattr(self, tag_type).items()
                        )
                    )
                )
            else:
                # Ensure that each item in the tag list is a string
                setattr(
                    self,
                    tag_type,
                    list(
                        map(
                            str,
                            getattr(self, tag_type)
                        )
                    )
                )

    def __call__(self) -> AnalysisTag:
        return AnalysisTag(
            technical_tags=self.technical_tags,
            user_tags=self.user_tags,
            reference_tags=self.reference_tags
        )

    def combine_tags(self, analysis_tags: 'ICAv2PipelineAnalysisTags') -> 'ICAv2PipelineAnalysisTags':
        """
        Combine tags from another tag object
        :param analysis_tags:
        :return:
        """
        # Combine the tags
        return ICAv2PipelineAnalysisTags(
            technical_tags=self.technical_tags + analysis_tags.technical_tags,
            user_tags=self.user_tags + analysis_tags.user_tags,
            reference_tags=self.reference_tags + analysis_tags.reference_tags
        )

    @classmethod
    def from_dict(cls, tags_dict):
        # Convert camel cases to snake cases
        tags_dict = sanitise_dict_keys(tags_dict)

        return cls(
                    technical_tags=tags_dict.get("technical_tags", []),
                    user_tags=tags_dict.get("user_tags", []),
                    reference_tags=tags_dict.get("reference_tags", [])
        )


class ICAv2EngineParameters:
    """
    The ICAv2 EngineParameters has the following properties
    """

    # Set in subclasses
    workflow_language = None

    def __init__(
            self,
            project_id: Optional[str] = None,
            pipeline_id: Optional[str] = None,
            analysis_input: Optional[Union[CwlAnalysisInput, NextflowAnalysisInput]] = None,
            analysis_output: Optional[List[AnalysisOutputMapping]] = None,
            tags: Optional[ICAv2PipelineAnalysisTags] = None,
            analysis_storage_id: Optional[str] = None,
            analysis_storage_size: Optional[AnalysisStorageSize] = None,
            activation_id: Optional[str] = None,
    ):
        # Initialise parameters

        # Launch parameters
        self.project_id: Optional[str] = project_id
        self.pipeline_id: Optional[str] = pipeline_id
        self.analysis_storage_id: Optional[str] = analysis_storage_id
        self.analysis_storage_size: Optional[AnalysisStorageSize] = analysis_storage_size
        self.activation_id: Optional[str] = activation_id

        # Output parameters
        self.analysis_output: Optional[List[AnalysisOutputMapping]] = analysis_output
        self.analysis_input: Optional[Union[CwlAnalysisInput, NextflowAnalysisInput]] = analysis_input

        # Meta parameters
        self.tags: ICAv2PipelineAnalysisTags = tags

        # self.stream_all_files: Optional[bool] = stream_all_files
        # self.stream_all_directories: Optional[bool] = stream_all_directories

        # Set placeholders
        current_utc_time = datetime.utcnow()
        self.placeholder_dict: Dict = {
            "__DATE_STR__": current_utc_time.strftime("%Y%m%d"),
            "__TIME_STR__": current_utc_time.strftime("%H%M%S"),
            "__UUID8_STR__": uuid4().hex[:8],
            "__UUID16_STR__": uuid4().hex[:16],
        }

    def __call__(self):
        # Assumed that the following have been run
        # set_launch_parameters
        # set_output_parameters
        # set_meta_parameters
        # All functions can be harmlessly called again
        self.set_launch_parameters()
        self.set_output_parameters()
        self.set_meta_parameters()

        # Now check parameters
        self.check_launch_parameters()
        self.check_output_parameters()
        self.check_meta_parameters()

    def set_launch_parameters(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        analysis_input: Optional[Union[CwlAnalysisJsonInput, CwlAnalysisStructuredInput]] = None
    ):
        # Local import of functions from classes to avoid circular imports
        from ..functions.project_pipelines_functions import (
            get_default_analysis_storage_id_from_project_pipeline,
            get_activation_id,
            get_analysis_storage_id_from_analysis_storage_size
        )
        # Check for project id
        if project_id is not None:
            self.project_id = project_id

        # Check for pipeline id
        if pipeline_id is not None:
            self.pipeline_id = pipeline_id

        # Check for analysis storage id
        if analysis_storage_id is not None:
            self.analysis_storage_id = analysis_storage_id

        # Check for analysis storage size
        if analysis_storage_size is not None:
            self.analysis_storage_size = analysis_storage_size

        # Check for analysis input
        if analysis_input is not None:
            self.analysis_input = analysis_input

        # If analysis storage size is set, but analysis storage id is not set
        if self.analysis_storage_size is not None and self.analysis_storage_id is None:
            self.analysis_storage_id = get_analysis_storage_id_from_analysis_storage_size(
                self.analysis_storage_size
            )

        elif self.analysis_storage_size is None and self.analysis_storage_id is None:
            # Ensure that project_id and pipeline_id are set at this point
            if self.project_id is None or self.pipeline_id is None:
                logger.error("Cannot get analysis storage id without project id and pipeline id")
                raise ValueError
            # Get the default analysis storage id
            self.analysis_storage_id = get_default_analysis_storage_id_from_project_pipeline(
                self.project_id,
                self.pipeline_id
            )

        # Check for activation id
        if activation_id is not None:
            self.activation_id = activation_id
        elif self.activation_id is None:
            # Ensure that both project_id and pipeline_id are set at this point
            if self.project_id is None or self.pipeline_id is None or self.analysis_input is None:
                logger.error("Cannot get the activation id without project id and pipeline id or analysis input")
                raise ValueError
            self.activation_id = get_activation_id(
                project_id=self.project_id,
                pipeline_id=self.pipeline_id,
                analysis_input=self.analysis_input,
                workflow_language=self.workflow_language
            )

    def check_launch_parameters(self):
        """
        Check the launch parameters have been set
        :return:
        """
        has_error = False
        for parameter in ["project_id", "pipeline_id", "analysis_storage_id", "activation_id"]:
            if self.__getattribute__(parameter) is None:
                logger.error(f"{parameter} has not been set")
                has_error = True
        if has_error:
            raise ValueError

    def set_output_parameters(
        self,
        analysis_output: Optional[List[AnalysisOutputMapping]] = None
    ):
        """
        Set analysis output
        :param analysis_output:
        :return:
        """
        self.analysis_output = analysis_output

    def check_output_parameters(self):
        """
        Ensure that at analysis_output is set
        :return:
        """
        # Ensure that at analysis_output is set
        if (
            self.analysis_output is None
        ):
            # Needed to specify one of the inputs
            logger.error("analysis_output is not set")
            raise ValueError

    def set_meta_parameters(
        self,
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
    ):
        # Set tags if tags is empty
        if tags is not None and self.tags is None:
            self.tags = tags

        # Combine tags if both are set
        elif tags is not None and self.tags is not None:
            self.tags = self.tags.combine_tags(tags)

    def check_meta_parameters(self):
        if self.tags is None:
            logger.warning("No tags set for this analysis")

    def update_engine_parameter(self, attribute_name: str, value: Any):
        self.__setattr__(attribute_name, value)

    def populate_placeholders_in_output_path(self, analysis_path: Path):
        """
        Populate placeholders in the output path
        :param analysis_path:
        :return:
        """
        # Import functions locally to avoid circular imports
        from ...utils import fill_placeholder_path
        return fill_placeholder_path(
            analysis_path,
            self.placeholder_dict
        )


class ICAv2PipelineAnalysis:
    """
    A parent analysis class with the following attributes

    * name / user-reference  # The pipeline name
    * pipeline-id  # The pipeline id
    * tags - Tags used on the analysis ICAv2PipelineAnalysisTags class
    * activationCodeDetailsId - Collection of the activation code is implemented in the subclass
    * analysisStorageId
    * analysisOutput ( we skip the outputParentFolderId, it doesn't give us much control)
        * This should be a path-based uri
    * analysisInput ( implemented in the subclass )
    """

    def __init__(
        self,
        # Launch parameters
        user_reference: str,
        project_id: str,
        pipeline_id: str,
        analysis_input: Union[CwlAnalysisInput, NextflowAnalysisInput],
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        analysis_output_uri: Optional[str] = None,
        ica_logs_uri: Optional[str] = None,
        # Meta parameters
        tags: Optional[ICAv2PipelineAnalysisTags] = None
    ):
        """
        Initialise parent analysis class with the following attributes

        * name / user-reference  # The analysis name - Required
        * project-id  # The project id - Required
        * pipeline-id  # The pipeline id - Required
        * tags - Tags used on the analysis ICAv2PipelineAnalysisTags class  - Required
        * analysis_storage_size - The size of the analysis storage - Optional
            * We can figure this out later
        * activation_id - Collection of the activation code is implemented in the subclass - Optional
            * We can figure this out later
        * analysisOutput ( we skip the outputParentFolderId, it doesn't give us much control)
            * This should be a path-based uri - Required
        * analysis_input ( implemented in the subclass )
        """
        # Launch parameters
        self.user_reference = user_reference
        self.project_id = project_id
        self.pipeline_id = pipeline_id
        self.activation_id = activation_id
        self.analysis_output_uri = analysis_output_uri
        self.ica_logs_uri = ica_logs_uri

        # Meta parameters
        self.tags = tags
        self.analysis_storage_id = analysis_storage_id
        self.analysis_storage_size = analysis_storage_size
        self.analysis_input = analysis_input

        self.engine_parameters: Optional[ICAv2EngineParameters] = None

        if self.analysis_output_uri is not None:
            self.analysis_output: List[AnalysisOutputMapping] = [self.get_analysis_output_mapping_from_uri()]
        else:
            self.analysis_output = None

        # Deprecated until https://github.com/umccr-illumina/ica_v2/issues/184 is resolved
        # if self.ica_logs_uri is not None:
        #     if self.analysis_output is not None:
        #         self.analysis_output.append(self.get_ica_logs_mapping_from_uri())
        #     else:
        #         self.analysis_output = self.get_ica_logs_mapping_from_uri()

        self.set_engine_parameters()

        # Set the analysis
        self.analysis: Optional[Union[CreateCwlAnalysis, CreateNextflowAnalysis]] = None

    def __call__(self) -> Analysis:
        """
        Fix up the engine parameters, and generate a CWL Analysis Object
        :return:
        """
        # Check engine parameters
        self.check_engine_parameters()

        # Create analysis
        self.analysis = self.create_analysis()

        return self.launch_analysis()

    def get_analysis_output_mapping_from_uri(self) -> AnalysisOutputMapping:
        """
        Convert the analysis output to a mapping
        :return:
        """
        from ...project_data import convert_icav2_uri_to_project_data_obj
        # Ensure that the path attribute of analysis_output_uri ends with /
        if not urlparse(self.analysis_output_uri).path.endswith("/"):
            raise ValueError("The analysis output uri must end with a /")

        analysis_output_obj: ProjectData = convert_icav2_uri_to_project_data_obj(
            self.analysis_output_uri,
            create_data_if_not_found=True
        )

        return AnalysisOutputMapping(
            source_path="out/",  # Hardcoded, all workflow outputs should be placed in the out folder,
            type=DataType.FOLDER.value,  # Hardcoded, out directory is a folder
            target_project_id=analysis_output_obj.project_id,
            target_path=analysis_output_obj.data.details.path
        )

    def get_ica_logs_mapping_from_uri(self) -> AnalysisOutputMapping:
        from ...project_data import convert_icav2_uri_to_project_data_obj
        # Ensure that the path attribute of analysis_output_uri ends with /
        if not urlparse(self.ica_logs_uri).path.endswith("/"):
            raise ValueError("The analysis output uri must end with a /")

        ica_logs_project_data_obj: ProjectData = convert_icav2_uri_to_project_data_obj(
            self.ica_logs_uri,
            create_data_if_not_found=True
        )

        return AnalysisOutputMapping(
            source_path="ica_logs/",  # Hardcoded, all logs should be placed in the ica_logs folder,
            type=DataType.FOLDER.value,  # Hardcoded, out directory is a folder
            target_project_id=ica_logs_project_data_obj.project_id,
            target_path=ica_logs_project_data_obj.data.details.path
        )

    def set_engine_parameters(self):
        """
        Implemented in subclass
        :return:
        """
        raise NotImplementedError

    def check_engine_parameters(self):
        """
        Implemented in subclass
        :return:
        """
        self.engine_parameters()

    def create_analysis(self) -> Union[CreateCwlAnalysis, CreateNextflowAnalysis]:
        # Implemented in subclass
        raise NotImplementedError

    def launch_analysis(self) -> Analysis:
        # Implemented in subclass
        raise NotImplementedError

    def get_analysis_launch_json(self) -> Dict:
        """
        Save the analysis launch json
        :return:
        """
        return recursively_build_open_api_body_from_libica_item(
            self.analysis
        )

    def save_analysis(self, json_path: Path):
        """
        Save the analysis to a json file
        :param json_path:
        :return:
        """
        analysis_dict = self.get_analysis_launch_json()

        with open(json_path, "w") as f:
            f.write(
                json.dumps(
                    analysis_dict,
                    indent=2
                )
            )
