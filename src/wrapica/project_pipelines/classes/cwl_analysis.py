#!/usr/bin/env python3

"""
Generate a CWL analysis
"""
# Imports
import json
from pathlib import Path
from typing import List, Dict, Optional, Union

# Libica imports
from libica.openapi.v2.model.analysis_v3 import AnalysisV3
from libica.openapi.v2.model.analysis_v4 import AnalysisV4
from libica.openapi.v2.model.analysis_input_data_mount import AnalysisInputDataMount
from libica.openapi.v2.model.analysis_input_external_data import AnalysisInputExternalData
from libica.openapi.v2.model.analysis_output_mapping import AnalysisOutputMapping
from libica.openapi.v2.model.create_cwl_analysis import CreateCwlAnalysis
from libica.openapi.v2.model.cwl_analysis_structured_input import CwlAnalysisStructuredInput
from libica.openapi.v2.model.cwl_analysis_json_input import CwlAnalysisJsonInput

CwlAnalysisInput = Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput]

# Local parent imports
from .analysis import (
    ICAv2PipelineAnalysisTags,
    ICAv2PipelineAnalysis,
    ICAv2EngineParameters,
    ICAv2AnalysisInput
)

# Local imports
from ...enums import AnalysisStorageSize, WorkflowLanguage
from ...utils.logger import get_logger

Analysis = Union[AnalysisV3, AnalysisV4]

# Set logger
logger = get_logger()


class ICAv2CwlAnalysisJsonInput(ICAv2AnalysisInput):
    """
    Generate a CWL Analysis input from a JSON input
    """

    object_type = "JSON"

    def __init__(
        self,
        input_json: Dict
    ):
        # Initialise parent class
        super().__init__(
            object_type=self.object_type
        )

        # Set inputs
        self.input_json: Dict = input_json

        # Initialise values after dereferencing
        self.data_ids: List[str] = []
        self.input_json_deferenced_dict: Dict = {}
        self.mount_paths_list: List[AnalysisInputDataMount] = []
        self.external_mounts_list: List[AnalysisInputExternalData] = []

        # Generate typed values for creating analysis
        self.input_json_str: Optional[str] = None

    def validate_input(self):
        """
        Cannot yet validate the input as we don't have an easy way to collect a JSON schema for a CWL input
        :return:
        """
        pass

    def create_analysis_input(self) -> CwlAnalysisJsonInput:
        # Deference cwl input json
        self.deference_cwl_input_json()

        # Set input json to string
        self.set_input_json()

        # Generate a CWL analysis input
        return CwlAnalysisJsonInput(
            object_type=self.object_type,
            input_json=self.input_json_str,
            mounts=self.mount_paths_list,
            external_data=self.external_mounts_list
        )

    def deference_cwl_input_json(self):
        from ..functions.project_pipelines_functions import (
            convert_icav2_uris_to_data_ids_from_cwl_input_json
        )
        self.input_json_deferenced_dict, self.mount_paths_list, self.external_mounts_list = convert_icav2_uris_to_data_ids_from_cwl_input_json(
              self.input_json
        )

    def set_input_json(self):
        self.input_json_str = json.dumps(self.input_json_deferenced_dict, indent=2)


class ICAv2CWLEngineParameters(ICAv2EngineParameters):
    """
    The ICAv2 EngineParameters has the following properties
    """

    workflow_language = WorkflowLanguage.CWL

    def __init__(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        output_parent_folder_id: Optional[str] = None,
        output_parent_folder_path: Optional[Path] = None,
        analysis_output: Optional[List[AnalysisOutputMapping]] = None,
        analysis_input: Optional[CwlAnalysisInput] = None,
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        cwltool_overrides: Optional[Dict] = None
    ):
        # Initialise parameters
        super().__init__(
            project_id=project_id,
            pipeline_id=pipeline_id,
            output_parent_folder_id=output_parent_folder_id,
            output_parent_folder_path=output_parent_folder_path,
            analysis_output=analysis_output,
            analysis_input=analysis_input,
            tags=tags,
            analysis_storage_id=analysis_storage_id,
            analysis_storage_size=analysis_storage_size,
            activation_id=activation_id
        )

        # Input parameters
        self.cwltool_overrides: Dict = cwltool_overrides
        # self.stream_all_files: Optional[bool] = stream_all_files
        # self.stream_all_directories: Optional[bool] = stream_all_directories

    def get_overrides_from_engine_parameters(self, inputs_overrides: Optional[Dict] = None) -> Dict:
        """
        Get the overrides from the engine parameters.
        Note overrides in the inputjson should NOT be overwritten
        :param inputs_overrides:
        :return:
        """

        # If overrides are in the engine parameters, put them in the input json
        if inputs_overrides is None:
            inputs_overrides = {}

        elif inputs_overrides is not None and not isinstance(inputs_overrides, Dict):
            logger.error(f"inputs_overrides was not a dict, instead type {type(inputs_overrides)}")
            raise TypeError("inputs_overrides was not a dict")

        # Initialise the engine parameters cwltool overrides dict
        engine_parameter_cwltool_overrides = self.cwltool_overrides.copy()

        # If there are no engine parameter overrides, return the input json overrides
        if engine_parameter_cwltool_overrides is None or len(engine_parameter_cwltool_overrides) == 0:
            return inputs_overrides

        # Don't override existing overrides in the input json
        # Instead overwrite engine parameter cwltool overrides
        for key, value in inputs_overrides.items():
            if key in engine_parameter_cwltool_overrides.keys():
                engine_parameter_cwltool_overrides[key].update(value)
            else:
                engine_parameter_cwltool_overrides[key] = value

        # Then write them back in here again
        # Now we pull them back in again
        for key, value in engine_parameter_cwltool_overrides.items():
            inputs_overrides[key] = value

        return inputs_overrides


class ICAv2CWLPipelineAnalysis(ICAv2PipelineAnalysis):
    """
    The ICAv2CWLPipelineAnalysis has the following properties
        * user_reference: str
        * input_json: Dict  (cwl_inputs)
        * engine_parameters: ICAv2EngineParameters

    The engineParameters then populate the following parameters for its parent class

    pipeline_id: str,
    tags: ICAv2PipelineAnalysisTags,
    analysis_input: Union[CwlAnalysisJsonInput, NextflowAnalysisInput],
    analysis_output: str,
    activation_code_details_id: Optional[str] = None,
    analysis_storage_size: Optional[AnalysisStorageSize] = None

    """

    def __init__(
        self,
        # Launch parameters
        user_reference: str,
        project_id: str,
        pipeline_id: str,
        analysis_input: CwlAnalysisInput,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        # Output parameters
        output_parent_folder_id: Optional[str] = None,
        output_parent_folder_path: Optional[str] = None,
        analysis_output_uri: Optional[str] = None,
        # Meta parameters
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
        # CWL Specific parameters
        cwltool_overrides: Optional[Dict] = None
    ):
        """
        Initialise input
        :param user_reference
        :param project_id
        :param pipeline_id
        :param analysis_input
        :param analysis_storage_id
        :param analysis_storage_size
        :param activation_id
        :param output_parent_folder_id
        :param output_parent_folder_path
        :param analysis_output_uri
        :param tags
        :param cwltool_overrides
        """
        # Initialise any cwl specific parameters first before calling the parent class
        # Set cwl specific inputs
        self.cwltool_overrides: Optional[Dict] = cwltool_overrides
        # Set under parent init script through set_engine_parameters
        self.engine_parameters: Optional[ICAv2CWLEngineParameters] = None

        # Call parent class
        super().__init__(
            user_reference=user_reference,
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_input=analysis_input,
            analysis_storage_id=analysis_storage_id,
            analysis_storage_size=analysis_storage_size,
            activation_id=activation_id,
            output_parent_folder_id=output_parent_folder_id,
            output_parent_folder_path=output_parent_folder_path,
            analysis_output_uri=analysis_output_uri,
            tags=tags
        )

    def set_engine_parameters(self):
        self.engine_parameters = ICAv2CWLEngineParameters(
            project_id=self.project_id,
            pipeline_id=self.pipeline_id,
            output_parent_folder_id=self.output_parent_folder_id,
            output_parent_folder_path=self.output_parent_folder_path,
            analysis_output=self.analysis_output,
            analysis_input=self.analysis_input,
            tags=self.tags,
            analysis_storage_id=self.analysis_storage_id,
            analysis_storage_size=self.analysis_storage_size,
            activation_id=self.activation_id,
            cwltool_overrides=self.cwltool_overrides
        )

    def create_analysis(self) -> CreateCwlAnalysis:
        return CreateCwlAnalysis(
            user_reference=self.user_reference,
            pipeline_id=self.pipeline_id,
            tags=self.engine_parameters.tags(),
            activation_code_detail_id=self.engine_parameters.activation_id,
            analysis_input=self.analysis_input,
            analysis_storage_id=self.engine_parameters.analysis_storage_id,
            output_parent_folder_id=self.engine_parameters.output_parent_folder_id,
            analysis_output=self.engine_parameters.analysis_output
        )

    def launch_analysis(self) -> Analysis:
        from ..functions.project_pipelines_functions import launch_cwl_workflow
        return launch_cwl_workflow(
            project_id=self.project_id,
            cwl_analysis=self.analysis,
        )
