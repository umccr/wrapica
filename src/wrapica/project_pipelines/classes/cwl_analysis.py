#!/usr/bin/env python3

"""
Generate a CWL analysis
"""
# Imports
import json
from typing import List, Dict, Optional, Union
from pydantic import UUID4

# Libica imports
from libica.openapi.v3.models import (
    AnalysisV4 as Analysis,
    AnalysisInputDataMount,
    AnalysisInputExternalData,
    AnalysisOutputMapping,
    CreateCwlWithJsonInputAnalysis,
    CwlAnalysisWithJsonInput,
    CreateAnalysisLogs
)

# Local parent imports
from .analysis import (
    ICAv2PipelineAnalysisTags,
    ICAv2PipelineAnalysis,
    ICAv2EngineParameters,
    ICAv2AnalysisInput
)

# Local imports
from ...literals import WorkflowLanguageType, AnalysisStorageSizeType
from ...utils.logger import get_logger

# Set logger
logger = get_logger()


class ICAv2CwlAnalysisJsonInput(ICAv2AnalysisInput):
    """
    Generate a CWL Analysis input from a JSON input
    """

    def __init__(
        self,
        input_json: Dict
    ):
        # Initialise parent class
        super().__init__()

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

    def create_analysis_input(self) -> CwlAnalysisWithJsonInput:
        # Deference cwl input json
        self.deference_cwl_input_json()

        # Set input json to string
        self.set_input_json()

        # Generate a CWL analysis input
        return CwlAnalysisWithJsonInput(
            inputJson=self.input_json_str,
            mounts=self.mount_paths_list,
            externalData=self.external_mounts_list,
            dataIds=self.data_ids
        )

    def deference_cwl_input_json(self):
        from ..functions.project_pipelines_functions import (
            convert_uris_to_data_ids_from_cwl_input_json
        )
        self.input_json_deferenced_dict, self.mount_paths_list, self.external_mounts_list = convert_uris_to_data_ids_from_cwl_input_json(
              self.input_json
        )
        self.data_ids = list(
            map(
                lambda mount_path_iter: mount_path_iter.data_id,
                self.mount_paths_list
            )
        )

    def set_input_json(self):
        self.input_json_str = json.dumps(self.input_json_deferenced_dict, indent=2)


class ICAv2CWLEngineParameters(ICAv2EngineParameters):
    """
    The ICAv2 EngineParameters has the following properties
    """

    workflow_language: WorkflowLanguageType = "CWL"

    def __init__(
        self,
        project_id: Optional[Union[UUID4, str]] = None,
        pipeline_id: Optional[Union[UUID4, str]] = None,
        analysis_output: Optional[List[AnalysisOutputMapping]] = None,
        logs_output: Optional[CreateAnalysisLogs] = None,
        analysis_input: Optional[CwlAnalysisWithJsonInput] = None,
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
        analysis_storage_id: Optional[Union[UUID4, str]] = None,
        analysis_storage_size: Optional[AnalysisStorageSizeType] = None,
        cwltool_overrides: Optional[Dict] = None
    ):
        # Initialise parameters
        super().__init__(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_output=analysis_output,
            logs_output=logs_output,
            analysis_input=analysis_input,
            tags=tags,
            analysis_storage_id=analysis_storage_id,
            analysis_storage_size=analysis_storage_size
        )

        # Input parameters
        self.cwltool_overrides: Dict = cwltool_overrides


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
    analysis_storage_size: Optional[AnalysisStorageSize] = None

    """

    def __init__(
        self,
        # Launch parameters
        user_reference: str,
        project_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str],
        analysis_input: CwlAnalysisWithJsonInput,
        analysis_storage_id: Optional[Union[UUID4, str]] = None,
        analysis_storage_size: Optional[AnalysisStorageSizeType] = None,
        # Output parameters
        analysis_output_uri: Optional[str] = None,
        ica_logs_uri: Optional[str] = None,
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
        :param analysis_output_uri
        :param ica_logs_uri
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
            analysis_output_uri=analysis_output_uri,
            ica_logs_uri=ica_logs_uri,
            tags=tags
        )

    def set_engine_parameters(self):
        self.engine_parameters = ICAv2CWLEngineParameters(
            project_id=self.project_id,
            pipeline_id=self.pipeline_id,
            analysis_output=self.analysis_output,
            logs_output=self.logs,
            analysis_input=self.analysis_input,
            tags=self.tags,
            analysis_storage_id=self.analysis_storage_id,
            analysis_storage_size=self.analysis_storage_size,
            cwltool_overrides=self.cwltool_overrides
        )

    def create_analysis(self) -> CreateCwlWithJsonInputAnalysis:
        return CreateCwlWithJsonInputAnalysis(
            userReference=self.user_reference,
            pipelineId=self.pipeline_id,
            tags=self.engine_parameters.tags(),
            analysisInput=self.analysis_input,
            analysisStorageId=self.engine_parameters.analysis_storage_id,
            analysisOutput=self.engine_parameters.analysis_output,
            logs=self.engine_parameters.logs_output,
            outputParentFolderId=None,
        )

    def launch_analysis(self, idempotency_key: Optional[str] = None) -> Analysis:
        from ..functions.project_pipelines_functions import launch_cwl_workflow
        return launch_cwl_workflow(
            project_id=self.project_id,
            cwl_analysis=self.analysis,
            idempotency_key=idempotency_key
        )
