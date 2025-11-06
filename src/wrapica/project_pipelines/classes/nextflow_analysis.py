#!/usr/bin/env python3

"""
Nextflow analysis
"""
# Standard imports
import json
from typing import List, Dict, Optional, Union
from pydantic import UUID4

# Libica imports
from libica.openapi.v3 import (
    AnalysisInputDataMount, AnalysisInputExternalData, NextflowAnalysisWithCustomInput,
    AnalysisOutputMapping, CreateNextflowWithCustomInputAnalysis,
    AnalysisV4 as Analysis, CreateAnalysisLogs
)

# Local parent imports
from .analysis import (
    ICAv2PipelineAnalysisTags,
    ICAv2PipelineAnalysis,
    ICAv2EngineParameters,
    ICAv2AnalysisInput
)

# Local imports
from ...literals import (
    WorkflowLanguageType, AnalysisStorageSizeType,
)
from ...utils.logger import get_logger


# Set logger
logger = get_logger()


class ICAv2NextflowAnalysisInput(ICAv2AnalysisInput):
    """
    Generate a Nextflow Analysis input from a JSON input - 
    if the input is an icav2 uri, it is considered an analysis data input
    """

    def __init__(
        self,
        input_json: Dict,
        cache_uri: Optional[str] = None,
    ):
        # Initialise parent class
        super().__init__()

        # Get the cache uri from the input_json if it exists
        # Required if the nextflow pipeline has a parameter called 'input' that is a list of objects
        self.cache_uri: Optional[str] = cache_uri

        # Set inputs
        self.input_json: Dict = input_json

        # Initialise values after dereferencing
        self.data_ids: List[str] = []
        self.input_json_deferenced_dict: Dict = {}
        self.mount_paths_list: List[AnalysisInputDataMount] = []
        self.external_mounts_list: List[AnalysisInputExternalData] = []
        self.input_json_str: Optional[str] = None

    def validate_input(self):
        """
        Cannot yet validate the input as we dont have an easy way to collect a JSON schema for the Nextflow pipeline.
        We can do this for non-proprietary pipelines, but not for proprietary pipelines.
        User is expected to know the input format of the Nextflow pipeline they are using.
        :return:
        """
        pass

    def create_analysis_input(self) -> NextflowAnalysisWithCustomInput:
        # Deference cwl input json
        self.deference_nextflow_input_json()

        # Set input json to string
        self.set_input_json()

        self.validate_input()

        # Generate a CWL analysis input
        return NextflowAnalysisWithCustomInput(
            customInput=self.input_json_str,
            dataIds=self.data_ids,
            mounts=self.mount_paths_list,
            externalData=self.external_mounts_list
        )

    def deference_nextflow_input_json(self):
        from ..functions.project_pipelines_functions import (
            convert_uris_to_data_ids_from_nextflow_input_json
        )
        self.input_json_deferenced_dict, self.mount_paths_list, self.external_mounts_list = convert_uris_to_data_ids_from_nextflow_input_json(
              self.input_json,
              cache_uri=self.cache_uri
        )
        self.data_ids = list(
            map(
                lambda mount_path_iter: mount_path_iter.data_id,
                self.mount_paths_list
            )
        )

    def set_input_json(self):
        if not 'outdir' in self.input_json_deferenced_dict.keys():
            self.input_json_deferenced_dict['outdir'] = 'out/'

        self.input_json_str = json.dumps(self.input_json_deferenced_dict, indent=2)


class ICAv2NextflowEngineParameters(ICAv2EngineParameters):
    """
    The ICAv2 EngineParameters has the following properties
    """

    workflow_language: WorkflowLanguageType = "NEXTFLOW"

    def __init__(
        self,
        project_id: Optional[Union[UUID4, str]] = None,
        pipeline_id: Optional[Union[UUID4, str]] = None,
        analysis_output: Optional[List[AnalysisOutputMapping]] = None,
        logs_output: Optional[CreateAnalysisLogs] = None,
        analysis_input: Optional[NextflowAnalysisWithCustomInput] = None,
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
        analysis_storage_id: Optional[Union[UUID4, str]] = None,
        analysis_storage_size: Optional[AnalysisStorageSizeType] = None,
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
            analysis_storage_size=analysis_storage_size,
        )


class ICAv2NextflowPipelineAnalysis(ICAv2PipelineAnalysis):
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
        analysis_input: NextflowAnalysisWithCustomInput,
        analysis_storage_id: Optional[Union[UUID4, str]] = None,
        analysis_storage_size: Optional[AnalysisStorageSizeType] = None,
        # Output parameters
        analysis_output_uri: Optional[str] = None,
        ica_logs_uri: Optional[str] = None,
        # Meta parameters
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
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
        """
        # Set under parent init script through set_engine_parameters
        self.engine_parameters: Optional[ICAv2NextflowEngineParameters] = None

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
        self.engine_parameters = ICAv2NextflowEngineParameters(
            project_id=self.project_id,
            pipeline_id=self.pipeline_id,
            analysis_output=self.analysis_output,
            logs_output=self.logs,
            analysis_input=self.analysis_input,
            tags=self.tags,
            analysis_storage_id=self.analysis_storage_id,
            analysis_storage_size=self.analysis_storage_size,
        )

    def create_analysis(self) -> CreateNextflowWithCustomInputAnalysis:
        return CreateNextflowWithCustomInputAnalysis(
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
        from ..functions.project_pipelines_functions import launch_nextflow_workflow
        return launch_nextflow_workflow(
            project_id=self.project_id,
            nextflow_analysis=self.analysis,
            idempotency_key=idempotency_key
        )
