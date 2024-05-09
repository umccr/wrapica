#!/usr/bin/env python3

"""
Nextflow analysis
"""

# Imports
from pathlib import Path
from typing import List, Dict, Optional, Union

# Libica imports
from libica.openapi.v2.model.analysis_data_input import AnalysisDataInput
from libica.openapi.v2.model.analysis_output_mapping import AnalysisOutputMapping
from libica.openapi.v2.model.analysis_parameter_input import AnalysisParameterInput
from libica.openapi.v2.model.analysis_v3 import AnalysisV3
from libica.openapi.v2.model.analysis_v4 import AnalysisV4
from libica.openapi.v2.model.create_nextflow_analysis import CreateNextflowAnalysis
from libica.openapi.v2.model.input_parameter import InputParameter
from libica.openapi.v2.model.nextflow_analysis_input import NextflowAnalysisInput
from libica.openapi.v2.model.pipeline_configuration_parameter import PipelineConfigurationParameter

# Local parent imports
from .analysis import (
    ICAv2PipelineAnalysisTags,
    ICAv2PipelineAnalysis,
    ICAv2EngineParameters,
    ICAv2AnalysisInput
)

# Local imports
from ...enums import (
    AnalysisStorageSize, WorkflowLanguage,
    StructuredInputParameterType, StructuredInputParameterTypeMapping
)
from ...utils.logger import get_logger

Analysis = Union[AnalysisV3, AnalysisV4]

# Set logger
logger = get_logger()


class ICAv2NextflowAnalysisInput(ICAv2AnalysisInput):
    """
    Generate a Nextflow Analysis input from a JSON input - 
    if the input is an icav2 uri, it is considered an analysis data input
    """

    object_type = "STRUCTURED"

    def __init__(
        self,
        input_json: Dict,
        pipeline_id: str,
        project_id: str
    ):
        # Initialise parent class
        super().__init__(
            object_type=self.object_type
        )

        # Set inputs
        self.input_json: Dict = input_json

        # Insert pipeline id and project id
        self.pipeline_id: str = pipeline_id
        self.project_id: str = project_id

        # Split input types
        self.inputs: List[AnalysisDataInput] = []
        self.parameters: List[AnalysisParameterInput] = []

    def validate_input(self):
        """
        Validate the input parameters much their expected inputs from the pipeline
        :return:
        """
        from ..functions.project_pipelines_functions import (
            get_project_pipeline_input_parameters,
            get_project_pipeline_configuration_parameters
        )

        # Get the pipeline input parameters
        pipeline_input_parameters: List[InputParameter] = get_project_pipeline_input_parameters(
            project_id=self.project_id,
            pipeline_id=self.pipeline_id
        )

        # Get the pipeline configuration parameters
        pipeline_configuration_parameters: List[PipelineConfigurationParameter] = (
            get_project_pipeline_configuration_parameters(
                project_id=self.project_id,
                pipeline_id=self.pipeline_id
            )
        )

        # With the input parameters, we can validate against the input json
        errors_list = []
        for input_parameter in pipeline_input_parameters:
            # Check required inputs
            if input_parameter.required:
                if not any(
                    list(
                        map(
                            lambda input_iter: input_iter.parameter_code == input_parameter.code,
                            self.inputs
                        )
                    )
                ):
                    logger.error(f"Required input parameter {input_parameter.code} not found in input json")
                    errors_list.append(f"Required input parameter {input_parameter.code} not found in input json")

            # Check if any multi-values allow for multiple values
            if not input_parameter.multi_value:
                if (
                    len(
                        list(
                            filter(
                                lambda input_iter: input_iter.parameter_code == input_parameter.code,
                                self.inputs
                            )
                        )
                    ) > 1
                ):
                    logger.error(f"non-multiple input parameter {input_parameter.code} specified multiple times")
                    errors_list.append(f"non-multiple input parameter {input_parameter.code} specified multiple times")

        # Iterate through configuration parameters
        for configuration_parameter in pipeline_configuration_parameters:
            # Check if required parameter is in configuration list
            if configuration_parameter.required:
                if not any(
                    list(
                        map(
                            lambda parameter_iter: parameter_iter.code == configuration_parameter.code,
                            self.parameters
                        )
                    )
                ):
                    logger.error(f"Required configuration parameter {configuration_parameter.code} not found in input json")
                    errors_list.append(f"Required configuration parameter {configuration_parameter.code} not found in input json")

            # Check if any multi-values allow for multiple values
            if not configuration_parameter.multi_value:
                if (
                    len(
                        list(
                            filter(
                                lambda parameter_iter: parameter_iter.code == configuration_parameter.code,
                                self.parameters
                            )
                        )
                    ) > 1
                ):
                    logger.error(f"non-multiple configuration parameter {configuration_parameter.code} specified multiple times")
                    errors_list.append(f"non-multiple configuration parameter {configuration_parameter.code} specified multiple times")

            # Check type of parameter matches the type in the configuration
            for parameter in self.parameters:
                if parameter.code == configuration_parameter.code:
                    if not (
                        # Check if the parameter value is of the expected configuration parameter type
                        isinstance(
                            parameter.value,
                            StructuredInputParameterTypeMapping[
                                StructuredInputParameterType(
                                    configuration_parameter.type
                                ).name
                            ].value
                        )
                    ):
                        logger.error(f"Parameter {parameter.code} is not of type {configuration_parameter.type}")
                        errors_list.append(f"Parameter {parameter.code} is not of type {configuration_parameter.type}")

            if len(errors_list) == 0:
                logger.info("Inputs validation passed")
                return

            logger.error("Inputs validation failed")
            for error in errors_list:
                logger.error(error)
            raise ValueError("Inputs validation failed")

    def create_analysis_input(self) -> NextflowAnalysisInput:
        # Set input json to string
        self.split_input_json_by_inputs_and_parameters()

        self.validate_input()

        # Generate a CWL analysis input
        return NextflowAnalysisInput(
            inputs=self.inputs,
            parameters=self.parameters,
        )

    def split_input_json_by_inputs_and_parameters(self):
        # Local imports for functions
        from ...project_data import convert_icav2_uri_to_data_obj
        for key, value in self.input_json.items():

            if isinstance(value, List):
                if len(value) == 0:
                    continue
                if value[0].startswith("icav2://"):
                    self.inputs.append(
                        AnalysisDataInput(
                            parameter_code=key,
                            data_ids=list(
                                map(
                                    lambda icav2_uri_iter: convert_icav2_uri_to_data_obj(icav2_uri_iter).data.id,
                                    value
                                )
                            )
                        )
                    )
                else:
                    self.parameters.append(
                        AnalysisParameterInput(
                            code=key,
                            multi_value=value,
                        )
                    )
            else:
                if isinstance(value, str) and value.startswith("icav2://"):
                    self.inputs.append(
                        AnalysisDataInput(
                            parameter_code=key,
                            data_ids=[convert_icav2_uri_to_data_obj(value).data.id]
                        )
                    )
                else:
                    self.parameters.append(
                        AnalysisParameterInput(
                            code=key,
                            value=str(value)
                        )
                    )


class ICAv2NextflowEngineParameters(ICAv2EngineParameters):
    """
    The ICAv2 EngineParameters has the following properties
    """

    workflow_language = WorkflowLanguage.NEXTFLOW

    def __init__(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        output_parent_folder_id: Optional[str] = None,
        output_parent_folder_path: Optional[Path] = None,
        analysis_output: Optional[List[AnalysisOutputMapping]] = None,
        analysis_input: Optional[NextflowAnalysisInput] = None,
        tags: Optional[ICAv2PipelineAnalysisTags] = None,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
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
    activation_code_details_id: Optional[str] = None,
    analysis_storage_size: Optional[AnalysisStorageSize] = None

    """

    def __init__(
        self,
        # Launch parameters
        user_reference: str,
        project_id: str,
        pipeline_id: str,
        analysis_input: NextflowAnalysisInput,
        analysis_storage_id: Optional[str] = None,
        analysis_storage_size: Optional[AnalysisStorageSize] = None,
        activation_id: Optional[str] = None,
        # Output parameters
        output_parent_folder_id: Optional[str] = None,
        output_parent_folder_path: Optional[str] = None,
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
        :param activation_id
        :param output_parent_folder_id
        :param output_parent_folder_path
        :param analysis_output_uri
        :param ica_logs_uri
        :param tags
        :param cwltool_overrides
        """
        # Initialise any cwl specific parameters first before calling the parent class
        # Set cwl specific inputs
        self.cwltool_overrides: Optional[Dict] = cwltool_overrides
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
            activation_id=activation_id,
            output_parent_folder_id=output_parent_folder_id,
            output_parent_folder_path=output_parent_folder_path,
            analysis_output_uri=analysis_output_uri,
            ica_logs_uri=ica_logs_uri,
            tags=tags
        )

    def set_engine_parameters(self):
        self.engine_parameters = ICAv2NextflowEngineParameters(
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
        )

    def create_analysis(self) -> CreateNextflowAnalysis:
        return CreateNextflowAnalysis(
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
        from ..functions.project_pipelines_functions import launch_nextflow_workflow
        return launch_nextflow_workflow(
            project_id=self.project_id,
            nextflow_analysis=self.analysis,
        )
