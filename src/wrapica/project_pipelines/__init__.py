#!/usr/bin/env python

# Standard imports
from typing import Union

from libica.openapi.v3 import ProjectPipelineV4
# Libica models
from libica.openapi.v3.models import (
    ActivationCodeDetail,
    AnalysisInputDataMount,
    AnalysisInputExternalData,
    AnalysisV3,
    AnalysisV4,
    AnalysisStorageV3,
    AnalysisStorageV4,
    CreateCwlAnalysis,
    CreateNextflowAnalysis,
    CwlAnalysisJsonInput,
    CwlAnalysisStructuredInput,
    InputParameter,
    InputParameterList,
    NextflowAnalysisInput,
    PipelineConfigurationParameter,
    PipelineConfigurationParameterList,
    PipelineFile,
    Project,
    ProjectData,
    ProjectPipeline,
    SearchMatchingActivationCodesForCwlAnalysis,
    SearchMatchingActivationCodesForNextflowAnalysis
)

# Import everything
from .functions.project_pipelines_functions import (
    # Project Data functions
    get_project_pipeline_obj,
    get_project_pipeline_obj_from_pipeline_code,
    get_project_pipeline_id_from_pipeline_code,
    get_default_analysis_storage_obj_from_project_pipeline,
    get_default_analysis_storage_id_from_project_pipeline,
    get_project_pipeline_description_from_pipeline_id,
    get_analysis_storage_id_from_analysis_storage_size,
    coerce_pipeline_id_or_code_to_project_pipeline_obj,
    get_analysis_storage_from_analysis_storage_id,
    get_analysis_storage_from_analysis_storage_size,
    coerce_analysis_storage_id_or_size_to_analysis_storage,
    create_cwl_input_json_analysis_obj,
    launch_cwl_workflow,
    launch_nextflow_workflow,
    get_project_pipeline_input_parameters,
    get_project_pipeline_configuration_parameters,
    convert_icav2_uris_to_data_ids_from_cwl_input_json,
    convert_uris_to_data_ids_from_cwl_input_json,
    list_project_pipelines,
    is_pipeline_in_project,
    list_projects_with_pipeline,
    create_blank_params_xml,
    create_params_xml,
    release_project_pipeline,
    update_pipeline_file,
    delete_pipeline_file,
    add_pipeline_file,
    create_cwl_project_pipeline,
    create_cwl_workflow_from_zip,
    create_nextflow_pipeline_from_zip,
    create_nextflow_pipeline_from_nf_core_zip,
    create_nextflow_project_pipeline,
)

from .classes.analysis import (
    # Wrapica classes
    ICAv2AnalysisInput,
    ICAv2PipelineAnalysisTags,
    ICAv2EngineParameters
)

from .classes.cwl_analysis import (
    # Wrapica classes
    ICAv2CwlAnalysisJsonInput,
    ICAv2CWLEngineParameters,
    ICAv2CWLPipelineAnalysis
)

from .classes.nextflow_analysis import (
    # Wrapica classes
    ICAv2NextflowAnalysisInput,
    ICAv2NextflowEngineParameters,
    ICAv2NextflowPipelineAnalysis
)

AnalysisType = Union[AnalysisV3, AnalysisV4]
AnalysisStorageType = Union[AnalysisStorageV3, AnalysisStorageV4]
ProjectPipelineType = Union[ProjectPipeline, ProjectPipelineV4]

__all__ = [
    # Libica models
    'ActivationCodeDetail',
    'AnalysisInputDataMount',
    'AnalysisInputExternalData',
    'AnalysisStorageType',
    'CreateCwlAnalysis',
    'CreateNextflowAnalysis',
    'CwlAnalysisJsonInput',
    'CwlAnalysisStructuredInput',
    'InputParameter',
    'InputParameterList',
    'NextflowAnalysisInput',
    'PipelineConfigurationParameter',
    'PipelineConfigurationParameterList',
    'PipelineFile',
    'Project',
    'ProjectData',
    'ProjectPipeline',
    'SearchMatchingActivationCodesForCwlAnalysis',
    'SearchMatchingActivationCodesForNextflowAnalysis',
    # Type Unions
    'AnalysisType',
    'AnalysisStorageType',
    'ProjectPipelineType',
    # Functions
    'get_project_pipeline_obj',
    'get_project_pipeline_obj_from_pipeline_code',
    'get_project_pipeline_id_from_pipeline_code',
    'get_default_analysis_storage_obj_from_project_pipeline',
    'get_default_analysis_storage_id_from_project_pipeline',
    'get_project_pipeline_description_from_pipeline_id',
    'get_analysis_storage_id_from_analysis_storage_size',
    'coerce_pipeline_id_or_code_to_project_pipeline_obj',
    'get_analysis_storage_from_analysis_storage_id',
    'get_analysis_storage_from_analysis_storage_size',
    'coerce_analysis_storage_id_or_size_to_analysis_storage',
    'create_cwl_input_json_analysis_obj',
    'launch_cwl_workflow',
    'launch_nextflow_workflow',
    'get_project_pipeline_input_parameters',
    'get_project_pipeline_configuration_parameters',
    'convert_icav2_uris_to_data_ids_from_cwl_input_json',
    'convert_uris_to_data_ids_from_cwl_input_json',
    'list_project_pipelines',
    'is_pipeline_in_project',
    'list_projects_with_pipeline',
    'create_blank_params_xml',
    'create_params_xml',
    'release_project_pipeline',
    'update_pipeline_file',
    'delete_pipeline_file',
    'add_pipeline_file',
    'create_cwl_project_pipeline',
    'create_cwl_workflow_from_zip',
    'create_nextflow_pipeline_from_zip',
    'create_nextflow_pipeline_from_nf_core_zip',
    'create_nextflow_project_pipeline',
    # classes
    'ICAv2AnalysisInput',
    'ICAv2PipelineAnalysisTags',
    'ICAv2EngineParameters',
    'ICAv2CwlAnalysisJsonInput',
    'ICAv2CWLEngineParameters',
    'ICAv2CWLPipelineAnalysis',
    'ICAv2NextflowAnalysisInput',
    'ICAv2NextflowEngineParameters',
    'ICAv2NextflowPipelineAnalysis',
]
