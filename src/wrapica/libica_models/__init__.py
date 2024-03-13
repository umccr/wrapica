#!/usr/bin/env python

"""
Models to easily import
"""
from typing import Union

from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.analysis_storage_api import AnalysisStorageApi
from libica.openapi.v2.api.entitlement_detail_api import EntitlementDetailApi
from libica.openapi.v2.api.project_analysis_api import ProjectAnalysisApi
from libica.openapi.v2.api.project_pipeline_api import ProjectPipelineApi

from libica.openapi.v2.model.activation_code_detail import ActivationCodeDetail
from libica.openapi.v2.model.analysis_v3 import AnalysisV3
from libica.openapi.v2.model.analysis_v4 import AnalysisV4
from libica.openapi.v2.model.analysis_input import AnalysisInput
from libica.openapi.v2.model.analysis_output import AnalysisOutput
from libica.openapi.v2.model.analysis_data_input import AnalysisDataInput
from libica.openapi.v2.model.analysis_input_data_mount import AnalysisInputDataMount
from libica.openapi.v2.model.analysis_input_external_data import AnalysisInputExternalData
from libica.openapi.v2.model.analysis_output_mapping import AnalysisOutputMapping
from libica.openapi.v2.model.analysis_parameter_input import AnalysisParameterInput
from libica.openapi.v2.model.analysis_step import AnalysisStep
from libica.openapi.v2.model.analysis_step_logs import AnalysisStepLogs
from libica.openapi.v2.model.analysis_tag import AnalysisTag
from libica.openapi.v2.model.aws_temp_credentials import AwsTempCredentials
from libica.openapi.v2.model.create_cwl_analysis import CreateCwlAnalysis
from libica.openapi.v2.model.create_data import CreateData
from libica.openapi.v2.model.create_nextflow_analysis import CreateNextflowAnalysis
from libica.openapi.v2.model.create_temporary_credentials import CreateTemporaryCredentials
from libica.openapi.v2.model.cwl_analysis_json_input import CwlAnalysisJsonInput
from libica.openapi.v2.model.cwl_analysis_structured_input import CwlAnalysisStructuredInput
from libica.openapi.v2.model.data_id_or_path_list import DataIdOrPathList
from libica.openapi.v2.model.data_url_with_path import DataUrlWithPath
from libica.openapi.v2.model.download import Download
from libica.openapi.v2.model.input_parameter import InputParameter
from libica.openapi.v2.model.input_parameter_list import InputParameterList
from libica.openapi.v2.model.nextflow_analysis_input import NextflowAnalysisInput
from libica.openapi.v2.model.pipeline_configuration_parameter import PipelineConfigurationParameter
from libica.openapi.v2.model.pipeline_configuration_parameter_list import PipelineConfigurationParameterList
from libica.openapi.v2.model.project_data import ProjectData
from libica.openapi.v2.model.project_paged_list import ProjectPagedList
from libica.openapi.v2.model.project_pipeline import ProjectPipeline
from libica.openapi.v2.model.project_pipeline_list import ProjectPipelineList
from libica.openapi.v2.model.search_matching_activation_codes_for_cwl_analysis import SearchMatchingActivationCodesForCwlAnalysis
from libica.openapi.v2.model.search_matching_activation_codes_for_nextflow_analysis import SearchMatchingActivationCodesForNextflowAnalysis
from libica.openapi.v2.model.temp_credentials import TempCredentials
from libica.openapi.v2.model.job import Job

Analysis = Union[AnalysisV3, AnalysisV4]
CwlAnalysisInput = Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput]