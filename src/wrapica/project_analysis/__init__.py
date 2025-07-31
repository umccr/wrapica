#!/usr/bin/env python

# Standard imports
from typing import Union

# Import libica models
from libica.openapi.v3.models import (
    AnalysisQueryParameters,
    AnalysisV3,
    AnalysisV4,
    AnalysisInput,
    AnalysisOutput,
    AnalysisOutputList,
    AnalysisStep,
    AnalysisStepLogs,
    CwlAnalysisInputJson,
    CwlAnalysisOutputJson,
    AnalysisStorageV3,
    AnalysisStorageV4
)

AnalysisType = Union[AnalysisV3, AnalysisV4]
AnalysisStorageType = Union[AnalysisStorageV3, AnalysisStorageV4]

# Import everything
from .functions.project_analysis_functions import (
    # Project Analysis functions
    get_project_analysis_inputs,
    get_analysis_input_object_from_analysis_input_code,
    get_outputs_object_from_analysis_id,
    get_analysis_output_object_from_analysis_output_code,
    get_cwl_outputs_json_from_analysis_id,
    get_analysis_obj_from_analysis_id,
    get_analysis_steps,
    get_analysis_log_from_analysis_step,
    write_analysis_step_logs,
    abort_analysis,
    list_analyses,
    get_cwl_analysis_input_json,
    get_cwl_analysis_output_json,
    analysis_step_to_dict,
    coerce_analysis_id_or_user_reference_to_analysis_id,
    get_analysis_obj_from_user_reference,
    coerce_analysis_id_or_user_reference_to_analysis_obj,
    update_analysis_obj,
    add_tag_to_analysis
)

__all__ = [
    # Models
    'AnalysisQueryParameters',
    'AnalysisType',
    'AnalysisV3',
    'AnalysisV4',
    'AnalysisInput',
    'AnalysisOutput',
    'AnalysisOutputList',
    'AnalysisStep',
    'AnalysisStepLogs',
    'CwlAnalysisInputJson',
    'CwlAnalysisOutputJson',
    'AnalysisStorageType',
    'AnalysisStorageV3',
    'AnalysisStorageV4',
    # Functions
    'get_project_analysis_inputs',
    'get_analysis_input_object_from_analysis_input_code',
    'get_outputs_object_from_analysis_id',
    'get_analysis_output_object_from_analysis_output_code',
    'get_cwl_outputs_json_from_analysis_id',
    'get_analysis_obj_from_analysis_id',
    'get_analysis_steps',
    'get_analysis_log_from_analysis_step',
    'write_analysis_step_logs',
    'abort_analysis',
    'list_analyses',
    'get_cwl_analysis_input_json',
    'get_cwl_analysis_output_json',
    'analysis_step_to_dict',
    'coerce_analysis_id_or_user_reference_to_analysis_id',
    'get_analysis_obj_from_user_reference',
    'coerce_analysis_id_or_user_reference_to_analysis_obj',
    'update_analysis_obj',
    'add_tag_to_analysis'
]
