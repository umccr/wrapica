# Libica model imports
from libica.openapi.v2.models import (
    PipelineFile,
    PipelineV3,
    PipelineV4
)

from typing import Union

# Local imports
from .functions.pipelines_functions import (
    get_pipeline_obj_from_pipeline_id,
    get_pipeline_obj_from_pipeline_code,
    coerce_pipeline_id_or_code_to_pipeline_obj,
    coerce_pipeline_id_or_code_to_pipeline_id,
    list_all_pipelines,
    download_pipeline_file,
    list_pipeline_files,
    download_pipeline_to_directory,
    download_pipeline_to_zip,
    get_cwl_obj_from_pipeline_id
)

Pipeline = Union[PipelineV3, PipelineV4]

__all__ = [
    # Libica imports
    'Pipeline',
    'PipelineFile',
    # Local functions
    'get_pipeline_obj_from_pipeline_id',
    'get_pipeline_obj_from_pipeline_code',
    'coerce_pipeline_id_or_code_to_pipeline_obj',
    'coerce_pipeline_id_or_code_to_pipeline_id',
    'list_all_pipelines',
    'download_pipeline_file',
    'list_pipeline_files',
    'download_pipeline_to_directory',
    'download_pipeline_to_zip',
    'get_cwl_obj_from_pipeline_id'
]
