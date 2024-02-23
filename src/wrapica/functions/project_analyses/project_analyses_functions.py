#!/usr/bin/env python
from io import TextIOWrapper
from typing import Tuple, List, Union

from libica.openapi.v2.model.analysis import Analysis
from libica.openapi.v2.model.analysis_step import AnalysisStep
from libica.openapi.v2.model.analysis_step_logs import AnalysisStepLogs
from libica.openapi.v2.model.project_data import ProjectData


def get_outputs_object_from_analysis_id(
    project_id: str,
    analysis_id: str
) -> Tuple[str, List[ProjectData]]:
    """
    Query the outputs object from the analysis id
    :param project_id:
    :param analysis_id:
    :return:
    """
    raise NotImplementedError


def get_analysis_obj(project_id: str, analysis_id: str) -> Analysis:
    """
    Get an analysis object given a project id and analysis id
    :param project_id:
    :param analysis_id:
    :return:
    """
    raise NotImplementedError


def get_analysis_steps(project_id: str, analysis_id: str, include_technical_steps: bool = False) -> List[AnalysisStep]:
    """
    Get the workflow steps for a given analysis
    :param project_id:
    :param analysis_id:
    :return:
    """
    raise NotImplementedError


def get_analysis_log_from_analysis_step(analysis_step: AnalysisStep) -> AnalysisStepLogs:
    """
    Get the logs for a given analysis step
    :param analysis_step:
    :return:
    """
    raise NotImplementedError


def write_analysis_step_logs(project_id: str, step_logs: AnalysisStepLogs, output_path: Union[str | TextIOWrapper]):
    """
    Write the analysis step logs to a file
    :param project_id:
    :param step_logs:
    :param output_path:
    :return:
    """
    raise NotImplementedError


def get_run_folder_inputs_from_analysis_id(project_id: str, analysis_id: str) -> Tuple[str, List[ProjectData]]:
    """
    Get the run folder inputs for a given analysis
    :param project_id:
    :param analysis_id:
    :return:
    """
    raise NotImplementedError









