Project Pipeline
=======================================

Classes
---------------

.. automodule:: wrapica.project_pipelines
    :undoc-members:
    :show-inheritance:
    :members:
      ICAv2AnalysisInput,
      ICAv2PipelineAnalysisTags,
      ICAv2EngineParameters,
      ICAv2CwlAnalysisJsonInput,
      ICAv2CWLEngineParameters,
      ICAv2CWLPipelineAnalysis,
      ICAv2NextflowAnalysisInput,
      ICAv2NextflowEngineParameters,
      ICAv2NextflowPipelineAnalysis

Functions
---------------

.. automodule:: wrapica.project_pipelines
    :members:
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
      get_activation_id,
      get_best_matching_entitlement_detail_for_cwl_analysis,
      get_best_matching_entitlement_detail_for_nextflow_analysis,
      create_cwl_input_json_analysis_obj,
      launch_cwl_workflow,
      launch_nextflow_workflow,
      get_project_pipeline_input_parameters,
      get_project_pipeline_configuration_parameters,
      convert_icav2_uris_to_data_ids_from_cwl_input_json,
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
      convert_uris_to_data_ids_from_nextflow_input_json
    :undoc-members:
    :show-inheritance:
    :exclude-members: