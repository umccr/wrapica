#!/usr/bin/env python

# External imports
import json
from pathlib import Path
from typing import List, Dict, Union
from xml.dom.minidom import Document
import pandas as pd
from libica.openapi.v3 import ApiClient
from ruamel.yaml import CommentedMap, CommentedSeq
from pydantic import UUID4


# Libica imports
from libica.openapi.v3.models import (
    PipelineLanguageVersion
)
from libica.openapi.v3.api.pipeline_language_api import PipelineLanguageApi

# Wrapica imports
from .configuration import get_icav2_configuration
from .globals import NEXTFLOW_PROCESS_LABEL_RE_OBJ, NEXTFLOW_TASK_POD_MAPPING, DEFAULT_NEXTFLOW_VERSION


def convert_base_config_to_icav2_base_config(base_config_path: Path):
    """
    Given a base config file, overwrite it to an ICA base config file
    By performing the following operations

    withLabel:process_single {
        cpus   = { ... }
        memory = { ... }
        time   = { ... }
    }

    Converted to

    withLabel:process_single {
        cpus   = { ... }
        memory = { ... }
        time   = { ... }
        pod = [ annotation: 'scheduler.illumina.com/presetSize' , value: 'standard-small' ]
    }

    With the following label mapping
    process_single -> standard-small
    process_low -> standard-medium
    process_high -> standard-xlarge
    process_high_memory  -> himem-large

    :param base_config_path:
    :return:
    """
    with open(base_config_path, 'r') as base_config_file:
        base_config = base_config_file.readlines()

    new_base_config = []
    process_label = None
    for line in base_config:
        if NEXTFLOW_PROCESS_LABEL_RE_OBJ.match(line.strip()) is not None:
            process_label = NEXTFLOW_PROCESS_LABEL_RE_OBJ.match(line.strip()).group(1)
        if process_label is not None and line.strip() == '}' and process_label in NEXTFLOW_TASK_POD_MAPPING.keys():
            new_base_config.extend([
                f'        // Added by Wrapica to allow usability icav2\n',
                f'        pod = [ annotation: \'scheduler.illumina.com/presetSize\' , value: \'{NEXTFLOW_TASK_POD_MAPPING[process_label]}\' ]\n',
                line
            ])
            process_label = None
        else:
            new_base_config.append(line)

    # This configuration should also set the base params and docker params
    new_base_config.extend(
        """
// Added by Wrapica to allow usability icav2
params {
    outdir = 'out'  
    publish_dir_mode = 'copy'  
    igenomes_ignore = false  
    enable_conda = false  
    email_on_fail = null  
    email = null  
    ica_smoke_test = false
}

// Edited by Wrapica to allow usability icav2
docker {
    enabled = true
}

        """
    )

    with open(base_config_path, 'w') as new_base_config_file:
        new_base_config_file.writelines(new_base_config)


def write_params_xml_from_nextflow_schema_json(
        nextflow_schema_json_path: Path,
        params_xml_path: Path,
        pipeline_name: str,
        pipeline_version: str
):
    """
    Get the params xml from the nextflow json input file (not the assets/schema_input.json file)
    :param nextflow_schema_json_path:
    :param params_xml_path:
    :param pipeline_name:
    :param pipeline_version:
    :return:
    """

    # Initialise the xml doc
    doc = Document()

    # Create root element
    pipeline = doc.createElement('pd:pipeline')
    pipeline.setAttribute('xmlns:pd', 'xsd://www.illumina.com/ica/cp/pipelinedefinition')
    pipeline.setAttribute('code', pipeline_name)
    pipeline.setAttribute('version', pipeline_version)
    doc.appendChild(pipeline)

    # Create dataInputs element
    data_inputs_element = doc.createElement('pd:dataInputs')
    pipeline.appendChild(data_inputs_element)

    # Create the steps elements
    steps = doc.createElement('pd:steps')
    pipeline.appendChild(steps)

    # Load nextflow_schema_json_path as a dict
    with open(nextflow_schema_json_path, 'r') as nextflow_schema_json_file:
        nextflow_schema_json = json.load(nextflow_schema_json_file)

    # Get the input output options from the nextflow schema json
    # And filter out the generic options
    all_options = dict(
        filter(
            # We don't want the generic options though
            lambda kv: kv[0] not in ["generic_options", "institutional_config_options"],
            nextflow_schema_json.get("definitions").items()
        )
    )

    # Get the params from the input output options
    for option_category_name, option_category_schema_definition in all_options.items():
        property_definitions = option_category_schema_definition.get("properties", {})
        required_property_names = option_category_schema_definition.get("required", [])
        # Create step element
        category_step = doc.createElement('pd:step')
        steps.appendChild(category_step)
        category_step.setAttribute('execution', 'MANDATORY')
        category_step.setAttribute('code', option_category_name)

        # Add label and description
        category_label_element = doc.createElement('pd:label')
        category_label_element.appendChild(doc.createTextNode(option_category_name))
        category_step.appendChild(category_label_element)
        category_description_element = doc.createElement('pd:description')
        if not option_category_schema_definition.get("description", "") == "":
            category_description_element.appendChild(doc.createTextNode(option_category_schema_definition.get("description", "")))
        else:
            category_description_element.appendChild(doc.createTextNode(option_category_name))
        category_step.appendChild(category_description_element)

        # Create the tool element
        tool_element = doc.createElement('pd:tool')
        tool_element.setAttribute('code', option_category_name)
        category_step.appendChild(tool_element)

        for property_name, property_schema_dict in property_definitions.items():
            if property_name == "email":
                # We don't want to send an email at the end of the pipeline
                continue
            if property_name == "outdir":
                # We hardcode this to 'out', so we don't make it available in the params xml
                continue
            if property_name in ["config_profile_name", "config_profile_description"]:
                # Don't need these
                continue
            # Data type
            if "type" not in property_schema_dict.keys():
                continue
            if (
                    property_schema_dict["type"] == "string" and
                    (
                            # Format hint
                            (
                                    property_schema_dict.get("format", None) is not None
                                    and (
                                            property_schema_dict.get("format") == "file-path" or
                                            property_schema_dict.get("format") == "directory-path"
                                    )
                            ) or
                            (
                                    property_schema_dict.get("mimetype", None) is not None
                                    and (
                                            property_schema_dict.get("mimetype") == "text/csv" or
                                            property_schema_dict.get("mimetype") == "text/tsv"
                                    )
                            )
                    )
            ):
                # We have a file object that needs to be added
                # Property schema looks like this:
                # {
                #    "type": "string",
                #    "format": "file-path",
                #    "exists": true,
                #    "schema": "assets/schema_input.json",
                #    "mimetype": "text/csv",
                #    "pattern": "^\\S+\\.tsv$",
                #    "description": "Path to comma-separated file containing information about the samples in the experiment.",
                #    "help_text": "You will need to create a design file with information about the samples in your experiment before running the pipeline. Use this parameter to specify its location. It has to be a comma-separated file with 3 columns, and a header row. See [usage docs](https://nf-co.re/airrflow/usage#samplesheet-input).",
                #    "fa_icon": "fas fa-file-csv"
                # }
                data_input_element = doc.createElement('pd:dataInput')
                data_inputs_element.appendChild(data_input_element)

                data_input_element.setAttribute('code', property_name)
                data_input_element.setAttribute('format', (
                    "UNKNOWN" if (
                            not property_schema_dict.get("mimetype") or
                            property_schema_dict.get("mimetype").split("/")[-1].upper() == "PLAIN"
                    )
                    else property_schema_dict.get("mimetype").split("/")[-1].upper()
                ))
                data_input_element.setAttribute('type', (
                    "FILE"
                    if property_schema_dict.get("mimetype") is not None
                       or property_schema_dict.get("format") == "file-path"
                    else "DIRECTORY"
                ))
                data_input_element.setAttribute('required', (
                    "true"
                    if property_name in required_property_names
                    else "false"
                ))
                # We don't allow multi-values just yet
                data_input_element.setAttribute('multiValue', "false")

                label_element = doc.createElement('pd:label')
                label_element.appendChild(doc.createTextNode(property_name))
                data_input_element.appendChild(label_element)

                description_element = doc.createElement('pd:description')
                description_element.appendChild(doc.createTextNode(property_schema_dict.get("description", "")))
                data_input_element.appendChild(description_element)

            # Parameter type
            else:
                parameter_element = doc.createElement('pd:parameter')
                tool_element.appendChild(parameter_element)

                parameter_element.setAttribute('code', property_name)
                parameter_element.setAttribute('minValues', (
                    "1"
                    if property_name in required_property_names
                    else "0"
                ))
                # We don't allow multi-values just yet
                parameter_element.setAttribute('maxValues', "1")
                # Always USER, would love to get some documentation on this one day
                parameter_element.setAttribute('classification', "USER")

                # Set label and description as childs
                label_element = doc.createElement('pd:label')
                label_element.appendChild(doc.createTextNode(property_name))
                parameter_element.appendChild(label_element)
                description_element = doc.createElement('pd:description')
                description_element.appendChild(doc.createTextNode(property_schema_dict.get("description", "")))
                parameter_element.appendChild(description_element)

                # Add type
                if (
                        property_schema_dict["type"] == "string" and
                        property_schema_dict.get("enum", None) is not None
                ):
                    options_type = doc.createElement("pd:optionsType")
                    parameter_element.appendChild(options_type)

                    for option in property_schema_dict.get("enum"):
                        option_type = doc.createElement("pd:option")
                        option_type.appendChild(doc.createTextNode(str(option)))
                        options_type.appendChild(option_type)
                elif (
                        property_schema_dict["type"] == "string"
                ):
                    string_type = doc.createElement("pd:stringType")
                    parameter_element.appendChild(string_type)
                elif (
                        property_schema_dict["type"] == "boolean"
                ):
                    boolean_type = doc.createElement("pd:booleanType")
                    parameter_element.appendChild(boolean_type)
                elif (
                        property_schema_dict["type"] == "integer"
                ):
                    integer_type = doc.createElement("pd:integerType")
                    parameter_element.appendChild(integer_type)
                elif (
                        property_schema_dict["type"] == "number"
                ):
                    float_type = doc.createElement("pd:floatType")
                    parameter_element.appendChild(float_type)

                # Add default
                if "default" in property_schema_dict.keys():
                    default_element = doc.createElement("pd:value")
                    default_element.appendChild(doc.createTextNode(str(property_schema_dict.get("default"))))
                    parameter_element.appendChild(default_element)

            # Put it all together
            with open(params_xml_path, 'w') as params_xml_file_h:
                params_xml_file_h.write(doc.toprettyxml(indent="  ", encoding="UTF-8", standalone=True).decode())
            #params_xml_file_h.write("\n")


def generate_samplesheet_file_from_input_dict(
        samplesheet_dict: List[Dict],
        schema_input_path: Path,
        samplesheet_path: Path
):
    """
    Generate the samplesheet csv from the samplesheet dict
    :param samplesheet_dict:
    :param schema_input_path:
    :param samplesheet_path:
    :return:
    """
    from ..project_data import convert_icav2_uri_to_project_data_obj, create_download_url

    # Collect the samplesheet list as a dataframe
    samplesheet_df = pd.DataFrame(samplesheet_dict)

    # Write the samplesheet to a csv
    # But note that any icav2 file path should first be presigned before being written to the samplesheet
    with open(schema_input_path, 'r') as schema_input_path_h:
        schema_input_dict = json.load(schema_input_path_h)

    # Get schema input properties
    required_columns = schema_input_dict.get("items", {}).get("required", [])

    # Check all required columns are present in samplesheet_df columns
    for required_column in required_columns:
        if required_column not in samplesheet_df.columns:
            raise ValueError(f"Required column {required_column} not found in samplesheet")

    # Collect all ICAv2 URIs, presign them and write them back to the samplesheet
    icav2_uris_to_presign = []
    for index, row in samplesheet_df.iterrows():
        for column in samplesheet_df.columns:
            if row[column].startswith("icav2://"):
                if ';' in row[column]:
                    # We have multiple URIs
                    icav2_uris_to_presign.extend(row[column].split(';'))
                else:
                    # Presign the URI
                    icav2_uris_to_presign.append(row[column])

    # Presign the URIs
    icav2_uris_to_presign = list(set(icav2_uris_to_presign))

    # Generate project data objects for each URI
    icav2_uris_to_project_data_obj_map = dict(
        map(
            lambda icav2_uri: (icav2_uri, convert_icav2_uri_to_project_data_obj(icav2_uri)),
            icav2_uris_to_presign
        )
    )

    # Specify the presigned url to download
    icav2_uris_to_presigned_url = dict(
        map(
            lambda icav2_uri_kv: (icav2_uri_kv[0], create_download_url(icav2_uri_kv[1].project_id, icav2_uri_kv[1].data.id)),
            icav2_uris_to_project_data_obj_map.items()
        )
    )

    # Replace the icav2 URIs with the presigned URLs
    for index, row in samplesheet_df.iterrows():
        for column in samplesheet_df.columns:
            if row[column].startswith("icav2://"):
                if ";" in row[column]:
                    # We have multiple URIs
                    samplesheet_df.at[index, column] = ";".join(
                        map(
                            lambda uri: icav2_uris_to_presigned_url[uri],
                            row[column].split(';')
                        )
                    )
                else:
                    samplesheet_df.at[index, column] = icav2_uris_to_presigned_url[row[column]]

    # Write the samplesheet to csv
    samplesheet_df.to_csv(samplesheet_path, index=False)


def generate_samplesheet_yaml_template_from_schema_input(
        schema_input_path: Path
) -> CommentedSeq:
    """
    Pull in the samplesheet schema input and generate a samplesheet template
    :param schema_input_path:
    :return:
    """

    # Load the schema input
    with open(schema_input_path, 'r') as schema_input_file:
        schema_input = json.load(schema_input_file)

    # Get the required columns
    required_columns = schema_input.get("items", {}).get("required", [])
    all_properties = schema_input.get("items", {}).get("properties", {})

    # Generate the samplesheet yaml
    samplesheet_yaml = CommentedSeq()

    init_record = CommentedMap()
    for property_name, property_object in all_properties.items():
        init_record[property_name] = ""

        if property_name in required_columns:
            init_record.yaml_add_eol_comment(
                key=property_name,
                comment="Required",
            )
        else:
            init_record.yaml_add_eol_comment(
                key=property_name,
                comment="Optional",
            )

    # Append the one record as an example to the samplesheet yaml
    samplesheet_yaml.append(init_record)

    return samplesheet_yaml


def generate_input_yaml_from_schema_json(
        nextflow_schema_json_path: Path
) -> CommentedMap:
    # Load nextflow_schema_json_path as a dict
    with open(nextflow_schema_json_path, 'r') as nextflow_schema_json_file:
        nextflow_schema_json = json.load(nextflow_schema_json_file)

    # Get the input output options from the nextflow schema json
    # And filter out the generic options
    all_options = dict(
        filter(
            # We don't want the generic options though
            lambda kv: kv[0] not in ["generic_options", "institutional_config_options"],
            nextflow_schema_json.get("definitions").items()
        )
    )

    input_yaml = CommentedMap()

    for option_category_name, option_category_schema_definition in all_options.items():

        property_definitions = option_category_schema_definition.get("properties", {})
        required_property_names = option_category_schema_definition.get("required", [])
        first_property_in_category = True

        for property_name, property_schema_dict in property_definitions.items():
            # Don't add in generic options
            if property_name == "email":
                # We don't want to send an email at the end of the pipeline
                continue
            if property_name == "outdir":
                # We hardcode this to 'out', so we don't make it available in the params xml
                continue
            if property_name in ["config_profile_name", "config_profile_description"]:
                # Don't need these
                continue

            # Make sure that this is a proper schema definition
            if "type" not in property_schema_dict.keys():
                continue

            # Check if property is required
            required_string = "Required " if property_name in required_property_names else "Optional "
            input_yaml[property_name] = f""

            # File type
            if (
                    property_schema_dict["type"] == "string" and
                    property_schema_dict.get("format", None) is not None and
                    property_schema_dict.get("format") == "file-path"
            ):
                # We have a file object that needs to be added
                # Property schema looks like this:
                # {
                #    "type": "string",
                #    "format": "file-path",
                #    "exists": true,
                #    "schema": "assets/schema_input.json",
                #    "mimetype": "text/csv",
                #    "pattern": "^\\S+\\.tsv$",
                #    "description": "Path to comma-separated file containing information about the samples in the experiment.",
                #    "help_text": "You will need to create a design file with information about the samples in your experiment before running the pipeline. Use this parameter to specify its location. It has to be a comma-separated file with 3 columns, and a header row. See [usage docs](https://nf-co.re/airrflow/usage#samplesheet-input).",
                #    "fa_icon": "fas fa-file-csv"
                # }
                if property_schema_dict.get("mimetype", None) is not None:
                    input_yaml.yaml_add_eol_comment(
                        key=property_name,
                        comment=f"icav2//project-id/path/to/file.{property_schema_dict['mimetype'].split('/')[-1]}"
                    )
                else:
                    input_yaml.yaml_add_eol_comment(
                        key=property_name,
                        comment=f": icav2//project-id/path/to/file"
                    )

            # Directory type
            elif (
                    property_schema_dict["type"] == "string" and
                    property_schema_dict.get("format", None) is not None and
                    property_schema_dict.get("format") == "directory-path"
            ):
                input_yaml[property_name] = f""
                input_yaml.yaml_add_eol_comment(
                    key=property_name,
                    comment=f"icav2//project-id/path/to/directory"
                )

            # Enum Type
            elif (
                    property_schema_dict["type"] == "string" and
                    property_schema_dict.get("enum", None) is not None
            ):
                input_yaml.yaml_add_eol_comment(
                    key=property_name,
                    comment=f"One of {' | '.join(map(str, property_schema_dict['enum']))}"
                )

            # Boolean Type
            elif (
                    property_schema_dict["type"] == "boolean"
            ):
                input_yaml.yaml_add_eol_comment(
                    key=property_name,
                    comment="True | False"
                )

            # Integer Type
            elif (
                    property_schema_dict["type"] == "integer"
            ):
                input_yaml.yaml_add_eol_comment(
                    key=property_name,
                    comment="Integer"
                )

            # Number Type
            elif (
                    property_schema_dict["type"] == "number"
            ):
                input_yaml.yaml_add_eol_comment(
                    key=property_name,
                    comment="Number / Float"
                )

            # Add a comment if this is the fist of the property definitions for this category
            if first_property_in_category:
                # Add comment header
                input_yaml.yaml_set_comment_before_after_key(
                    key=property_name,
                    before=(
                            "\n" +
                            option_category_schema_definition.get("description", "") + "\n" +
                            required_string + property_schema_dict.get("description", "") + (
                                f"\nDefault: {property_schema_dict.get('default', '')}"
                                if property_schema_dict.get('default', '') != '' else ""
                            )
                    ),
                    indent=4
                )
                first_property_in_category = False
            else:
                input_yaml.yaml_set_comment_before_after_key(
                    key=property_name,
                    before=required_string + property_schema_dict.get("description", "") + (
                        (
                            f"\nDefault: {property_schema_dict.get('default', '')}"
                            if property_schema_dict.get('default', '') != '' else ""
                        )
                    ),
                    indent=4
                )

    return input_yaml


def download_nextflow_schema_file_from_pipeline_id(
        pipeline_id: Union[UUID4, str],
        schema_json_path: Path
):
    """
    Download the nextflow schema file from the pipeline id

    :param pipeline_id:
    :param schema_json_path:
    :return:
    """
    from ..pipelines import (
        download_pipeline_file, list_pipeline_files
    )

    schema_json_pipeline_file_id = next(
        filter(
            lambda pipeline_file_iter: pipeline_file_iter.get("name") == "nextflow_schema.json",
            list_pipeline_files(pipeline_id)
        )
    )

    download_pipeline_file(
        pipeline_id=pipeline_id,
        file_id=schema_json_pipeline_file_id.get("id"),
        file_path=schema_json_path
    )


def download_nextflow_schema_input_json_from_pipeline_id(
        pipeline_id: Union[UUID4, str],
        schema_input_json_path: Path
):
    """
    Download the nextflow schema file from the pipeline id

    :param pipeline_id:
    :param schema_input_json_path:
    :return:
    """
    from ..pipelines import (
        download_pipeline_file, list_pipeline_files
    )

    schema_json_pipeline_file_id = next(
        filter(
            lambda pipeline_file_iter: pipeline_file_iter.get("name") == "assets/schema_input.json",
            list_pipeline_files(pipeline_id)
        )
    )

    download_pipeline_file(
        pipeline_id=pipeline_id,
        file_id=schema_json_pipeline_file_id.get("id"),
        file_path=schema_input_json_path
    )


def get_nextflow_pipeline_versions_list() -> List[PipelineLanguageVersion]:
    """
    Get the list of nextflow pipeline versions from the ICAv2 API
    :return:
    """
    # Create the API client
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        pipeline_language_api = PipelineLanguageApi(api_client)

    # Get the nextflow pipeline versions
    return pipeline_language_api.get_nextflow_versions().items


def get_default_nextflow_pipeline_version() -> PipelineLanguageVersion:
    """
    Get the default nextflow pipeline version from the ICAv2 API
    :return:
    """
    # Get the list of nextflow pipeline versions
    nextflow_pipeline_versions: List[PipelineLanguageVersion] = get_nextflow_pipeline_versions_list()

    # Return the default nextflow pipeline version
    return next(
        filter(
            lambda version_obj_iter_: version_obj_iter_.name == DEFAULT_NEXTFLOW_VERSION,
            nextflow_pipeline_versions
        )
    )


def get_default_nextflow_pipeline_version_id() -> str:
    """
    Get the default nextflow pipeline version name from the ICAv2 API
    :return:
    """
    return str(get_default_nextflow_pipeline_version().id)