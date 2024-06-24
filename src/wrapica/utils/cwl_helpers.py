#!/usr/bin/env python3

"""
CWL helpers

Handy for generating input templates, i.e from a cwl object,
Generate an input yaml template for the cwl object

Can also generate the overrides template from a cwl object
"""
from copy import deepcopy
from pathlib import Path
# External imports
from typing import List, Dict, Union
import re
from urllib.parse import urlparse

from cwl_utils.parser.cwl_v1_2 import shortname, RecordSchema, WorkflowStep
from ruamel.yaml import YAML, CommentedMap

# Local imports
from .logger import get_logger
from .cwl_typing_helpers import WorkflowInputParameterType, InputEnumSchemaType, RecordSchemaType, InputArraySchemaType, \
    InputRecordSchemaType

# Get logger
logger = get_logger()


class CWLSchema:
    """
    Missing component of cwlutils
    """

    def __init__(self, cwl_obj, file_path):
        self.cwl_obj: RecordSchemaType = cwl_obj
        self.cwl_file_path: Path = file_path

        # Confirm type is record
        if not self.cwl_obj.type_.get("type") == "record":
            logger.error("Expected record type")

    def get_input_from_str_type(self, workflow_input: Dict) -> Union[Dict, str, List]:
        if workflow_input.get("type").endswith("[]"):
            new_workflow_input = deepcopy(workflow_input)
            new_workflow_input["type"] = re.sub(r"\[]$", "", workflow_input.get("type"))
            return [
                self.get_input_from_str_type(new_workflow_input)
            ]
        if workflow_input.get("type").rstrip("?") == "Directory":
            return {
                "class": "Directory",
                "location": "icav2://project_id/path/to/dir/"
            }
        elif workflow_input.get("type").rstrip("?") == "File":
            return {
                "class": "File",
                "location": "icav2://project_id/path/to/file"
            }
        elif workflow_input.get("type").rstrip("?") == "boolean":
            return workflow_input.get("default") if workflow_input.get("default") is not None else False
        elif workflow_input.get("type").rstrip("?") == "int":
            return workflow_input.get("default") if workflow_input.get("default") is not None else "string"
        elif workflow_input.get("type").rstrip("?") == "string":
            return workflow_input.get("default") if workflow_input.get("default") is not None else "string"

    def get_input_from_array_type(self, workflow_input: Dict) -> Union[Dict, str, List]:
        """
        Likely first element of type is null
        :param workflow_input:
        :return:
        """
        workflow_input_new = deepcopy(workflow_input)
        if workflow_input.get("type")[0] == "null":
            workflow_input_new["type"] = workflow_input.get("type")[1]
        else:
            workflow_input_new["type"] = workflow_input.get("type")[0]

        if isinstance(workflow_input_new.get("type"), Dict):
            return self.get_input_from_dict_type(workflow_input_new)
        elif isinstance(workflow_input_new.get("type"), List):
            return self.get_input_from_array_type(workflow_input_new)
        elif isinstance(workflow_input_new.get("type"), str):
            return self.get_input_from_str_type(workflow_input_new)
        else:
            logger.error(f"Unsure what to do with type {type(workflow_input_new.get('type'))}")
            raise NotImplementedError

    def get_input_from_record_type(self, workflow_input: Dict) -> Union[Dict]:
        """
        Very similar to schema base command
        :param workflow_input:
        :return:
        """
        workflow_inputs = {}
        for field_key, field_dict in workflow_input.get("fields").items():
            if isinstance(field_dict.get("type"), Dict):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_dict_type(field_dict)
                    }
                )
            elif isinstance(field_dict.get("type"), List):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_array_type(field_dict)
                    }
                )
            elif isinstance(field_dict.get("type"), str):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_str_type(field_dict)
                    }
                )
            else:
                logger.warning(f"Don't know what to do with type {type(field_dict.get('type'))} for key {field_key}")
        return workflow_inputs

    def get_input_from_dict_type(self, workflow_input: Dict) -> Union[Dict, List]:
        """
        Dict type
        :param workflow_input:
        :return:
        """
        if "type" in workflow_input.get("type").keys() and workflow_input.get("type").get("type") == "record":
            return self.get_input_from_record_type(workflow_input.get("type"))
        if "type" in workflow_input.get("type").keys() and workflow_input.get("type").get("type") == "array":
            if isinstance(workflow_input.get("type").get("items"), str):
                return [
                    self.get_input_from_str_type(
                        {
                            "type": workflow_input.get("type").get("items")
                        }
                    )
                ]
            elif isinstance(workflow_input.get("type").get("items"), Dict):
                # We have an import
                return self.get_input_from_dict_type(
                    {
                        "type": workflow_input.get("type").get("items")
                    }
                )
        if "$import" in workflow_input.get("type").keys():
            schema_path = self.cwl_file_path.parent.joinpath(
                workflow_input.get("type").get("$import").split("#", 1)[0]
            ).resolve()
            return CWLSchema.load_schema_from_uri(schema_path.as_uri()).get_inputs_template()

    def get_inputs_template(self) -> Dict:
        """
        Return get inputs from dict
        :return:
        """
        workflow_inputs = {}
        for field_key, field_dict in self.cwl_obj.type_.get("fields").items():
            if isinstance(field_dict.get("type"), Dict):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_dict_type(field_dict)
                    }
                )
            elif isinstance(field_dict.get("type"), List):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_array_type(field_dict)
                    }
                )
            elif isinstance(field_dict.get("type"), str):
                workflow_inputs.update(
                    {
                        field_key: self.get_input_from_str_type(field_dict)
                    }
                )
            else:
                logger.warning(f"Don't know what to do with type {type(field_dict.get('type'))} for key {field_key}")
        return workflow_inputs

    @classmethod
    def load_schema_from_uri(cls, uri_input):
        file_path: Path = Path(urlparse(uri_input).path)

        yaml_handler = YAML()

        with open(file_path, "r") as schema_h:
            schema_obj = yaml_handler.load(schema_h)

        return cls(RecordSchema(schema_obj), file_path)


def get_workflow_input_type(workflow_input: WorkflowInputParameterType):
    if isinstance(workflow_input.type_, str):
        return get_workflow_input_type_from_str_type(workflow_input)
    elif isinstance(workflow_input.type_, InputEnumSchemaType):
        return get_workflow_input_type_from_enum_schema(workflow_input)
    elif isinstance(workflow_input.type_, InputArraySchemaType):
        return get_workflow_input_type_from_array_schema(workflow_input)
    elif isinstance(workflow_input.type_, InputRecordSchemaType):
        return get_workflow_input_type_from_record_schema(workflow_input)
    elif isinstance(workflow_input.type_, List):
        return get_workflow_input_type_from_array_type(workflow_input)
    else:
        logger.warning(f"Don't know what to do here with {type(workflow_input.type_)}")


def get_workflow_input_type_from_str_type(workflow_input: WorkflowInputParameterType):
    """
    Workflow input type is a string type
    :param workflow_input:
    :return: A list with the following attributes
      {

      }
    """
    if workflow_input.type_.startswith("file://"):
        # This is a schema!
        return CWLSchema.load_schema_from_uri(workflow_input.type_).get_inputs_template()
    if workflow_input.type_ == "Directory":
        return {
            "class": "Directory",
            "location": "icav2://project_id/path/to/dir/"
        }
    elif workflow_input.type_ == "File":
        return {
            "class": "File",
            "location": "icav2://project_id/path/to/file"
        }
    elif workflow_input.type_ == "boolean":
        return workflow_input.default if workflow_input.default is not None else False
    elif workflow_input.type_ == "int":
        return workflow_input.default if workflow_input.default is not None else "string"
    elif workflow_input.type_ == "string":
        return workflow_input.default if workflow_input.default is not None else "string"
    else:
        logger.warning(f"Don't know what to do here with {workflow_input.type_}")


def get_workflow_input_type_from_array_type(workflow_input: WorkflowInputParameterType):
    """
    Workflow input is type list -
    likely that the first input is 'null'
    :param workflow_input:
    :return:
    """
    if not workflow_input.type_[0] == "null":
        logger.error("Unsure what to do with input of type list where first element is not null")
        raise ValueError
    workflow_input_new = deepcopy(workflow_input)
    workflow_input_new.type_ = workflow_input.type_[1]
    return get_workflow_input_type(workflow_input_new)


def get_workflow_input_type_from_enum_schema(workflow_input: WorkflowInputParameterType):
    """
    Workflow input type is a enum type
    :param workflow_input:
    :return:
    """
    workflow_type: InputEnumSchemaType = workflow_input.type_
    return shortname(workflow_type.symbols[0])


def get_workflow_input_type_from_array_schema(workflow_input: WorkflowInputParameterType):
    """
    Workflow input type is an array schema
    items attribute may be a file uri
    :param workflow_input:
    :return:
    """
    return [
        CWLSchema.load_schema_from_uri(workflow_input.type_.items).get_inputs_template()
    ]


def get_workflow_input_type_from_record_schema(workflow_input: WorkflowInputParameterType):
    raise NotImplementedError


def get_workflow_input_is_optional(workflow_input: WorkflowInputParameterType):
    if isinstance(workflow_input.type_, List) and workflow_input.type_[0] == 'null':
        return True
    elif getattr(workflow_input, 'default') is not None:
        return True
    return False


def create_template_from_workflow_inputs(
        workflow_inputs: List[WorkflowInputParameterType]
) -> CommentedMap:
    """
    List inputs by template
    :param workflow_inputs:

    :return: A commented map of the inputs
    :rtype: CommentedMap
    """
    input_type_map = CommentedMap()

    for workflow_input in workflow_inputs:
        input_type_map.update(
            {
                shortname(workflow_input.id): get_workflow_input_type(workflow_input)
            }
        )

        # Add label
        before_comments = [
            f"{workflow_input.label} ({'Required' if not get_workflow_input_is_optional(workflow_input) else 'Optional'})"
        ]

        # Add default if it exists
        if workflow_input.default is not None:
            before_comments.append(f"Default value: {workflow_input.default}")

        # Add docs
        before_comments.append(f"Docs: {workflow_input.doc}")

        # Write comment to commented map
        input_type_map.yaml_set_comment_before_after_key(
            key=shortname(workflow_input.id),
            before='\n' + '\n'.join(before_comments),
            after="\n"
        )

    return input_type_map


def get_overrides_from_workflow_steps(workflow_steps: List[WorkflowStep]) -> Dict:
    """
    Get overrides from workflow steps
    :param workflow_steps:
    :return:
    """
    overrides = CommentedMap()

    for step in workflow_steps:
        overrides.update(
            {
                shortname(step.id): ""
            }
        )
    return overrides
