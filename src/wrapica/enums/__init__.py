#!/usr/bin/env python3

"""
All the enums!
"""

from enum import Enum


class DataType(Enum):
    FOLDER = "FOLDER"
    FILE = "FILE"


class AnalysisStorageSize(Enum):
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"


class ProjectDataSortParameters(Enum):
    TIME_CREATED = "timeCreated"
    TIME_CREATED_DESC = "-timeCreated"
    TIME_MODIFIED = "timeModified"
    TIME_MODIFIED_DESC = "-timeModified"
    NAME = "name"
    NAME_DESC = "-name"
    PATH = "path"
    PATH_DESC = "-path"
    FILE_SIZE_IN_BYTES = "fileSizeInBytes"
    FILE_SIZE_IN_BYTES_DESC = "-fileSizeInBytes"
    STATUS = "status"
    STATUS_DESC = "-status"
    FORMAT = "format"
    FORMAT_DESC = "-format"
    DATATYPE = "dataType"
    DATATYPE_DESC = "-dataType"
    WILL_BE_ARCHIVED_AT = "willBeArchivedAt"
    WILL_BE_ARCHIVED_AT_DESC = "-willBeArchivedAt"
    WILL_BE_DELETED_AT = "willBeDeletedAt"
    WILL_BE_DELETED_AT_DESC = "-willBeDeletedAt"


class ProjectDataStatusValues(Enum):
    PARTIAL = "PARTIAL"
    AVAILABLE = "AVAILABLE"
    ARCHIVING = "ARCHIVING"
    ARCHIVED = "ARCHIVED"
    UNARCHIVING = "UNARCHIVING"
    DELETING = "DELETING"


class ProjectAnalysisSortParameters(Enum):
    REFERENCE = "reference"
    REFERENCE_DESC = "-reference"
    USER_REFERENCE = "userReference"
    USER_REFERENCE_DESC = "-userReference"
    PIPELINE = "pipeline"
    PIPELINE_DESC = "-pipeline"
    STATUS = "status"
    STATUS_DESC = "-status"
    START_DATE = "startDate"
    START_DATE_DESC = "-startDate"
    END_DATE = "endDate"
    END_DATE_DESC = "-endDate"
    SUMMARY = "summary"
    SUMMARY_DESC = "-summary"


class ProjectAnalysisStatusValues(Enum):
    # In process
    REQUESTED = "REQUESTED"
    QUEUED = "QUEUED"
    INITIALIZING = "INITIALIZING"
    PREPARING_INPUTS = "PREPARING_INPUTS"
    IN_PROGRESS = "IN_PROGRESS"
    GENERATING_OUTPUTS = "GENERATING_OUTPUTS"
    # Terminal Successful
    SUCCEEDED = "SUCCEEDED"
    # Terminal Failed
    ABORTING = "ABORTING"
    FAILED = "FAILED"
    FAILED_FINAL = "FAILED_FINAL"
    ABORTED = "ABORTED"
    # Unknown (maybe a nf thing)
    AWAITING_INPUT = "AWAITING_INPUT"


class WorkflowLanguage(Enum):
    CWL = "CWL"
    NEXTFLOW = "NEXTFLOW"


class StructuredInputParameterType(Enum):
    BOOLEAN = "boolean"
    STRING = "string"
    INTEGER = "integer"
    OPTION = "options"


class StructuredInputParameterTypeMapping(Enum):
    BOOLEAN = bool
    STRING = str
    INTEGER = int
    OPTION = str
