#!/usr/bin/env python3

# Standard imports
"""
All the enums!
"""
from enum import Enum


class BundleStatus(Enum):
    DRAFT = "DRAFT"
    RELEASED = "RELEASED"
    DEPRECATED = "DEPRECATED"


class Data(Enum):
    FOLDER = "FOLDER"
    FILE = "FILE"


# Deprecated: Use Data instead or literal DataType from literals
DataType = Data


class AnalysisStorageSize(Enum):
    SMALL = "Small"
    MEDIUM = "Medium"
    LARGE = "Large"
    XLARGE = "XLarge"
    XXLARGE = "2XLarge"
    XXXLARGE = "3XLarge"


class PipelineStatus(Enum):
    DRAFT = "DRAFT"
    RELEASED = "RELEASED"


class ProjectDataSortParameter(Enum):
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


class ProjectAnalysisStatus(Enum):
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


class ProjectAnalysisStepStatus(Enum):
    FAILED = "FAILED"
    DONE = "DONE"
    RUNNING = "RUNNING"
    INTERRUPTED = "INTERRUPTED"
    ABORTED = "ABORTED"
    WAITING = "WAITING"


class WorkflowLanguage(Enum):
    CWL = "CWL"
    NEXTFLOW = "NEXTFLOW"


class StructuredInputParameterType(Enum):
    BOOLEAN = "boolean"
    STRING = "string"
    INTEGER = "integer"
    OPTION = "options"
    FLOAT = "float"


class StructuredInputParameterTypeMapping(Enum):
    BOOLEAN = bool
    STRING = str
    INTEGER = int
    OPTION = str
    FLOAT = float


class AnalysisLogStreamName(Enum):
    STDOUT = "stdout"
    STDERR = "stderr"


class JobStatus(Enum):
    INITIALIZED = "INITIALIZED"
    WAITING_FOR_RESOURCES = "WAITING_FOR_RESOURCES"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    PARTIALLY_SUCCEEDED = "PARTIALLY_SUCCEEDED"
    FAILED = "FAILED"


class Uri(Enum):
    S3 = "s3"
    ICAV2 = "icav2"


class DataTag(Enum):
    TECHNICAL_TAG = "technical_tag"
    USER_TAG = "user_tag"
    CONNECTOR_TAG = "connector_tag"
    RUN_IN_TAG = "run_in_tag"
    RUN_OUT_TAG = "run_out_tag"
    REFERENCE_TAG = "reference_tag"


class AnalysisTag(Enum):
    USER_TAG = "user_tag"
    TECHNICAL_TAG = "technical_tag"
    REFERENCE_TAG = "reference_tag"
