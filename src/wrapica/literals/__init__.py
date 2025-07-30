#!/usr/bin/env python3

"""
All the literals!
"""

# Standard library imports
from datetime import datetime
from typing import Literal, TypedDict, Optional


BundleStatusType = Literal[
    "DRAFT",
    "RELEASED",
    "DEPRECATED",
]


DataType = Literal[
    "FOLDER",
    "FILE",
]

AnalysisStorageSizeType = Literal[
    "Small",
    "Medium",
    "Large",
    "XLarge",
    "2XLarge",
    "3XLarge",
]

PipelineStatusType = Literal[
    "DRAFT",
    "RELEASED",
]


ProjectDataSortParameterType = Literal[
    "timeCreated",
    "-timeCreated",
    "timeModified",
    "-timeModified",
    "name",
    "-name",
    "path",
    "-path",
    "fileSizeInBytes",
    "-fileSizeInBytes",
    "status",
    "-status",
    "format",
    "-format",
    "dataType",
    "-dataType",
    "willBeArchivedAt",
    "-willBeArchivedAt",
    "willBeDeletedAt",
    "-willBeDeletedAt",
]


ProjectDataStatusValuesType = Literal[
    "PARTIAL",
    "AVAILABLE",
    "ARCHIVING",
    "ARCHIVED",
    "UNARCHIVING",
    "DELETING",
]


ProjectAnalysisSortParametersType = Literal[
    "reference",
    "-reference",
    "userReference",
    "-userReference",
    "pipeline",
    "-pipeline",
    "status",
    "-status",
    "startDate",
    "-startDate",
    "endDate",
    "-endDate",
    "summary",
    "-summary",
]


ProjectAnalysisStatusType = Literal[
    # In process
    "REQUESTED",
    "QUEUED",
    "INITIALIZING",
    "PREPARING_INPUTS",
    "IN_PROGRESS",
    "GENERATING_OUTPUTS",
    # Terminal Successful
    "SUCCEEDED",
    # Terminal Failed
    "ABORTING",
    "FAILED",
    "FAILED_FINAL",
    "ABORTED",
    # Unknown (maybe a nf thing)
    "AWAITING_INPUT",
]


ProjectAnalysisStepStatusType = Literal[
    "FAILED",
    "DONE",
    "RUNNING",
    "INTERRUPTED",
    "ABORTED",
    "WAITING",
]


WorkflowLanguageType = Literal[
    "CWL",
    "NEXTFLOW"
]


AnalysisLogStreamNameType = Literal[
    "stdout",
    "stderr",
]

JobStatusType = Literal[
    "INITIALIZED",
    "WAITING_FOR_RESOURCES",
    "RUNNING",
    "STOPPED",
    "SUCCEEDED",
    "PARTIALLY_SUCCEEDED",
    "FAILED",
]


UriType = Literal[
    "s3",
    "icav2",
]


DataTagType = Literal[
    "technical_tag",
    "user_tag",
    "connector_tag",
    "run_in_tag",
    "run_out_tag",
    "reference_tag",
]


AnalysisTagType = Literal[
    "user_tag",
    "technical_tag",
    "reference_tag",
]


## Typed dicts
class AnalysisStepDict(TypedDict):
    name: str
    status: ProjectAnalysisStepStatusType
    queue_date: Optional[datetime]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
