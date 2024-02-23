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
    TIMECREATED = "timeCreated"
    TIMECREATED_DESC = "-timeCreated"
    TIMEMODIFIED = "timeModified"
    TIMEMODIFIED_DESC = "-timeModified"
    NAME = "name"
    NAME_DESC = "-name"
    PATH = "path"
    PATH_DESC = "-path"
    FILESIZEINBYTES = "fileSizeInBytes"
    FILESIZEINBYTES_DESC = "-fileSizeInBytes"
    STATUS = "status"
    STATUS_DESC = "-status"
    FORMAT = "format"
    FORMAT_DESC = "-format"
    DATATYPE = "dataType"
    DATATYPE_DESC = "-dataType"
    WILLBEARCHIVEDAT = "willBeArchivedAt"
    WILLBEARCHIVEDAT_DESC = "-willBeArchivedAt"
    WILLBEDELETEDAT = "willBeDeletedAt"
    WILLBEDELETEDAT_DESC = "-willBeDeletedAt"


class ProjectDataStatusValues(Enum):
    PARTIAL = "PARTIAL"
    AVAILABLE = "AVAILABLE"
    ARCHIVING = "ARCHIVING"
    ARCHIVED = "ARCHIVED"
    UNARCHIVING = "UNARCHIVING"
    DELETING = "DELETING"


class WorkflowLanguage(Enum):
    CWL: "CWL"
    NEXTFLOW: "NEXTFLOW"
