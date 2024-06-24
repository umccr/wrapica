#!/usr/bin/env python

"""
Models to easily import
"""
from typing import Union

# Import all models
from libica.openapi.v2.models import *

Analysis = Union[AnalysisV3, AnalysisV4]
CwlAnalysisInput = Union[CwlAnalysisStructuredInput, CwlAnalysisJsonInput]
