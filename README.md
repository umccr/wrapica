# wrapica

[![Documentation Status](https://readthedocs.org/projects/wrapica/badge/?version=latest)](https://wrapica.readthedocs.io/en/latest/?badge=latest)
[![Build and Deploy][pipeline_on_tag_badge_svg_url]][pipeline_on_tag_url]
[![PyPI version][badge_fury_svg_url]][badge_fury_url]


A suite of secondary / tertiary functions for running ICAv2 API calls.  

Please visit our [ReadTheDocs site][read_the_docs_url] for more information on installing and using wrapica.

## Installing wrapica

```
pip install wrapica
```

## Using wrapica

### Project Data

```python
# Standard imports
from pathlib import Path

# Wrapica imports
from wrapica.project import get_project_id_from_project_code
from wrapica.enums import DataType
from wrapica.libica_models import ProjectData
from wrapica.project_data import (
    # Functions
    get_project_data_obj_from_project_id_and_path,
    # Types
    ProjectData, DataType
)

# Get project data object
my_project_folder_obj: ProjectData = get_project_data_obj_from_project_id_and_path(
    project_id=get_project_id_from_project_code("my_project_name"),
    # Required value
    data_path=Path("/path/to/my/project/folder"),  
    # Optional FILE or FOLDER if not specified searches for both
    data_type=DataType.FOLDER,  
    # Optional, default is True (cannot be created if data_type is not specified)
    create_data_if_not_found=False,
)
```

### Project Pipeline

```
from wrapica.configuration import get_configuration
from wrapica.project_pipeline import get_project_pipeline_id_from_pipeline_code


my_pipeline_id = get_project_pipeline_id_from_pipeline_code(
    project_id,
    pipeline_code
)
```



### Project Analysis

```python
from typing import List

from wrapica.project_analyses import get_workflow_steps
from wrapica.libica_models import AnalysisStep

workflow_steps: List[AnalysisStep] = get_workflow_steps(
    # Required values
    project_id="my_project_id",
    analysis_id="my_analysis_id",
)

for workflow_step in workflow_steps:
    print(workflow_step.name)
    print(workflow_step.status)
```

[read_the_docs_url]: https://wrapica.readthedocs.io/en/latest/?badge=latest
[pipeline_on_tag_url]: https://github.com/umccr/wrapica/actions/workflows/pipeline_on_tag.yml
[pipeline_on_tag_badge_svg_url]: https://github.com/umccr/wrapica/actions/workflows/pipeline_on_tag.yml/badge.svg
[badge_fury_url]: https://badge.fury.io/py/wrapica
[badge_fury_svg_url]: https://badge.fury.io/py/wrapica.svg