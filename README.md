# wrapica

[![Documentation Status](https://readthedocs.org/projects/wrapica/badge/?version=latest)](https://wrapica.readthedocs.io/en/latest/?badge=latest)
[![Build and Deploy][pipeline_on_tag_badge_svg_url]][pipeline_on_tag_url]
[![PyPI version][badge_fury_svg_url]][badge_fury_url]


A suite of secondary / tertiary functions for running ICAv2 API calls

## Installing wrapica

```
pip install wrapica
```

## Using wrapica

### Project Data

```python
from pathlib import Path
from wrapica.configuration import get_configuration
from wrapica.project_data import get_data_obj_from_project_id_and_path, ProjectData

my_project_folder_obj: ProjectData = get_data_obj_from_project_id_and_path(
    project_id="my_project_id",  # Required value
    # Required value
    path=Path("/path/to/my/project/folder"),  
    # Optional FILE or FOLDER if not specified searches for both
    data_type="FOLDER",  
    # Optional, default is True (cannot be created if data_type is not specified)
    create_on_missing=False,
    # Optional, can be used to override the default configuration
    configuration=get_configuration()
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
from wrapica.configuration import get_configuration
from wrapica.project_analysis import get_workflow_steps, AnalysisStep
from typing import List

workflow_steps: List[AnalysisStep] = get_workflow_steps(
    # Required values
    project_id="my_project_id",
    analysis_id="my_analysis_id",
    # Optional, can be used to override the default configuration
    configuration=get_configuration()
)
```

[read_the_docs_url]: https://wrapica.readthedocs.io/en/latest/?badge=latest
[pipeline_on_tag_url]: https://github.com/umccr/wrapica/actions/workflows/pipeline_on_tag.yml
[pipeline_on_tag_badge_svg_url]: https://github.com/umccr/wrapica/actions/workflows/pipeline_on_tag.yml/badge.svg
[badge_fury_url]: https://badge.fury.io/py/wrapica
[badge_fury_svg_url]: https://badge.fury.io/py/wrapica.svg