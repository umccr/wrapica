# Product: wrapica

wrapica is a Python library providing secondary/tertiary convenience functions for the Illumina Connected Analytics v2 (ICAv2) REST API. It wraps the lower-level `libica` SDK to offer higher-level operations for managing projects, data, pipelines, analyses, jobs, bundles, and storage on the ICA platform.

## Key capabilities
- Project data management (list, create, copy, move, delete, presign URLs, read/write file contents)
- Pipeline and analysis orchestration (launch, monitor, retrieve logs and steps)
- URI handling for `icav2://` and `s3://` scheme conversion
- CWL and Nextflow workflow helpers
- Credential and storage configuration management
- Bundle and tenant operations

## Users
Bioinformaticians and platform engineers at UMCCR (University of Melbourne Centre for Cancer Research) who automate genomics workflows on ICAv2.

## Distribution
Published to PyPI as `wrapica`. Documentation hosted on ReadTheDocs.
