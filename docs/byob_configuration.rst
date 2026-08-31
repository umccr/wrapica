BYOB Configuration
==================

wrapica provides helper functions for working with Bring-Your-Own-Bucket (BYOB) storage
configurations on ICAv2. These functions resolve S3 URIs to ICAv2 URIs (and vice versa),
map projects to their storage configurations, and find the root S3 prefix of a project's
self-managed storage configuration.

API-based resolution (recommended)
-----------------------------------

With recent ICA API improvements, wrapica can now resolve storage configurations and
project-to-storage-configuration mappings **directly from the API** — no YAML files required.

When the corresponding environment variables are *not* set, wrapica will automatically
call the ICA API to:

1. List all storage configurations available in the tenant.
2. List all projects and determine which storage configuration each project uses.
3. Resolve the root S3 subfolder for a given project.

This means you can use the conversion and lookup functions out of the box as long as
``ICAV2_ACCESS_TOKEN`` (and optionally ``ICAV2_BASE_URL``) are set.

Example: resolve a project's S3 root
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from wrapica.storage_configuration import get_project_self_storage_configuration_s3_uri

    # Returns the full S3 URI prefix for the project, e.g. "s3://bucket/key-prefix/project-sub/"
    s3_root = get_project_self_storage_configuration_s3_uri("abcd1234-ab12-ab12-ab12-abcdef123456")

Example: map projects to storage configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from wrapica.storage_configuration import get_project_to_storage_configuration_mapping_list

    mappings = get_project_to_storage_configuration_mapping_list()

    for mapping in mappings:
        print(f"Project: {mapping['name']} -> Storage Config: {mapping['storageConfigurationId']}, prefix: {mapping.get('prefix')}")

Example: convert between S3 and ICAv2 URIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from wrapica.storage_configuration import (
        convert_s3_uri_to_icav2_uri,
        convert_icav2_uri_to_s3_uri,
    )

    icav2_uri = convert_s3_uri_to_icav2_uri("s3://my-bucket/prefix/data/file.txt")
    # icav2://abcd1234-ab12-ab12-ab12-abcdef123456/data/file.txt

    s3_uri = convert_icav2_uri_to_s3_uri("icav2://abcd1234-ab12-ab12-ab12-abcdef123456/data/file.txt")
    # s3://my-bucket/prefix/data/file.txt


YAML file overrides
--------------------

The storage configuration and project-to-storage-configuration mapping YAML files are
**optional** — wrapica will fall back to the API when they are not set.

However, the **storage credential list YAML is still mandatory** for S3 buckets that are
not mounted onto any ICA project. The API does not expose which S3 prefixes a given
storage credential has access to, so this mapping must be provided explicitly.

If your ICA token does not have the required permissions to list storage configurations
or projects, or you want to avoid extra API calls for performance reasons, you can
also supply YAML files for the other two lists via environment variables. When set,
wrapica will use these files **instead of** calling the API.

Storage Configuration Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Env var:

.. code-block:: bash

    ICAV2_STORAGE_CONFIGURATION_LIST_FILE=/path/to/storage-configuration-file.yaml

Yaml file contents:

List of objects where each object has the following attributes:

* id: The storage configuration id
* bucketName: The name of the bucket
* keyPrefix: The s3 prefix for this storage configuration object
* storageCredentialId: The storage credential id used to build this storage configuration

Your storage configuration yaml file may look like this:

.. code-block:: yaml

    - id: 81657569-adce-4ae6-bd0d-87225fe819e9
      bucketName: reference-data-bucket
      keyPrefix: reference-data
    - id: af421f82-4127-4cee-94e7-f249ff4ddc43
      bucketName: research-project-bucket
      keyPrefix: research-data/

You may be able to generate the storage configuration mapping programatically with the following command:

.. code-block:: bash

    curl --fail --silent --location \
      --request "GET" \
      --url "https://ica.illumina.com/ica/rest/api/storageConfigurations" \
      --header "Accept: application/vnd.illumina.v3+json" \
      --header "Authorization: Bearer ${ICAV2_ACCESS_TOKEN}" | \
    jq --raw-output \
      '
        .items |
        map(
          {
            "id": .id,
            "bucketName": .storageConfigurationDetails.awsS3.bucketName,
            "keyPrefix": .storageConfigurationDetails.awsS3.keyPrefix
          }
        )
      ' | \
    yq --prettyPrint --unwrapScalar


Project to Storage Configuration List
-------------------------------------

Env var:

.. code-block:: bash

    ICAV2_PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_FILE=/path/to/project-to-storage-configuration-file.yaml


Yaml file contents:

List of objects where each object has the following attributes:

* id: The id of the project
* name: The name of the project
* storageConfigurationId: The storage configuration that the project is mounted on
* prefix: The prefix of the **project** on the storage configuration
    * if the project is set to the root of the storage configuration, set the prefix parameter to :code:`null`

Your project-to-storage configuration yaml may look something like this

.. code-block:: yaml

    - id: 81657569-adce-4ae6-bd0d-87225fe819e9
      bucketName: reference-data-bucket
      storageConfigurationId: 81657569-adce-4ae6-bd0d-87225fe819e9
      # Project mounted at the root of the prefix (s3://research-data-bucket/reference-data/)
      prefix: null
    - id: 33103584-2531-4d10-8d2e-fd3fa16aeb7c
      name: colon-cancer-project
      storageConfigurationId: af421f82-4127-4cee-94e7-f249ff4ddc43
      # Project mounted at the prefix (s3://research-project-bucket/research-data/colon-cancer-data/)
      prefix: colon-cancer-data

Storage Credential Setup
------------------------

Env var:

.. code-block:: bash

    ICAV2_STORAGE_CREDENTIAL_LIST_FILE=/path/to/storage-credentials.yaml

Yaml file contents:

List of objects where each object has the following attributes:

* id: The id of the storage credentials
* name: The name of storage credentials
* s3UriList:  The list of objects with the following keys:
    * bucketName: A bucket that this storage credential has access to
    * keyPrefix: A key prefix on the bucket that this storage credential has access to

Your storage credential yaml may look something like this

.. code-block:: yaml

    # Storage credentials for ICAv2
    # Reference data
    - id: e737fcdd-d61c-4bd7-9a7b-ff70fe88d405
      name: icav2_ref_data_aws_user
      s3UriList:
        - bucketName: reference-data-bucket
          keyPrefix: refdata/
    # Research data
    - id: 33103584-2531-4d10-8d2e-fd3fa16aeb7c
      name: icav2_research_data_aws_user
      s3UriList:
        - bucketName: research-project-bucket
          keyPrefix: research-data/colon-cancer-data/
        - bucketName: research-project-bucket
          keyPrefix: clinical-trial-control-data/colon-control-data/


How it works
------------

The following diagram shows the resolution order wrapica uses:

1. **Storage Configuration List** — if ``ICAV2_STORAGE_CONFIGURATION_LIST_FILE`` is set,
   load from YAML; otherwise call ``GET /api/storageConfigurations``.

2. **Project-to-Storage Mapping** — if ``ICAV2_PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_FILE``
   is set, load from YAML; otherwise iterate over all projects (via ``list_projects()``) and
   call ``GET /api/projects/{id}/selfManagedStorageConfiguration`` for each project that has
   a self-managed storage configuration.

3. **URI Conversion** — with both lists resolved, wrapica can map any S3 URI to an ICAv2
   URI (and back) by matching the bucket + key prefix against the storage configuration and
   project prefix.

Key functions
~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Function
     - Description
   * - ``get_storage_configuration_list()``
     - List all storage configurations in the tenant
   * - ``get_project_to_storage_configuration_mapping_list()``
     - Map each project to its storage configuration and prefix
   * - ``get_project_self_storage_configuration_s3_uri(project_id)``
     - Get the root S3 URI for a project's self-managed storage configuration
   * - ``get_s3_key_prefix_by_project_id(project_id)``
     - Get the S3 key prefix URI for a project
   * - ``get_project_id_by_s3_key_prefix(s3_key_prefix)``
     - Resolve which project owns a given S3 prefix
   * - ``convert_s3_uri_to_icav2_uri(s3_uri)``
     - Convert an S3 URI to an ICAv2 URI
   * - ``convert_icav2_uri_to_s3_uri(icav2_uri)``
     - Convert an ICAv2 URI to an S3 URI
   * - ``convert_project_data_obj_to_s3_uri(project_data_obj)``
     - Convert a ProjectData object to its S3 URI
   * - ``convert_s3_uri_to_project_data_obj(s3_uri)``
     - Convert an S3 URI to a ProjectData object
