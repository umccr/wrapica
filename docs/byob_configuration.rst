BYOB Configuration
==================

Unfortunately, it is not easy to resolve an s3 URI to an ICAv2 (or vice versa), due
to API limitations on storage configurations.

Hence you will need three separate environment variables that point to three separate yaml files :)
This will include:

* A storage configuration setup
    * What is the bucket / prefix for each storage configuration (saves an API call each time we need to convert an S3 URI to an ICAv2 URI
* A project to storage configuration mapping
    * which project is configured to which storage configuration, and at what prefix (if any).
* A storage credential setup
    * For a given storage credential id (that the user either created or has been shared across the tenant), what are the s3 prefixes that this storage configuration has access to?


Storage Configuration Setup
---------------------------

Env var:

.. code-block:: bash

    ICAV2_STORAGE_CONFIGURATION_LIST_FILE=/path/to/storage-configuration-file.yaml

Yaml file contents:

List of objects where each object has the following attributes:

* id: The storage configuration id
* bucketName: The name of the bucket
* keyPrefix: The s3 prefix for this storage configuration object

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