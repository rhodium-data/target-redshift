# target-redshift

[![CI](https://github.com/rhodium-data/target-redshift/actions/workflows/ci.yml/badge.svg)](https://github.com/rhodium-data/target-redshift/actions/workflows/ci.yml)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

[Singer](https://www.singer.io/) target that loads data into Amazon Redshift following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## Project History

This project is a community-maintained fork of [pipelinewise-target-redshift](https://github.com/transferwise/pipelinewise-target-redshift), originally created by TransferWise (now Wise).

In 2024, Wise announced the sunsetting of the PipelineWise project ([v0.64.1](https://github.com/transferwise/pipelinewise/tree/v0.64.1) was the last version). This fork continues development independently with:

- **Active maintenance** - Bug fixes, dependency updates, and security patches
- **New features** - Mock integration testing with Docker, improved test coverage
- **Community-driven** - Open to contributions and feature requests

We are deeply grateful to the TransferWise team and all original contributors for creating this excellent Redshift target connector. Their robust implementation has served the community well for many years.


## How to use it

This [Singer Target](https://singer.io) can be run independently or as part of a Singer-based data pipeline. It follows the Singer specification for loading data into Amazon Redshift.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install target-redshift
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### Platform-Specific Installation Notes

When installing from source with extras (e.g., `pip install .[test]`), different shells require different syntax:

| Shell | Command | Notes |
|-------|---------|-------|
| Bash/sh | `pip install .[test]` | No escaping needed |
| Zsh (macOS Catalina+) | `pip install .\[test\]` | Brackets must be escaped |
| Fish | `pip install .[test]` | No escaping needed |
| Windows CMD/PowerShell | `pip install ".[test]"` | Use quotes |

**Tip:** Using the provided Makefile (`make install-dev`) works on all platforms and handles these differences automatically.

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-redshift --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Amazon Redshift.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Configuration settings

Running the the target connector requires a `config.json` file. Example with the minimal settings:

   ```json
   {

     "host": "xxxxxx.redshift.amazonaws.com",
     "port": 5439,
     "user": "my_user",
     "password": "password",
     "dbname": "database_name",
     "aws_access_key_id": "secret",
     "aws_secret_access_key": "secret",
     "s3_bucket": "bucket_name",
     "default_target_schema": "my_target_schema"
   }
   ```

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| host                                | String  | Yes        | Redshift Host                                                 |
| port                                | Integer | Yes        | Redshift Port                                                 |
| user                                | String  | Yes        | Redshift User                                                 |
| password                            | String  | Yes        | Redshift Password                                             |
| dbname                              | String  | Yes        | Redshift Database name                                        |
| aws_profile                         | String  | No         | AWS profile name for profile based authentication. If not provided, `AWS_PROFILE` environment variable will be used. |
| aws_access_key_id                   | String  | No         | S3 Access Key Id. Used for S3 and Redshfit copy operations. If not provided, `AWS_ACCESS_KEY_ID` environment variable will be used. |
| aws_secret_access_key               | String  | No         | S3 Secret Access Key. Used for S3 and Redshfit copy operations. If not provided, `AWS_SECRET_ACCESS_KEY` environment variable will be used.  |
| aws_session_token                   | String  | No         | S3 AWS STS token for temporary credentials. If not provided, `AWS_SESSION_TOKEN` environment variable will be used. |
| aws_redshift_copy_role_arn          | String  | No         | AWS Role ARN to be used for the Redshift COPY operation. Used instead of the given AWS keys for the COPY operation if provided - the keys are still used for other S3 operations |
| s3_acl                              | String  | No         | S3 Object ACL                                                |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. |
| copy_options                        | String  |            | (Default: `EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS TIMEFORMAT 'auto' COMPUPDATE OFF STATUPDATE OFF`). Parameters to use in the COPY command when loading data to Redshift. Some basic file formatting parameters are fixed values and not recommended overriding them by custom values. They are like: `CSV GZIP DELIMITER ',' REMOVEQUOTES ESCAPE` |
| batch_size_rows                     | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Redshift. |
| flush_all_streams                   | Boolean |            | (Default: False) Flush and load every stream into Redshift when one batch is full. Warning: This may trigger the COPY command to use files with low number of records, and may cause performance problems. |
| parallelism                         | Integer |            | (Default: 0) The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max. |
| max_parallelism                     | Integer |            | (Default: 16) Max number of parallel threads to use when flushing tables. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permissions | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific list of users or groups. Example: `{"users": ["user_1","user_2"], "groups": ["group_1", "group_2"]}` If `schema_mapping` is not defined then every stream sent by the tap is granted accordingly.   |
| schema_mapping                      | Object  |            | Useful if you want to load multiple streams from one tap to multiple Redshift schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permissions` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a [PipelineWise YAML Example]
| disable_table_cache                 | Boolean |            | (Default: False) By default the connector caches the available table structures in Redshift at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in redshift etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The metadata columns are documented at https://transferwise.github.io/pipelinewise/data_structure/sdc-columns.html. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Redshift. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Redshift to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |
| data_flattening_max_level           | Integer |            | (Default: 0) Object type RECORD items from taps can be loaded into VARIANT columns as JSON (default) or we can flatten the schema by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. |
| primary_key_required                | Boolean |            | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. |
| validate_records                    | Boolean |            | (Default: False) Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by Snowflake. Enabling this option will detect invalid records earlier but could cause performance degradation. |
| skip_updates                        | Boolean |    No      | (Default: False) Do not update existing records when Primary Key is defined. Useful to improve performance when records are immutable, e.g. events
| compression                         | String  |    No        | The compression method to use when writing files to S3 and running Redshift `COPY`. The currently supported methods are `gzip` or `bzip2`. Defaults to none (`""`). |
| slices                              | Integer |    No      | The number of slices to split files into prior to running COPY on Redshift. This should be set to the number of Redshift slices. The number of slices per node depends on the node size of the cluster - run `SELECT COUNT(DISTINCT slice) slices FROM stv_slices` to calculate this. Defaults to `1`. |
| temp_dir                            | String  |            | (Default: platform-dependent) Directory of temporary CSV files with RECORD messages. |

## Development

### Testing

This project includes comprehensive unit and integration tests to ensure reliability and correctness of the Redshift target connector.

#### Unit Tests

Unit tests verify the core functionality without requiring external dependencies like Redshift or AWS. They use mocking to isolate and test individual components.

**What Unit Tests Cover:**

- **Configuration Validation** (in `tests/unit/test_db_sync.py`)
  - Validates required configuration fields
  - Tests minimal valid configuration
  - Validates schema mapping configurations

- **Data Type Mapping** (in `tests/unit/test_db_sync.py`)
  - JSON schema types to Redshift column types (string, integer, number, boolean, etc.)
  - Special type handling: SUPER type, date-time, time formats
  - VARCHAR length calculations and constraints
  - Tests with `with_length` parameter variations

- **Column Transformations** (in `tests/unit/test_db_sync.py`)
  - Tests `parse_json` transformation for object and array types
  - Verifies SUPER types don't get unnecessary transformations
  - Validates transformation function mappings

- **Column Name Safety** (in `tests/unit/test_db_sync.py`)
  - Safe column name formatting (uppercase, quoting)
  - Special character handling (dashes, spaces)
  - Column clause generation for CREATE TABLE statements
  - Primary key column name extraction

- **Schema Flattening** (in `tests/unit/test_db_sync.py`)
  - Nested JSON object flattening at various levels
  - Flattening with `max_level` parameter (0, 1, 10, etc.)
  - Edge cases: anyOf/oneOf patterns, duplicate column detection
  - Complex nested object hierarchies

- **Record Flattening** (in `tests/unit/test_db_sync.py`)
  - Record data transformation matching flattened schemas
  - JSON stringification for nested objects
  - Flatten key generation with separator handling
  - Column name truncation for long identifiers

- **Stream Name Parsing** (in `tests/unit/test_db_sync.py`)
  - Singer stream name format parsing (catalog-schema-table)
  - Redshift table format parsing with custom separators
  - Extraction of catalog, schema, and table names

- **Batch Processing** (in `tests/unit/test_target_rs.py`)
  - Tests batch size limits and flushing behavior
  - Validates stream flushing at correct intervals

**Running Unit Tests:**

Unit tests require no external configuration and run entirely locally using mocks.

```bash
# Using Makefile (recommended)
make test-unit

# Or manually
coverage run -m pytest -vv --disable-pytest-warnings tests/unit && coverage report
```

#### Mock Integration Tests (Docker PostgreSQL)

Mock integration tests provide a fast, local alternative to full integration testing. They use PostgreSQL running in Docker to simulate Redshift behavior without requiring AWS infrastructure.

**Benefits:**
- **No AWS Required**: Runs entirely locally without Redshift or S3
- **Fast Feedback**: Quick setup and execution for rapid development
- **Cost-Free**: No AWS charges for testing
- **CI/CD Friendly**: Easy to integrate into automated pipelines
- **Isolated**: Each test run uses a fresh database

**What Mock Integration Tests Cover:**

Mock integration tests verify core functionality that doesn't depend on Redshift-specific features:
- Basic data loading and COPY operations (using local files instead of S3)
- Table creation and schema management
- Data type conversions and transformations
- Metadata column handling
- Primary key operations and updates
- Unicode character support
- Simple schema evolution

**Limitations:**

Mock tests cannot verify:
- Redshift-specific features (SUPER type, DISTKEY, SORTKEY)
- S3 integration and authentication
- Redshift performance optimizations
- AWS IAM role-based authentication
- Redshift-specific SQL syntax variations

**Running Mock Integration Tests:**

```bash
# Using Makefile (recommended - handles Docker automatically)
make test-mock-integration

# Or manually:
# 1. Start PostgreSQL container
make docker-up

# 2. Run tests
pytest -vv tests/mock_integration

# 3. Stop container when done
make docker-down
```

**Docker Commands:**

```bash
# Start PostgreSQL container
make docker-up

# Stop and remove container
make docker-down

# View container logs
make docker-logs

# Clean up all Docker resources
make clean-docker
```

**Configuration:**

Mock tests use environment variables with sensible defaults:
```bash
export MOCK_TARGET_HOST=localhost
export MOCK_TARGET_PORT=5439  # Matches Redshift default
export MOCK_TARGET_USER=test_user
export MOCK_TARGET_PASSWORD=test_password
export MOCK_TARGET_DBNAME=test_db
export MOCK_TARGET_SCHEMA=test_target_schema
```

The Docker setup is configured in `docker-compose.yml` and uses PostgreSQL 14 Alpine for a lightweight footprint.

#### Integration Tests (Real Redshift)

Integration tests validate end-to-end functionality by connecting to a real Redshift cluster and S3 bucket. These tests verify actual data loading, transformations, and edge cases in a live environment.

**What Integration Tests Cover:**

1. **Basic Data Loading** (in `tests/integration/test_target_redshift.py`)
   - Loading multiple tables with various column types
   - Validates data integrity after loading
   - Tests with and without metadata columns

2. **Compression Methods** (in `tests/integration/test_target_redshift.py`)
   - GZIP compression for S3 uploads
   - BZIP2 compression for S3 uploads
   - Validates data integrity with different compression methods

3. **Performance Configurations** (in `tests/integration/test_target_redshift.py`)
   - Custom parallelism settings (multi-threaded loading)
   - Slicing files for optimal COPY performance
   - Tests different batch size configurations

4. **Metadata and Delete Operations** (in `tests/integration/test_target_redshift.py`)
   - Adding Singer metadata columns (_sdc_extracted_at, _sdc_batched_at, _sdc_deleted_at)
   - Hard delete mode (physical deletion of soft-deleted rows)
   - Validates deleted rows are properly removed

5. **Schema Operations** (in `tests/integration/test_target_redshift.py`)
   - Multiple SCHEMA messages for the same stream
   - Column name changes and schema evolution
   - Column additions with proper NULL handling
   - Reserved words as table names
   - Tables with spaces in names

6. **Character Encoding** (in `tests/integration/test_target_redshift.py`)
   - Unicode characters (Chinese, Russian, Thai, Arabic, Greek)
   - Special characters and escape sequences
   - Long text handling (128, 256, 1K, 4K, 32K+ characters)
   - VARCHAR length validation

7. **Data Flattening** (in `tests/integration/test_target_redshift.py`)
   - Nested JSON objects without flattening (stored as JSON strings)
   - Nested JSON objects with flattening (expanded to columns)
   - Different flattening levels (max_level parameter)

8. **Column Name Normalization** (in `tests/integration/test_target_redshift.py`)
   - camelCase column names
   - Columns with special characters (dashes, underscores)
   - Non-database-friendly naming conventions

9. **Access Control** (in `tests/integration/test_target_redshift.py`)
   - GRANT SELECT privileges to users
   - GRANT SELECT privileges to groups
   - Schema-level USAGE grants
   - Validates permission errors for non-existent users/groups

10. **AWS Authentication Methods** (in `tests/integration/test_target_redshift.py`)
    - Explicit AWS access keys
    - Environment variable credentials
    - IAM role ARN for COPY operations

11. **Advanced Features** (in `tests/integration/test_target_redshift.py`)
    - Custom COPY options
    - Invalid COPY option error handling
    - Custom temporary directory for CSV staging

12. **Logical Replication (CDC)** (in `tests/integration/test_target_redshift.py`)
    - PostgreSQL logical replication streams
    - Handles INSERT, UPDATE, DELETE operations
    - LSN (Log Sequence Number) tracking
    - Bookmark state management with incremental flushes
    - Tests with various batch sizes (default, 5, 10, 1000)

13. **State Management** (in `tests/integration/test_target_redshift.py`)
    - State emission without intermediate flushes
    - State emission with intermediate flushes
    - Per-stream bookmark updates
    - flush_all_streams mode behavior

14. **Record Validation** (in `tests/integration/test_target_redshift.py`)
    - Schema validation when enabled
    - Catches invalid records before loading
    - Tests validation error handling

15. **Error Handling** (in `tests/integration/test_target_redshift.py`)
    - Invalid JSON message handling
    - Invalid message order (RECORD before SCHEMA)
    - Validates appropriate exceptions are raised

16. **Update Strategies** (in `tests/integration/test_target_redshift.py`)
    - skip_updates mode for immutable records
    - Tests that existing records aren't updated when flag is set
    - Validates inserts still work with skip_updates enabled

**Integration Test Configuration:**

Integration tests require a live Redshift cluster and S3 bucket. Configuration is provided via environment variables:

| Environment Variable | Description | Required |
|---------------------|-------------|----------|
| `TARGET_REDSHIFT_HOST` | Redshift cluster endpoint | Yes |
| `TARGET_REDSHIFT_PORT` | Redshift port (usually 5439) | Yes |
| `TARGET_REDSHIFT_USER` | Database user with CREATE/DROP schema permissions | Yes |
| `TARGET_REDSHIFT_PASSWORD` | Database password | Yes |
| `TARGET_REDSHIFT_DBNAME` | Database name | Yes |
| `TARGET_REDSHIFT_SCHEMA` | Target schema for test tables (will be dropped/recreated) | Yes |
| `TARGET_REDSHIFT_AWS_ACCESS_KEY` | AWS access key for S3 operations | Yes |
| `TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY` | AWS secret access key | Yes |
| `TARGET_REDSHIFT_S3_ACL` | S3 object ACL (e.g., "private", "bucket-owner-full-control") | Yes |
| `TARGET_REDSHIFT_S3_BUCKET` | S3 bucket name for staging files | Yes |
| `TARGET_REDSHIFT_S3_KEY_PREFIX` | S3 key prefix/directory for staging files | Yes |
| `TARGET_REDSHIFT_AWS_REDSHIFT_COPY_ROLE_ARN` | (Optional) IAM role ARN for COPY operation | No |

**Important Notes:**

- **Schema Cleanup**: Tests automatically DROP and recreate the target schema. Never use a production schema!
- **S3 Bucket**: The specified S3 bucket will have files written and deleted during tests. Use a dedicated test bucket.
- **Permissions Required**:
  - Redshift: CREATE SCHEMA, DROP SCHEMA, CREATE TABLE, CREATE USER, CREATE GROUP, GRANT
  - S3: PutObject, DeleteObject, GetObject
  - IAM: If using role ARN, the role must have S3 read access

**Running Integration Tests:**

```bash
# Set required environment variables
export TARGET_REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
export TARGET_REDSHIFT_PORT=5439
export TARGET_REDSHIFT_USER=test_user
export TARGET_REDSHIFT_PASSWORD=MySecurePassword123
export TARGET_REDSHIFT_DBNAME=dev
export TARGET_REDSHIFT_SCHEMA=test_target_redshift
export TARGET_REDSHIFT_AWS_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export TARGET_REDSHIFT_S3_ACL=private
export TARGET_REDSHIFT_S3_BUCKET=my-test-bucket
export TARGET_REDSHIFT_S3_KEY_PREFIX=test-target-redshift/

# Run integration tests using Makefile
make test-integration

# Or manually
coverage run -m pytest -vv --disable-pytest-warnings tests/integration && coverage report
```

**Tip:** Consider using a `.env` file with `direnv` or `python-dotenv` to manage environment variables securely. Never commit credentials to version control.

### CI/CD

This project uses GitHub Actions for continuous integration and deployment:

**Continuous Integration:**
- Runs on every push and pull request to `master`/`main` branches
- Tests against Python 3.10, 3.11, and 3.12
- Executes linting (pylint) to maintain code quality
- Runs all unit tests with coverage reporting
- Uploads coverage to Codecov (optional)

**Workflows:**
- `.github/workflows/ci.yml` - Main CI pipeline with unit tests
- `.github/workflows/pythonpublish.yml` - PyPI publishing on release

**Testing in CI:**
```bash
# Unit tests run on all Python versions (3.10, 3.11, 3.12)
coverage run -m pytest -vv tests/unit
coverage report
```

The CI pipeline ensures all contributions are tested and validated before merging.

**Note:** Mock integration tests with Docker are available locally via `make test-mock-integration` but are not run in CI to keep builds fast and simple.

### Quick Setup with Makefile

A Makefile is provided to simplify common development tasks:

```bash
# Show all available commands
make help

# Create and activate virtual environment
make venv
source .venv/bin/activate

# Install package with development dependencies
make install-dev

# Run unit tests with coverage
make test-unit

# Run linting
make lint

# Clean build artifacts and virtual environment
make clean
```

**Platform-specific notes:**
- **Using Makefile (recommended):** The Makefile handles special characters automatically across all shells (bash, zsh, fish, etc.), so `make install-dev` works everywhere.
- **Zsh (macOS Catalina+ default):** If running pip commands manually, escape brackets: `pip install .\[test\]`
- **Bash/sh/other shells:** No escaping needed: `pip install .[test]`
- **Fish shell:** No escaping needed: `pip install .[test]`
- **Windows Command Prompt/PowerShell:** Use quotes: `pip install ".[test]"`

### Manual Testing Setup

For detailed information about what the tests cover and how to configure them, see the [Testing](#testing) section above.

1. Install python dependencies in a virtual env:

<details>
<summary><b>Shell-specific commands (click to expand)</b></summary>

**Bash/sh:**
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install .[test]
```

**Zsh (macOS Catalina+ default):**
```zsh
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install .\[test\]
```

**Fish:**
```fish
python3 -m venv .venv
. .venv/bin/activate.fish
pip install --upgrade pip
pip install .[test]
```

**Windows Command Prompt:**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install --upgrade pip
pip install ".[test]"
```

**Windows PowerShell:**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install ".[test]"
```

</details>

Or simply use the Makefile (works on all platforms with make installed):
```bash
make venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
make install-dev
```

2. To run unit tests:

```bash
  coverage run -m pytest -vv --disable-pytest-warnings tests/unit && coverage report
```

Or using the Makefile:

```bash
  make test-unit
```

3. To run integration tests:

Integration tests require a live Redshift cluster and S3 bucket. See the [Integration Test Configuration](#integration-tests) section for detailed setup instructions including all required environment variables and permissions.

Quick example:

```bash
  export TARGET_REDSHIFT_HOST=<redshift-host>
  export TARGET_REDSHIFT_PORT=<redshift-port>
  export TARGET_REDSHIFT_USER=<redshift-user>
  export TARGET_REDSHIFT_PASSWORD=<redshift-password>
  export TARGET_REDSHIFT_DBNAME=<redshift-database-name>
  export TARGET_REDSHIFT_SCHEMA=<redshift-target-schema>
  export TARGET_REDSHIFT_AWS_ACCESS_KEY=<aws-access-key-id>
  export TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
  export TARGET_REDSHIFT_S3_ACL=<s3-target-acl>
  export TARGET_REDSHIFT_S3_BUCKET=<s3-bucket>
  export TARGET_REDSHIFT_S3_KEY_PREFIX=<s3-bucket-directory>

  coverage run -m pytest -vv --disable-pytest-warnings tests/integration && coverage report
```

Or using the Makefile:

```bash
  make test-integration
```

**Warning:** Integration tests will DROP and recreate the target schema. Never use a production schema!

### Running Linter

<details>
<summary><b>Manual installation (shell-specific)</b></summary>

**Bash/sh:**
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install .[test]
pip install pylint
pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code
```

**Zsh (macOS Catalina+ default):**
```zsh
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install .\[test\]
pip install pylint
pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code
```

**Fish:**
```fish
python3 -m venv .venv
. .venv/bin/activate.fish
pip install --upgrade pip
pip install .[test]
pip install pylint
pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code
```

**Windows (Command Prompt/PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install ".[test]"
pip install pylint
pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code
```

</details>

Or simply use the Makefile (recommended, works on all platforms):

```bash
make install-dev
make lint
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
