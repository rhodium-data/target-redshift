"""
Mock integration tests for target-redshift using PostgreSQL in Docker.

These tests provide a fast feedback loop without requiring AWS infrastructure.
They test the core loading logic using PostgreSQL as a Redshift substitute.
"""
import pytest
import os

import target_redshift
from target_redshift.db_sync import DbSync

try:
    import tests.mock_integration.utils as test_utils
except ImportError:
    import utils as test_utils


class TestTargetRedshiftMock:
    """
    Mock Integration Tests for Target Redshift using PostgreSQL
    """

    @classmethod
    def setup_class(cls):
        """Wait for PostgreSQL to be ready before running tests"""
        config = test_utils.get_mock_db_config()
        if not test_utils.wait_for_postgres(config, max_retries=30, retry_delay=2):
            pytest.skip("PostgreSQL is not available")

    def setup_method(self):
        """Setup test database"""
        self.config = test_utils.get_mock_db_config()
        postgres = DbSync(self.config)

        # Drop target schema
        if self.config["default_target_schema"]:
            try:
                postgres.query(
                    "DROP SCHEMA IF EXISTS {} CASCADE".format(
                        self.config["default_target_schema"]
                    )
                )
            except Exception as e:
                print(f"Warning: Could not drop schema: {e}")

    def teardown_method(self):
        pass

    def test_connection(self):
        """Test that we can connect to PostgreSQL"""
        postgres = DbSync(self.config)
        result = postgres.query("SELECT 1 as test")
        assert result[0]['test'] == 1

    def test_loading_simple_table(self):
        """Test loading a simple table with basic data types"""
        # Use the same test data as regular integration tests
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Load data
        target_redshift.persist_lines(self.config, tap_lines)

        # Verify data was loaded
        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Note: Tables are created with uppercase names and quoted in Redshift style
        table_one = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_ONE" ORDER BY "C_PK"'.format(target_schema)
        )

        # Check that we got the expected data
        assert len(table_one) == 1
        # Column names are also uppercase when created with quotes
        assert table_one[0]['C_PK'] == 1
        assert table_one[0]['C_INT'] == 1
        assert table_one[0]['C_VARCHAR'] == '1'

    def test_loading_with_metadata_columns(self):
        """Test loading data with metadata columns enabled"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Enable metadata columns
        self.config["add_metadata_columns"] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Verify metadata columns exist
        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        table_one = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_ONE" ORDER BY "C_PK"'.format(target_schema)
        )

        # Check metadata columns exist (uppercase names)
        assert '_SDC_EXTRACTED_AT' in table_one[0]
        assert '_SDC_BATCHED_AT' in table_one[0]
        assert '_SDC_DELETED_AT' in table_one[0]

    def test_use_local_copy_flag(self):
        """Test that use_local_copy flag is properly set"""
        assert self.config.get('use_local_copy') is True

        # Verify S3 client is not initialized
        postgres = DbSync(self.config)
        assert postgres.s3 is None
        assert postgres.use_local_copy is True

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_unicode_characters(self):
        """Test loading unicode characters"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-unicode-characters.json")

        target_redshift.persist_lines(self.config, tap_lines)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_unicode = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_UNICODE" ORDER BY "C_PK"'.format(target_schema)
        )

        # Verify unicode data loaded correctly
        assert len(table_unicode) == 6

        # Verify each row has data and correct PK
        for i, row in enumerate(table_unicode, start=1):
            assert row['C_PK'] == i
            assert row['C_INT'] == i
            assert row['C_VARCHAR'] is not None
            assert len(row['C_VARCHAR']) > 0

        # Verify specific language markers exist in the data
        assert 'Hello world' in table_unicode[0]['C_VARCHAR']  # Greek row
        assert 'Chinese' in table_unicode[1]['C_VARCHAR']  # Chinese row
        assert 'Russian' in table_unicode[2]['C_VARCHAR']  # Russian row
        assert 'Thai' in table_unicode[3]['C_VARCHAR']  # Thai row
        assert 'Arabic' in table_unicode[4]['C_VARCHAR']  # Arabic row
        assert 'Special Characters' in table_unicode[5]['C_VARCHAR']  # Special chars row

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_nested_objects_and_arrays(self):
        """Test loading nested objects and arrays (stored as JSON strings)"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-nested-schema.json")

        target_redshift.persist_lines(self.config, tap_lines)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Query the table - table name gets sanitized to remove hyphens
        result = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_NESTED_SCHEMA" ORDER BY "C_PK"'.format(target_schema)
        )

        assert len(result) == 1
        assert result[0]['C_PK'] == 1

        # Nested objects are stored as JSON strings in VARCHAR columns
        assert 'C_ARRAY' in result[0]
        assert 'C_OBJECT' in result[0]
        assert 'C_NESTED_OBJECT' in result[0]

        # Verify JSON content is preserved
        import json
        c_array = json.loads(result[0]['C_ARRAY'])
        assert c_array == [1, 2, 3]

        c_nested = json.loads(result[0]['C_NESTED_OBJECT'])
        assert c_nested['nested_prop_1'] == 'nested_value_1'
        assert c_nested['nested_prop_3']['multi_nested_prop_1'] == 'multi_value_1'

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_upsert_operations(self):
        """Test that upsert operations work correctly (updates and deletes)"""
        # First load initial data
        tap_lines_initial = test_utils.get_test_tap_lines("messages-with-three-streams.json")
        target_redshift.persist_lines(self.config, tap_lines_initial)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Verify initial load - table name gets sanitized
        initial_result = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_THREE" ORDER BY "C_PK"'.format(target_schema)
        )
        assert len(initial_result) == 3

        # Now load upsert data (updates some records, adds soft deletes)
        tap_lines_upsert = test_utils.get_test_tap_lines("messages-with-three-streams-upserts.json")
        target_redshift.persist_lines(self.config, tap_lines_upsert)

        # Verify updates were applied
        upsert_result = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_THREE" ORDER BY "C_PK"'.format(target_schema)
        )

        # Should have 4 records now (3 original + 1 new)
        assert len(upsert_result) == 4

        # Verify updated values
        pk1_record = [r for r in upsert_result if r['C_PK'] == 1][0]
        assert pk1_record['C_VARCHAR'] == '1_UPDATED'

        pk2_record = [r for r in upsert_result if r['C_PK'] == 2][0]
        assert pk2_record['C_VARCHAR'] == '2_UPDATED'

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_multiple_streams(self):
        """Test loading multiple streams in a single run"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        target_redshift.persist_lines(self.config, tap_lines)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Verify all three tables were created and loaded
        table_one = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_ONE"'.format(target_schema)
        )
        assert table_one[0]['cnt'] == 1

        table_two = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_TWO"'.format(target_schema)
        )
        assert table_two[0]['cnt'] == 2

        # Table name gets sanitized to remove hyphens
        table_three = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_THREE"'.format(target_schema)
        )
        assert table_three[0]['cnt'] == 3

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_schema_evolution(self):
        """Test that schema changes are handled correctly"""
        # First load with initial schema
        tap_lines_initial = test_utils.get_test_tap_lines("messages-with-three-streams.json")
        target_redshift.persist_lines(self.config, tap_lines_initial)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Get initial column count for TEST_TABLE_TWO (gets c_date modified)
        initial_columns = postgres.query(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = '{}'
               AND UPPER(table_name) = 'TEST_TABLE_TWO'
               ORDER BY ordinal_position""".format(target_schema)
        )
        initial_column_count = len(initial_columns)

        # Get initial column count for TEST_TABLE_THREE (gets new c_time_renamed column)
        initial_three_columns = postgres.query(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = '{}'
               AND UPPER(table_name) = 'TEST_TABLE_THREE'
               ORDER BY ordinal_position""".format(target_schema)
        )
        initial_three_column_count = len(initial_three_columns)

        # Load data with modified schema (columns changed/added)
        tap_lines_modified = test_utils.get_test_tap_lines("messages-with-three-streams-modified-column.json")
        target_redshift.persist_lines(self.config, tap_lines_modified)

        # Get new column count for TEST_TABLE_TWO
        modified_columns = postgres.query(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = '{}'
               AND UPPER(table_name) = 'TEST_TABLE_TWO'
               ORDER BY ordinal_position""".format(target_schema)
        )
        modified_column_count = len(modified_columns)

        # Get new column count for TEST_TABLE_THREE
        modified_three_columns = postgres.query(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = '{}'
               AND UPPER(table_name) = 'TEST_TABLE_THREE'
               ORDER BY ordinal_position""".format(target_schema)
        )
        modified_three_column_count = len(modified_three_columns)

        # TEST_TABLE_TWO should have more columns (c_date type changed creates versioned column)
        assert modified_column_count > initial_column_count

        # TEST_TABLE_THREE should have one more column (c_time_renamed added)
        assert modified_three_column_count == initial_three_column_count + 1

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_non_db_friendly_column_names(self):
        """Test that non-database-friendly column names are handled correctly"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-non-db-friendly-columns.json")

        target_redshift.persist_lines(self.config, tap_lines)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Get columns to verify they were sanitized
        columns = postgres.query(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = '{}'
               AND table_name = 'TEST_TABLE_NON_DB_FRIENDLY_COLUMNS'
               ORDER BY ordinal_position""".format(target_schema)
        )

        column_names = [col['column_name'] for col in columns]

        # Verify some data was loaded
        result = postgres.query(
            'SELECT * FROM {}."TEST_TABLE_NON_DB_FRIENDLY_COLUMNS"'.format(target_schema)
        )
        assert len(result) > 0

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'integration', 'resources')),
        reason="Integration test resources not available"
    )
    def test_reserved_word_as_table_name(self):
        """Test that SQL reserved words can be used as table names"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-reserved-name-as-table-name.json")

        target_redshift.persist_lines(self.config, tap_lines)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Verify data was loaded (table name should be quoted to handle reserved word)
        result = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."{}"'.format(target_schema, "ORDER")
        )
        assert result[0]['cnt'] > 0

    def test_soft_delete_with_metadata(self):
        """Test that soft delete preserves records with _sdc_deleted_at column"""
        # Load initial data with metadata columns enabled
        self.config["add_metadata_columns"] = True
        tap_lines_initial = test_utils.get_test_tap_lines("messages-with-three-streams.json")
        target_redshift.persist_lines(self.config, tap_lines_initial)

        postgres = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")

        # Table name gets sanitized
        initial_count = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_THREE"'.format(target_schema)
        )[0]['cnt']
        assert initial_count == 3

        # Load data with soft deletes (hard_delete=False, which is default)
        tap_lines_upsert = test_utils.get_test_tap_lines("messages-with-three-streams-upserts.json")
        target_redshift.persist_lines(self.config, tap_lines_upsert)

        # Count records - soft delete should preserve all records
        final_count = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_THREE"'.format(target_schema)
        )[0]['cnt']

        # With soft delete, all records are preserved (3 original + 1 new)
        assert final_count == 4

        # Check that soft-deleted records have _SDC_DELETED_AT set
        deleted_records = postgres.query(
            'SELECT COUNT(*) as cnt FROM {}."TEST_TABLE_THREE" WHERE "_SDC_DELETED_AT" IS NOT NULL'.format(target_schema)
        )[0]['cnt']
        assert deleted_records == 2  # PKs 3 and 4 have _sdc_deleted_at
