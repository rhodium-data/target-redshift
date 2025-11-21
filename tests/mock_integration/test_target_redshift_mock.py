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
