"""Unit tests for target_redshift utility functions"""
import pytest
from datetime import datetime
from decimal import Decimal
import target_redshift


class TestUtilityFunctions:
    """Test utility functions in target_redshift.__init__"""

    def test_float_to_decimal_with_float(self):
        """Test converting float to Decimal"""
        result = target_redshift.float_to_decimal(3.14159)
        assert isinstance(result, Decimal)
        assert str(result) == '3.14159'

    def test_float_to_decimal_with_list(self):
        """Test converting list of floats to Decimals"""
        result = target_redshift.float_to_decimal([1.5, 2.5, 3.5])
        assert len(result) == 3
        assert all(isinstance(x, Decimal) for x in result)
        assert str(result[0]) == '1.5'
        assert str(result[1]) == '2.5'
        assert str(result[2]) == '3.5'

    def test_float_to_decimal_with_dict(self):
        """Test converting dict with floats to Decimals"""
        result = target_redshift.float_to_decimal({
            'price': 99.99,
            'tax': 8.25
        })
        assert isinstance(result['price'], Decimal)
        assert isinstance(result['tax'], Decimal)
        assert str(result['price']) == '99.99'
        assert str(result['tax']) == '8.25'

    def test_float_to_decimal_with_nested_structure(self):
        """Test converting nested data structure"""
        result = target_redshift.float_to_decimal({
            'items': [
                {'price': 10.5, 'quantity': 2},
                {'price': 20.99, 'quantity': 1}
            ],
            'total': 42.99
        })
        assert isinstance(result['total'], Decimal)
        assert isinstance(result['items'][0]['price'], Decimal)
        assert isinstance(result['items'][1]['price'], Decimal)
        assert str(result['total']) == '42.99'

    def test_float_to_decimal_with_non_float(self):
        """Test that non-float values pass through unchanged"""
        # Strings
        assert target_redshift.float_to_decimal('hello') == 'hello'
        # Integers
        assert target_redshift.float_to_decimal(42) == 42
        # None
        assert target_redshift.float_to_decimal(None) is None
        # Boolean
        assert target_redshift.float_to_decimal(True) is True

    def test_add_metadata_columns_to_schema(self):
        """Test adding metadata columns to schema"""
        schema_message = {
            'type': 'SCHEMA',
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': 'integer'},
                    'name': {'type': 'string'}
                }
            }
        }

        result = target_redshift.add_metadata_columns_to_schema(schema_message)

        # Check that original properties still exist
        assert 'id' in result['schema']['properties']
        assert 'name' in result['schema']['properties']

        # Check that metadata columns were added
        assert '_sdc_extracted_at' in result['schema']['properties']
        assert '_sdc_batched_at' in result['schema']['properties']
        assert '_sdc_deleted_at' in result['schema']['properties']

        # Check metadata column definitions
        assert result['schema']['properties']['_sdc_extracted_at']['type'] == ['null', 'string']
        assert result['schema']['properties']['_sdc_extracted_at']['format'] == 'date-time'
        assert result['schema']['properties']['_sdc_batched_at']['type'] == ['null', 'string']
        assert result['schema']['properties']['_sdc_batched_at']['format'] == 'date-time'
        assert result['schema']['properties']['_sdc_deleted_at']['type'] == ['null', 'string']

    def test_add_metadata_values_to_record(self):
        """Test adding metadata values to record"""
        record_message = {
            'type': 'RECORD',
            'stream': 'test_stream',
            'record': {
                'id': 1,
                'name': 'Test'
            },
            'time_extracted': '2024-01-15T10:30:00Z'
        }
        stream_to_sync = {}

        result = target_redshift.add_metadata_values_to_record(record_message, stream_to_sync)

        # Check original fields
        assert result['id'] == 1
        assert result['name'] == 'Test'

        # Check metadata fields
        assert result['_sdc_extracted_at'] == '2024-01-15T10:30:00Z'
        assert '_sdc_batched_at' in result
        # Verify batched_at is a valid ISO datetime
        datetime.fromisoformat(result['_sdc_batched_at'].replace('Z', '+00:00'))

    def test_add_metadata_values_to_record_with_deleted(self):
        """Test adding metadata to a deleted record"""
        record_message = {
            'type': 'RECORD',
            'stream': 'test_stream',
            'record': {
                'id': 1,
                'name': 'Test',
                '_sdc_deleted_at': '2024-01-15T12:00:00Z'
            },
            'time_extracted': '2024-01-15T10:30:00Z'
        }
        stream_to_sync = {}

        result = target_redshift.add_metadata_values_to_record(record_message, stream_to_sync)

        # Check that deleted_at was preserved
        assert result['_sdc_deleted_at'] == '2024-01-15T12:00:00Z'

    def test_get_schema_names_from_config_with_default(self):
        """Test getting schema names with only default_target_schema"""
        config = {
            'default_target_schema': 'public'
        }
        result = target_redshift.get_schema_names_from_config(config)
        assert result == ['public']

    def test_get_schema_names_from_config_with_mapping(self):
        """Test getting schema names with schema_mapping"""
        config = {
            'default_target_schema': 'public',
            'schema_mapping': {
                'source_db.source_schema': {
                    'target_schema': 'target_schema_1'
                },
                'another_db.another_schema': {
                    'target_schema': 'target_schema_2'
                }
            }
        }
        result = target_redshift.get_schema_names_from_config(config)
        assert 'public' in result
        assert 'target_schema_1' in result
        assert 'target_schema_2' in result
        assert len(result) == 3

    def test_get_schema_names_from_config_empty(self):
        """Test getting schema names from empty config"""
        config = {}
        result = target_redshift.get_schema_names_from_config(config)
        assert result == []

    def test_emit_state(self, capsys):
        """Test state emission to stdout"""
        state = {
            'currently_syncing': 'test_stream',
            'bookmarks': {
                'test_stream': {
                    'last_sync': '2024-01-15T10:00:00Z'
                }
            }
        }

        target_redshift.emit_state(state)

        captured = capsys.readouterr()
        assert 'currently_syncing' in captured.out
        assert 'test_stream' in captured.out
        assert 'bookmarks' in captured.out

    def test_emit_state_with_none(self, capsys):
        """Test that emit_state handles None gracefully"""
        target_redshift.emit_state(None)

        captured = capsys.readouterr()
        # Should not emit anything for None
        assert captured.out == ''
