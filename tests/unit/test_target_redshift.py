"""Unit tests for target_redshift utility functions"""
import pytest
import json
from datetime import datetime
from decimal import Decimal
from unittest import mock
import target_redshift
from target_redshift import RecordValidationException, InvalidValidationOperationException


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


class TestRecordProcessing:
    """Test record processing and validation"""

    def test_record_validation_exception(self):
        """Test RecordValidationException can be raised"""
        with pytest.raises(RecordValidationException) as exc_info:
            raise RecordValidationException("Invalid record")
        assert "Invalid record" in str(exc_info.value)

    def test_invalid_validation_operation_exception(self):
        """Test InvalidValidationOperationException can be raised"""
        with pytest.raises(InvalidValidationOperationException) as exc_info:
            raise InvalidValidationOperationException("Invalid operation")
        assert "Invalid operation" in str(exc_info.value)


class TestDbSyncUtilities:
    """Test additional db_sync utility functions"""

    def test_column_type_with_integer_and_string(self):
        """Test column type for mixed integer/string"""
        from target_redshift.db_sync import column_type

        schema_property = {
            'type': ['null', 'integer', 'string']
        }
        result = column_type(schema_property)
        assert 'character varying' in result
        # Should default to LONG_VARCHAR_LENGTH
        assert '65535' in result

    def test_column_type_boolean(self):
        """Test column type for boolean"""
        from target_redshift.db_sync import column_type

        schema_property = {
            'type': ['null', 'boolean']
        }
        result = column_type(schema_property, with_length=False)
        assert result == 'boolean'

    def test_column_type_number(self):
        """Test column type for number (float)"""
        from target_redshift.db_sync import column_type

        schema_property = {
            'type': ['null', 'number']
        }
        result = column_type(schema_property, with_length=False)
        assert result == 'double precision'

    def test_column_trans_with_super_type(self):
        """Test column transformation for SUPER type"""
        from target_redshift.db_sync import column_trans

        # Test with 'super' in type
        schema_property = {
            'type': ['null', 'super']
        }
        result = column_trans(schema_property)
        assert result == ''

    def test_column_trans_with_super_format(self):
        """Test column transformation for SUPER format"""
        from target_redshift.db_sync import column_trans

        # Test with format='super'
        schema_property = {
            'type': ['null', 'string'],
            'format': 'super'
        }
        result = column_trans(schema_property)
        assert result == ''

    def test_column_trans_with_object(self):
        """Test column transformation for object type"""
        from target_redshift.db_sync import column_trans

        schema_property = {
            'type': ['null', 'object']
        }
        result = column_trans(schema_property)
        assert result == 'parse_json'

    def test_column_trans_with_array(self):
        """Test column transformation for array type"""
        from target_redshift.db_sync import column_trans

        schema_property = {
            'type': ['null', 'array']
        }
        result = column_trans(schema_property)
        assert result == 'parse_json'

    def test_column_trans_with_string(self):
        """Test column transformation for plain string"""
        from target_redshift.db_sync import column_trans

        schema_property = {
            'type': ['null', 'string']
        }
        result = column_trans(schema_property)
        assert result == ''

    def test_flatten_key_simple(self):
        """Test flatten_key with simple key"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('child', ['parent'], '__')
        assert result == 'parent__child'

    def test_flatten_key_with_empty_parent(self):
        """Test flatten_key with no parent"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('key', [], '__')
        assert result == 'key'

    def test_flatten_key_with_multiple_parents(self):
        """Test flatten_key with nested parents"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('child', ['grandparent', 'parent'], '__')
        assert result == 'grandparent__parent__child'

    def test_primary_column_names_single_key(self):
        """Test extracting primary key from schema"""
        from target_redshift.db_sync import primary_column_names

        stream_schema_message = {
            'key_properties': ['id']
        }
        result = primary_column_names(stream_schema_message)
        assert result == ['"ID"']

    def test_primary_column_names_composite_key(self):
        """Test extracting composite primary key"""
        from target_redshift.db_sync import primary_column_names

        stream_schema_message = {
            'key_properties': ['org_id', 'user_id']
        }
        result = primary_column_names(stream_schema_message)
        assert result == ['"ORG_ID"', '"USER_ID"']

    def test_primary_column_names_no_keys(self):
        """Test extracting primary keys when none defined"""
        from target_redshift.db_sync import primary_column_names

        stream_schema_message = {
            'key_properties': []
        }
        result = primary_column_names(stream_schema_message)
        assert result == []

    def test_stream_name_to_dict_full(self):
        """Test parsing stream name with catalog-schema-table"""
        from target_redshift.db_sync import stream_name_to_dict

        result = stream_name_to_dict('my_catalog-my_schema-my_table')
        assert result['catalog_name'] == 'my_catalog'
        assert result['schema_name'] == 'my_schema'
        assert result['table_name'] == 'my_table'

    def test_stream_name_to_dict_schema_table(self):
        """Test parsing stream name with schema-table"""
        from target_redshift.db_sync import stream_name_to_dict

        result = stream_name_to_dict('my_schema-my_table')
        assert result['catalog_name'] is None
        assert result['schema_name'] == 'my_schema'
        assert result['table_name'] == 'my_table'

    def test_stream_name_to_dict_table_only(self):
        """Test parsing stream name with table only"""
        from target_redshift.db_sync import stream_name_to_dict

        result = stream_name_to_dict('my_table')
        assert result['catalog_name'] is None
        assert result['schema_name'] is None
        assert result['table_name'] == 'my_table'

    def test_stream_name_to_dict_custom_separator(self):
        """Test parsing stream name with custom separator"""
        from target_redshift.db_sync import stream_name_to_dict

        result = stream_name_to_dict('catalog_schema_table', separator='_')
        assert result['catalog_name'] == 'catalog'
        assert result['schema_name'] == 'schema'
        assert result['table_name'] == 'table'

    def test_should_json_dump_value_with_dict(self):
        """Test _should_json_dump_value with dictionary"""
        from target_redshift.db_sync import _should_json_dump_value

        value = {'key': 'value'}
        result = _should_json_dump_value('field', value)
        assert result is True

    def test_should_json_dump_value_with_list(self):
        """Test _should_json_dump_value with list"""
        from target_redshift.db_sync import _should_json_dump_value

        value = [1, 2, 3]
        result = _should_json_dump_value('field', value)
        assert result is True

    def test_should_json_dump_value_with_string(self):
        """Test _should_json_dump_value with string"""
        from target_redshift.db_sync import _should_json_dump_value

        value = "simple string"
        result = _should_json_dump_value('field', value)
        assert result is False

    def test_should_json_dump_value_with_flatten_schema_requiring_dump(self):
        """Test _should_json_dump_value with flatten schema that requires JSON dump"""
        from target_redshift.db_sync import _should_json_dump_value

        flatten_schema = {
            'field': {'type': 'string'}  # Will require JSON dump for complex types
        }
        value = {'nested': 'object'}
        result = _should_json_dump_value('field', value, flatten_schema)
        assert result is True


class TestLoadTableCache:
    """Test load_table_cache function"""

    @mock.patch('target_redshift.DbSync')
    def test_load_table_cache_enabled(self, mock_db_sync):
        """Test load_table_cache when cache is enabled"""
        mock_db_instance = mock.Mock()
        mock_db_instance.get_table_columns.return_value = [
            {'schema': 'public', 'table': 'users', 'column': 'id'},
            {'schema': 'public', 'table': 'users', 'column': 'name'}
        ]
        mock_db_sync.return_value = mock_db_instance

        config = {
            'host': 'localhost',
            'default_target_schema': 'public'
        }

        result = target_redshift.load_table_cache(config)

        assert len(result) == 2
        mock_db_instance.get_table_columns.assert_called_once()

    def test_load_table_cache_disabled(self):
        """Test load_table_cache when cache is disabled"""
        config = {
            'host': 'localhost',
            'disable_table_cache': True
        }

        result = target_redshift.load_table_cache(config)

        assert result == []
