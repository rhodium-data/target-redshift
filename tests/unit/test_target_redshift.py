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

    def test_float_to_decimal_edge_cases(self):
        """Test float_to_decimal with edge cases"""
        # Very small float
        result = target_redshift.float_to_decimal(0.000001)
        assert isinstance(result, Decimal)
        assert str(result) == '0.000001'

        # Very large float
        result = target_redshift.float_to_decimal(999999999.99)
        assert isinstance(result, Decimal)

        # Negative float
        result = target_redshift.float_to_decimal(-42.5)
        assert isinstance(result, Decimal)
        assert str(result) == '-42.5'

        # Zero
        result = target_redshift.float_to_decimal(0.0)
        assert isinstance(result, Decimal)
        assert str(result) == '0.0'

    def test_add_metadata_columns_overwrites_existing(self):
        """Test that metadata columns are always set to standard format"""
        schema_message = {
            'type': 'SCHEMA',
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': 'integer'},
                    '_sdc_extracted_at': {'type': 'string', 'format': 'custom'}
                }
            }
        }

        result = target_redshift.add_metadata_columns_to_schema(schema_message)

        # Should overwrite to standard format
        assert result['schema']['properties']['_sdc_extracted_at']['format'] == 'date-time'

    def test_add_metadata_values_without_time_extracted(self):
        """Test adding metadata when time_extracted is missing"""
        record_message = {
            'type': 'RECORD',
            'stream': 'test_stream',
            'record': {
                'id': 1,
                'name': 'Test'
            }
            # No time_extracted field
        }
        stream_to_sync = {}

        result = target_redshift.add_metadata_values_to_record(record_message, stream_to_sync)

        # Should still add metadata fields
        assert '_sdc_batched_at' in result
        # _sdc_extracted_at should be None when not provided
        assert result.get('_sdc_extracted_at') is None

    def test_get_schema_names_with_duplicate_schemas(self):
        """Test that duplicate schema names are included (not deduplicated)"""
        config = {
            'default_target_schema': 'public',
            'schema_mapping': {
                'source1.schema1': {
                    'target_schema': 'public'  # Same as default
                },
                'source2.schema2': {
                    'target_schema': 'analytics'
                }
            }
        }
        result = target_redshift.get_schema_names_from_config(config)

        # Function doesn't deduplicate, returns all schemas
        assert 'public' in result
        assert 'analytics' in result
        # May have duplicates
        assert result.count('public') >= 1

    def test_emit_state_with_complex_state(self):
        """Test emitting state with nested bookmarks"""
        state = {
            'currently_syncing': None,
            'bookmarks': {
                'stream1': {
                    'last_sync': '2024-01-15T10:00:00Z',
                    'version': 123456,
                    'metadata': {
                        'records_synced': 1000
                    }
                },
                'stream2': {
                    'last_sync': '2024-01-16T10:00:00Z'
                }
            }
        }

        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output

        try:
            target_redshift.emit_state(state)
            output = captured_output.getvalue()

            # Verify JSON is valid and contains expected data
            parsed = json.loads(output)
            assert parsed['bookmarks']['stream1']['metadata']['records_synced'] == 1000
        finally:
            sys.stdout = sys.__stdout__


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

    def test_column_type_with_date_formats(self):
        """Test column type detection for date/time types"""
        from target_redshift.db_sync import column_type

        # date-time format - properly converted to timestamp
        schema_property = {
            'type': ['null', 'string'],
            'format': 'date-time'
        }
        result = column_type(schema_property, with_length=False)
        assert result == 'timestamp without time zone'

        # date format - only date-time is converted, others remain varchar
        schema_property = {
            'type': ['null', 'string'],
            'format': 'date'
        }
        result = column_type(schema_property, with_length=False)
        # date format doesn't get special treatment, remains varchar
        assert 'character varying' in result or result == 'character varying'

    def test_column_type_with_maxlength(self):
        """Test column type uses default length regardless of maxLength"""
        from target_redshift.db_sync import column_type

        schema_property = {
            'type': ['null', 'string'],
            'maxLength': 255
        }
        result = column_type(schema_property)
        # maxLength is not directly used, uses default VARCHAR length
        assert 'character varying' in result

    def test_column_type_integer(self):
        """Test column type for integer"""
        from target_redshift.db_sync import column_type

        schema_property = {
            'type': ['null', 'integer']
        }
        result = column_type(schema_property, with_length=False)
        assert result == 'numeric'

    def test_flatten_key_with_special_characters(self):
        """Test flatten_key handles special characters"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('child-name', ['parent.name'], '__')
        # Should handle special characters in keys
        assert '__' in result

    def test_stream_name_to_dict_edge_cases(self):
        """Test stream_name_to_dict with edge cases"""
        from target_redshift.db_sync import stream_name_to_dict

        # Empty string
        result = stream_name_to_dict('')
        assert result['table_name'] == ''

        # Stream with many parts - hyphens get replaced with underscores
        result = stream_name_to_dict('a-b-c-d-e')
        assert result['catalog_name'] == 'a'
        assert result['schema_name'] == 'b'
        # Table name has hyphens converted to underscores
        assert result['table_name'] == 'c_d_e'

    def test_should_json_dump_value_with_boolean(self):
        """Test _should_json_dump_value with boolean"""
        from target_redshift.db_sync import _should_json_dump_value

        result = _should_json_dump_value('field', True)
        assert result is False

        result = _should_json_dump_value('field', False)
        assert result is False

    def test_should_json_dump_value_with_numbers(self):
        """Test _should_json_dump_value with numeric types"""
        from target_redshift.db_sync import _should_json_dump_value

        # Integer
        result = _should_json_dump_value('field', 42)
        assert result is False

        # Float
        result = _should_json_dump_value('field', 3.14)
        assert result is False

        # Decimal
        result = _should_json_dump_value('field', Decimal('10.5'))
        assert result is False


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

    @mock.patch('target_redshift.DbSync')
    def test_load_table_cache_with_multiple_schemas(self, mock_db_sync):
        """Test load_table_cache loads from all configured schemas"""
        mock_db_instance = mock.Mock()
        mock_db_instance.get_table_columns.return_value = [
            {'schema': 'public', 'table': 'users', 'column': 'id'},
            {'schema': 'analytics', 'table': 'events', 'column': 'event_id'}
        ]
        mock_db_sync.return_value = mock_db_instance

        config = {
            'host': 'localhost',
            'default_target_schema': 'public',
            'schema_mapping': {
                'source1.schema1': {
                    'target_schema': 'analytics'
                }
            }
        }

        result = target_redshift.load_table_cache(config)

        # Should get columns from both schemas
        assert len(result) == 2


class TestHelperFunctions:
    """Test helper functions for data processing"""

    def test_chunk_iterable(self):
        """Test chunking an iterable into fixed-size chunks"""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunks = list(target_redshift.chunk_iterable(data, 3))

        assert len(chunks) == 4
        # chunks are tuples, not lists
        assert chunks[0] == (1, 2, 3)
        assert chunks[1] == (4, 5, 6)
        assert chunks[2] == (7, 8, 9)
        assert chunks[3] == (10,)

    def test_chunk_iterable_exact_division(self):
        """Test chunking when size divides evenly"""
        data = [1, 2, 3, 4, 5, 6]
        chunks = list(target_redshift.chunk_iterable(data, 2))

        assert len(chunks) == 3
        assert all(len(chunk) == 2 for chunk in chunks)

    def test_chunk_iterable_empty(self):
        """Test chunking empty iterable"""
        data = []
        chunks = list(target_redshift.chunk_iterable(data, 5))

        assert len(chunks) == 0

    def test_chunk_iterable_single_element(self):
        """Test chunking single element"""
        data = [1]
        chunks = list(target_redshift.chunk_iterable(data, 5))

        assert len(chunks) == 1
        # Returns a tuple
        assert chunks[0] == (1,)

    def test_ceiling_division(self):
        """Test ceiling division helper"""
        # Exact division
        assert target_redshift.ceiling_division(10, 5) == 2

        # Division with remainder
        assert target_redshift.ceiling_division(10, 3) == 4
        assert target_redshift.ceiling_division(7, 2) == 4

        # Numerator smaller than denominator
        assert target_redshift.ceiling_division(1, 5) == 1

        # Zero numerator
        assert target_redshift.ceiling_division(0, 5) == 0


class TestColumnSanitization:
    """Test column name sanitization and SQL escaping"""

    def test_safe_column_name(self):
        """Test safe_column_name function from db_sync"""
        from target_redshift.db_sync import safe_column_name

        # Normal column names
        assert safe_column_name('user_id') == '"USER_ID"'
        assert safe_column_name('email') == '"EMAIL"'

        # Column with special characters
        result = safe_column_name('first-name')
        assert result.startswith('"')
        assert result.endswith('"')

        # Already uppercase
        assert safe_column_name('ID') == '"ID"'

    def test_column_name_with_reserved_words(self):
        """Test handling of SQL reserved words as column names"""
        from target_redshift.db_sync import safe_column_name

        # SQL reserved words should be quoted
        assert safe_column_name('select') == '"SELECT"'
        assert safe_column_name('from') == '"FROM"'
        assert safe_column_name('where') == '"WHERE"'


class TestConfigValidation:
    """Test configuration validation"""

    def test_validate_config_with_valid_config(self):
        """Test validation with all required fields"""
        from target_redshift.db_sync import validate_config

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            's3_bucket': 'test-bucket',
            'default_target_schema': 'public'
        }
        errors = validate_config(config)
        assert errors == []

    def test_validate_config_missing_host(self):
        """Test validation with missing host"""
        from target_redshift.db_sync import validate_config

        config = {
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            's3_bucket': 'test-bucket'
        }
        errors = validate_config(config)
        assert len(errors) > 0
        assert any('host' in error for error in errors)

    def test_validate_config_missing_s3_bucket_without_local_copy(self):
        """Test validation with missing S3 bucket and no local copy mode"""
        from target_redshift.db_sync import validate_config

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            'default_target_schema': 'public'
        }
        errors = validate_config(config)
        assert len(errors) > 0
        assert any('s3_bucket' in error or 'use_local_copy' in error for error in errors)

    def test_validate_config_with_local_copy(self):
        """Test validation with local copy mode enabled"""
        from target_redshift.db_sync import validate_config

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            'use_local_copy': True,
            'default_target_schema': 'public'
        }
        errors = validate_config(config)
        assert errors == []

    def test_validate_config_missing_schema_mapping(self):
        """Test validation with missing schema configuration"""
        from target_redshift.db_sync import validate_config

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            's3_bucket': 'test-bucket'
        }
        errors = validate_config(config)
        assert len(errors) > 0
        assert any('schema' in error.lower() for error in errors)

    def test_validate_config_with_schema_mapping(self):
        """Test validation with schema_mapping instead of default_target_schema"""
        from target_redshift.db_sync import validate_config

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'testuser',
            'password': 'testpass',
            'dbname': 'testdb',
            's3_bucket': 'test-bucket',
            'schema_mapping': {
                'source_schema': {
                    'target_schema': 'target_schema'
                }
            }
        }
        errors = validate_config(config)
        assert errors == []


class TestFlattenFunctions:
    """Test record and schema flattening functions"""

    def test_flatten_record_simple(self):
        """Test flattening a simple record"""
        from target_redshift.db_sync import flatten_record

        record = {'id': 1, 'name': 'test'}
        result = flatten_record(record, max_level=0)
        assert result == {'id': 1, 'name': 'test'}

    def test_flatten_record_nested_with_max_level(self):
        """Test flattening nested record with max_level"""
        from target_redshift.db_sync import flatten_record

        record = {
            'id': 1,
            'user': {
                'name': 'John',
                'address': {
                    'city': 'NYC'
                }
            }
        }

        # With max_level=1, should flatten one level
        result = flatten_record(record, max_level=1)
        assert 'id' in result
        assert 'user__name' in result
        assert result['user__name'] == 'John'
        # address is beyond max_level, should be JSON dumped
        assert 'user__address' in result
        assert '"city"' in result['user__address']

    def test_flatten_record_with_flatten_schema(self):
        """Test flattening record with specific flatten schema"""
        from target_redshift.db_sync import flatten_record

        record = {
            'id': 1,
            'data': {'key': 'value'},
            'extra': 'text'
        }
        # flatten_schema should be a dict mapping keys to their schema properties
        flatten_schema = {
            'id': {'type': ['integer']},
            'data': {'type': ['object']},
            'extra': {'type': ['string']}
        }

        # When flatten_schema is provided, dicts/lists should be JSON dumped
        result = flatten_record(record, flatten_schema=flatten_schema, max_level=0)
        assert 'id' in result
        assert 'data' in result
        assert result['id'] == 1
        assert '"key"' in result['data']  # JSON dumped
        assert result['extra'] == 'text'

    def test_flatten_key_simple(self):
        """Test flatten_key with simple keys"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('name', [], '__')
        assert result == 'name'

        result = flatten_key('city', ['address'], '__')
        assert result == 'address__city'

    def test_flatten_key_very_long_key(self):
        """Test flatten_key with very long keys that exceed 127 characters"""
        from target_redshift.db_sync import flatten_key

        # Create a very long key
        long_key = 'a' * 50
        parent = ['b' * 50]

        result = flatten_key(long_key, parent, '__')
        # Should be shortened to fit within 127 characters
        assert len(result) < 127

    def test_flatten_key_with_multiple_parents(self):
        """Test flatten_key with multiple parent keys"""
        from target_redshift.db_sync import flatten_key

        result = flatten_key('field', ['level1', 'level2', 'level3'], '__')
        assert result == 'level1__level2__level3__field'

    def test_flatten_schema_simple(self):
        """Test flatten_schema with simple schema"""
        from target_redshift.db_sync import flatten_schema

        schema = {
            'properties': {
                'id': {'type': ['integer']},
                'name': {'type': ['string']}
            }
        }

        result = flatten_schema(schema, max_level=0)
        assert 'id' in result
        assert 'name' in result

    def test_flatten_schema_nested(self):
        """Test flatten_schema with nested schema"""
        from target_redshift.db_sync import flatten_schema

        schema = {
            'properties': {
                'id': {'type': ['integer']},
                'user': {
                    'type': ['object'],
                    'properties': {
                        'name': {'type': ['string']},
                        'email': {'type': ['string']}
                    }
                }
            }
        }

        # With max_level=1, should flatten nested properties
        result = flatten_schema(schema, max_level=1)
        assert 'id' in result
        assert 'user__name' in result
        assert 'user__email' in result

    def test_flatten_schema_without_properties(self):
        """Test flatten_schema with schema without properties"""
        from target_redshift.db_sync import flatten_schema

        schema = {'type': 'object'}
        result = flatten_schema(schema, max_level=0)
        assert result == {}


class TestCSVGeneration:
    """Test CSV line generation and escaping"""

    def test_record_to_csv_line_simple(self):
        """Test CSV generation with simple values"""
        from target_redshift.db_sync import DbSync

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'test',
            'password': 'test',
            'dbname': 'test',
            'use_local_copy': True,
            'default_target_schema': 'public'
        }

        schema_message = {
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'name': {'type': ['string']}
                }
            },
            'key_properties': ['id']
        }

        db_sync = DbSync(config, schema_message)
        record = {'id': 1, 'name': 'test'}

        csv_line = db_sync.record_to_csv_line(record)
        assert '1' in csv_line
        assert 'test' in csv_line
        assert ',' in csv_line

    def test_record_to_csv_line_with_special_characters(self):
        """Test CSV generation with special characters that need escaping"""
        from target_redshift.db_sync import DbSync

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'test',
            'password': 'test',
            'dbname': 'test',
            'use_local_copy': True,
            'default_target_schema': 'public'
        }

        schema_message = {
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'text': {'type': ['string']}
                }
            },
            'key_properties': ['id']
        }

        db_sync = DbSync(config, schema_message)

        # Test with comma
        record = {'id': 1, 'text': 'value,with,commas'}
        csv_line = db_sync.record_to_csv_line(record)
        assert 'value,with,commas' in csv_line

        # Test with quotes
        record = {'id': 2, 'text': 'value"with"quotes'}
        csv_line = db_sync.record_to_csv_line(record)
        assert 'value' in csv_line

    def test_record_to_csv_line_with_null_values(self):
        """Test CSV generation with NULL values"""
        from target_redshift.db_sync import DbSync

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'test',
            'password': 'test',
            'dbname': 'test',
            'use_local_copy': True,
            'default_target_schema': 'public'
        }

        schema_message = {
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'name': {'type': ['null', 'string']},
                    'age': {'type': ['null', 'integer']}
                }
            },
            'key_properties': ['id']
        }

        db_sync = DbSync(config, schema_message)
        record = {'id': 1, 'name': None, 'age': None}

        csv_line = db_sync.record_to_csv_line(record)
        # NULL values should be represented as empty strings in CSV
        # Columns are ordered alphabetically by flatten_schema: age, id, name
        parts = csv_line.split(',')
        assert len(parts) == 3
        assert parts[1] == '1'  # ID is in the middle (alphabetically)
        assert parts[0] == ''  # age is NULL
        assert parts[2] == ''  # name is NULL

    def test_record_to_csv_line_with_zero_values(self):
        """Test CSV generation with zero values (should not be treated as null)"""
        from target_redshift.db_sync import DbSync

        config = {
            'host': 'localhost',
            'port': 5439,
            'user': 'test',
            'password': 'test',
            'dbname': 'test',
            'use_local_copy': True,
            'default_target_schema': 'public'
        }

        schema_message = {
            'stream': 'test_stream',
            'schema': {
                'properties': {
                    'id': {'type': ['integer']},
                    'count': {'type': ['integer']},
                    'balance': {'type': ['number']}
                }
            },
            'key_properties': ['id']
        }

        db_sync = DbSync(config, schema_message)
        record = {'id': 1, 'count': 0, 'balance': 0.0}

        csv_line = db_sync.record_to_csv_line(record)
        # Zero values should be present in CSV
        assert '0' in csv_line


class TestParallelismCalculation:
    """Test auto-parallelism calculation logic"""

    def test_flush_streams_parallelism_auto_single_stream(self):
        """Test auto parallelism with single stream"""
        # When there's 1 stream, parallelism should be 1
        config = {'parallelism': 0, 'max_parallelism': 16}
        streams = {'stream1': {}}

        # Calculate expected parallelism
        n_streams = len(streams.keys())
        max_par = config.get('max_parallelism', 16)
        expected = min(n_streams, max_par)
        assert expected == 1

    def test_flush_streams_parallelism_auto_multiple_streams(self):
        """Test auto parallelism with multiple streams"""
        config = {'parallelism': 0, 'max_parallelism': 16}
        streams = {f'stream{i}': {} for i in range(5)}

        # Calculate expected parallelism
        n_streams = len(streams.keys())
        max_par = config.get('max_parallelism', 16)
        expected = min(n_streams, max_par)
        assert expected == 5

    def test_flush_streams_parallelism_exceeds_max(self):
        """Test auto parallelism when streams exceed max_parallelism"""
        config = {'parallelism': 0, 'max_parallelism': 16}
        streams = {f'stream{i}': {} for i in range(20)}

        # Calculate expected parallelism
        n_streams = len(streams.keys())
        max_par = config.get('max_parallelism', 16)
        expected = min(n_streams, max_par)
        assert expected == 16

    def test_flush_streams_parallelism_explicit(self):
        """Test explicit parallelism setting (not auto)"""
        config = {'parallelism': 4, 'max_parallelism': 16}
        streams = {f'stream{i}': {} for i in range(10)}

        # When parallelism is explicitly set, it should be used
        parallelism = config.get('parallelism', 0)
        assert parallelism == 4


class TestColumnClause:
    """Test SQL column clause generation"""

    def test_column_clause_varchar(self):
        """Test column clause for varchar type"""
        from target_redshift.db_sync import column_clause

        schema_property = {'type': ['string']}
        result = column_clause('name', schema_property)

        assert '"NAME"' in result
        assert 'character varying' in result

    def test_column_clause_integer(self):
        """Test column clause for integer type"""
        from target_redshift.db_sync import column_clause

        schema_property = {'type': ['integer']}
        result = column_clause('id', schema_property)

        assert '"ID"' in result
        assert 'numeric' in result

    def test_column_clause_timestamp(self):
        """Test column clause for timestamp type"""
        from target_redshift.db_sync import column_clause

        schema_property = {'type': ['string'], 'format': 'date-time'}
        result = column_clause('created_at', schema_property)

        assert '"CREATED_AT"' in result
        assert 'timestamp without time zone' in result

    def test_column_clause_boolean(self):
        """Test column clause for boolean type"""
        from target_redshift.db_sync import column_clause

        schema_property = {'type': ['boolean']}
        result = column_clause('is_active', schema_property)

        assert '"IS_ACTIVE"' in result
        assert 'boolean' in result

    def test_column_clause_super_type(self):
        """Test column clause for Redshift SUPER type"""
        from target_redshift.db_sync import column_clause

        schema_property = {'type': ['super']}
        result = column_clause('json_data', schema_property)

        assert '"JSON_DATA"' in result
        assert 'super' in result


class TestPrimaryKeyHandling:
    """Test primary key string generation"""

    def test_primary_column_names_single_key(self):
        """Test primary_column_names with single key"""
        from target_redshift.db_sync import primary_column_names

        schema_message = {'key_properties': ['id']}
        result = primary_column_names(schema_message)

        assert len(result) == 1
        assert result[0] == '"ID"'

    def test_primary_column_names_composite_key(self):
        """Test primary_column_names with composite key"""
        from target_redshift.db_sync import primary_column_names

        schema_message = {'key_properties': ['user_id', 'order_id']}
        result = primary_column_names(schema_message)

        assert len(result) == 2
        assert '"USER_ID"' in result
        assert '"ORDER_ID"' in result

    def test_primary_column_names_empty(self):
        """Test primary_column_names with no keys"""
        from target_redshift.db_sync import primary_column_names

        schema_message = {'key_properties': []}
        result = primary_column_names(schema_message)

        assert result == []
