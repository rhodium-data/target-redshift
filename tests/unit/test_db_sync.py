import pytest
import mock
import target_redshift


class TestTargetRedshift(object):
    """
    Unit Tests for Target Redshift
    """
    def setup_method(self):
        self.config = {}


    def teardown_method(self):
        pass


    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_redshift.db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'host':                     "dummy-value",
            'port':                     5439,
            'user':                     "dummy-value",
            'password':                 "dummy-value",
            'dbname':                   "dummy-value",
            'aws_access_key_id':        "dummy-value",
            'aws_secret_access_key':    "dummy-value",
            's3_bucket':                "dummy-value",
            'default_target_schema':    "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors > 0)
        assert len(validator(empty_config)) > 0

        # Minimal configuratino should pass - (nr_of_errors == 0)
        assert len(validator(minimal_config)) == 0

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        assert len(validator(config_with_no_schema)) > 0

        # Configuration with schema mapping - (nr_of_errors == 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        assert len(validator(config_with_schema_mapping)) == 0


    def test_column_type_mapping(self):
        """Test JSON type to Redshift column type mappings"""
        mapper = target_redshift.db_sync.column_type

        # Incoming JSON schema types
        json_str =          {"type": ["string"]             }
        json_str_or_null =  {"type": ["string", "null"]     }
        json_dt =           {"type": ["string"]             , "format": "date-time"}
        json_dt_or_null =   {"type": ["string", "null"]     , "format": "date-time"}
        json_t =            {"type": ["string"]             , "format": "time"}
        json_t_or_null =    {"type": ["string", "null"]     , "format": "time"}
        json_num =          {"type": ["number"]             }
        json_int =          {"type": ["integer"]            }
        json_int_or_str =   {"type": ["integer", "string"]  }
        json_bool =         {"type": ["boolean"]            }
        json_obj =          {"type": ["object"]             }
        json_arr =          {"type": ["array"]              }
        json_super =        {"type": "super"                }
        json_super_list =   {"type": ["super"]              }
        json_super_null =   {"type": ["super", "null"]      }
        json_obj_super =    {"type": ["object"]             , "format": "super"}

        # Mapping from JSON schema types ot Redshift column types
        assert mapper(json_str)          == 'character varying(10000)'
        assert mapper(json_str_or_null)  == 'character varying(10000)'
        assert mapper(json_dt)           == 'timestamp without time zone'
        assert mapper(json_dt_or_null)   == 'timestamp without time zone'
        assert mapper(json_t)            == 'character varying(256)'
        assert mapper(json_t_or_null)    == 'character varying(256)'
        assert mapper(json_num)          == 'double precision'
        assert mapper(json_int)          == 'numeric'
        assert mapper(json_int_or_str)   == 'character varying(65535)'
        assert mapper(json_bool)         == 'boolean'
        assert mapper(json_obj)          == 'character varying(65535)'
        assert mapper(json_arr)          == 'character varying(65535)'
        assert mapper(json_super)        == 'super'
        assert mapper(json_super_list)   == 'super'
        assert mapper(json_super_null)   == 'super'
        assert mapper(json_obj_super)    == 'super'

        # Test with maxLength that exceeds DEFAULT_VARCHAR_LENGTH
        json_str_long = {"type": ["string"], "maxLength": 50000}
        assert mapper(json_str_long) == 'character varying(65535)'

        # Test with_length=False parameter
        json_str_no_length = {"type": ["string"]}
        assert mapper(json_str_no_length, with_length=False) == 'character varying'

        json_obj_no_length = {"type": ["object"]}
        assert mapper(json_obj_no_length, with_length=False) == 'character varying'

        json_time_no_length = {"type": ["string"], "format": "time"}
        assert mapper(json_time_no_length, with_length=False) == 'character varying'


    def test_column_trans_mapping(self):
        """Test column transformation function mappings"""
        trans_mapper = target_redshift.db_sync.column_trans

        # Test object and array types get parse_json
        json_obj = {"type": ["object"]}
        json_arr = {"type": ["array"]}
        assert trans_mapper(json_obj) == 'parse_json'
        assert trans_mapper(json_arr) == 'parse_json'

        # Test SUPER types don't get parse_json
        json_super = {"type": "super"}
        json_super_list = {"type": ["super"]}
        json_super_null = {"type": ["super", "null"]}
        json_obj_super = {"type": ["object"], "format": "super"}
        assert trans_mapper(json_super) == ''
        assert trans_mapper(json_super_list) == ''
        assert trans_mapper(json_super_null) == ''
        assert trans_mapper(json_obj_super) == ''

        # Test other types don't get transformation
        json_str = {"type": ["string"]}
        json_int = {"type": ["integer"]}
        assert trans_mapper(json_str) == ''
        assert trans_mapper(json_int) == ''


    def test_safe_column_name(self):
        """Test safe column name formatting"""
        safe_name = target_redshift.db_sync.safe_column_name

        # Test basic column name
        assert safe_name('my_column') == '"MY_COLUMN"'

        # Test with special characters
        assert safe_name('column-with-dash') == '"COLUMN-WITH-DASH"'

        # Test with spaces
        assert safe_name('column with spaces') == '"COLUMN WITH SPACES"'

        # Test lowercase conversion
        assert safe_name('MixedCase') == '"MIXEDCASE"'


    def test_column_clause(self):
        """Test column clause generation"""
        clause = target_redshift.db_sync.column_clause

        # Test string column
        assert clause('name', {"type": ["string"]}) == '"NAME" character varying(10000)'

        # Test integer column
        assert clause('id', {"type": ["integer"]}) == '"ID" numeric'

        # Test SUPER column
        assert clause('metadata', {"type": "super"}) == '"METADATA" super'

        # Test timestamp column
        assert clause('created_at', {"type": ["string"], "format": "date-time"}) == '"CREATED_AT" timestamp without time zone'


    def test_primary_column_names(self):
        """Test primary column names extraction"""
        get_primary = target_redshift.db_sync.primary_column_names

        # Test single primary key
        schema_msg = {"key_properties": ["id"]}
        assert get_primary(schema_msg) == ['"ID"']

        # Test composite primary key
        schema_msg = {"key_properties": ["user_id", "product_id"]}
        assert get_primary(schema_msg) == ['"USER_ID"', '"PRODUCT_ID"']

        # Test no primary key
        schema_msg = {"key_properties": []}
        assert get_primary(schema_msg) == []


    def test_flatten_key(self):
        """Test flatten key generation"""
        flatten_key = target_redshift.db_sync.flatten_key

        # Test simple key
        assert flatten_key('column', [], '__') == 'column'

        # Test nested key
        assert flatten_key('address', ['user'], '__') == 'user__address'

        # Test multiple levels
        assert flatten_key('zip', ['user', 'address'], '__') == 'user__address__zip'

        # Test with long names that need truncation (>= 127 chars)
        long_parent = ['very_long_column_name_that_exceeds_the_maximum_length_allowed_by_redshift_for_column_identifiers']
        result = flatten_key('another_very_long_column_name_for_testing', long_parent, '__')
        # Should be shortened to stay under 127 chars
        assert len(result) < 127


    def test_should_json_dump_value(self):
        """Test _should_json_dump_value helper function"""
        should_dump = target_redshift.db_sync._should_json_dump_value

        # Test dict values should be dumped
        assert should_dump('key', {'nested': 'value'}, None) is True

        # Test list values should be dumped
        assert should_dump('key', ['item1', 'item2'], None) is True

        # Test string values should not be dumped
        assert should_dump('key', 'simple_string', None) is False

        # Test integer values should not be dumped
        assert should_dump('key', 123, None) is False

        # Test with flatten_schema that marks field as object/array type
        flatten_schema = {
            'json_field': {'type': ['null', 'object', 'array']}
        }
        assert should_dump('json_field', 'some_value', flatten_schema) is True

        # Test with flatten_schema but key not matching
        assert should_dump('other_field', 'some_value', flatten_schema) is False


    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_table') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_schema-my_table') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_catalog-my_schema-my_table') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_table', separator='.') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_schema.my_table', separator='.') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}


    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = target_redshift.db_sync.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        assert flatten_schema(schema_with_no_properties) == {}

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}
        # NO FLATTENNING - Schema with simple properties should be a plain dictionary
        assert flatten_schema(not_nested_schema) == not_nested_schema['properties']

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        assert flatten_schema(nested_schema_with_no_properties) == nested_schema_with_no_properties['properties']

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        assert flatten_schema(nested_schema_with_properties) == nested_schema_with_properties['properties']

        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        assert flatten_schema(nested_schema_with_properties, max_level=0) == nested_schema_with_properties['properties']

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=1) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3': {
                    'type': ['null', 'object'],
                    "properties": {
                        "multi_nested_prop1": {"type": ["null", "string"]},
                        "multi_nested_prop2": {"type": ["null", "string"]}
                    }
                }
        }

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=10) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
            }


    def test_flatten_schema_edge_cases(self):
        """Test flatten_schema edge cases and error conditions"""
        flatten_schema = target_redshift.db_sync.flatten_schema

        # Test schema with anyOf/oneOf pattern (no direct 'type' key)
        schema_with_anyof = {
            "type": "object",
            "properties": {
                "field1": {
                    "anyOf": [
                        {"type": "string"}
                    ]
                }
            }
        }
        result = flatten_schema(schema_with_anyof)
        assert 'field1' in result
        assert result['field1']['type'] == ['null', 'string']

        # Test schema with anyOf pattern for array
        schema_with_array_anyof = {
            "type": "object",
            "properties": {
                "field2": {
                    "anyOf": [
                        {"type": "array"}
                    ]
                }
            }
        }
        result = flatten_schema(schema_with_array_anyof)
        assert 'field2' in result
        assert result['field2']['type'] == ['null', 'array']

        # Test schema with anyOf pattern for object
        schema_with_object_anyof = {
            "type": "object",
            "properties": {
                "field3": {
                    "anyOf": [
                        {"type": "object"}
                    ]
                }
            }
        }
        result = flatten_schema(schema_with_object_anyof)
        assert 'field3' in result
        assert result['field3']['type'] == ['null', 'object']

        # Test duplicate column names raises ValueError
        # This would happen if flattening creates the same column name twice
        schema_with_duplicate_potential = {
            "type": "object",
            "properties": {
                "a__b": {"type": ["string"]},
                "a": {
                    "type": ["object"],
                    "properties": {
                        "b": {"type": ["string"]}
                    }
                }
            }
        }
        try:
            flatten_schema(schema_with_duplicate_potential, max_level=1)
            assert False, "Should have raised ValueError for duplicate column names"
        except ValueError as e:
            assert 'Duplicate column name' in str(e)


    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = target_redshift.db_sync.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        assert flatten_record(empty_record) == {}

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENNING - Record with simple properties should be a plain dictionary
        assert flatten_record(not_nested_record) == not_nested_record

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENNING - No flattening (default)
        assert \
            flatten_record(nested_record) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # NO FLATTENNING
        #   max_level: 0 : No flattening (default)
        assert \
            flatten_record(nested_record, max_level=0) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # SEMI FLATTENNING
        #   max_level: 1 : Semi-flattening (default)
        assert \
            flatten_record(nested_record, max_level=1) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}'
            }

        # FLATTENNING
        assert \
            flatten_record(nested_record, max_level=10) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
            }

    def test_flatten_record_with_flatten_schema(self):
        flatten_record = target_redshift.db_sync.flatten_record

        flatten_schema = {
            "id": {
                "type": [
                    "object",
                    "array",
                    "null"
                ]
            }
        }

        test_cases = [
            (
                True,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": "1",
                    "data": "xyz"
                }
            ),
            (
                False,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": 1,
                    "data": "xyz"
                }
            )
        ]

        for idx, (should_use_flatten_schema, record, expected_output) in enumerate(test_cases):
            output = flatten_record(record, flatten_schema if should_use_flatten_schema else None)
            assert output == expected_output
