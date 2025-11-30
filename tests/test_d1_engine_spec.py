import unittest
import warnings
from datetime import datetime
from unittest.mock import Mock

# Ignore the warning from external libraries
warnings.filterwarnings("ignore", category=DeprecationWarning, module="flask_babel")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="flask_sqlalchemy")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="flask_appbuilder")

from superset_engine_d1.d1_engine_spec import D1EngineSpec
from superset.errors import SupersetErrorType


class DummyD1MetaData:
    TABLES = ["test_table", "another_table", "_cf_configs"]
    VIEWS = ["test_view", "_cf_data_view"]

    COLUMNS = {
        "test_table": [
            {"name": "id", "type": "INTEGER", "nullable": False, "default": None, "autoincrement": True},
            {"name": "name", "type": "TEXT", "nullable": False, "default": None, "autoincrement": False},
            {"name": "value", "type": "INTEGER", "nullable": False, "default": None, "autoincrement": False},
            {"name": "active", "type": "BOOLEAN", "nullable": True, "default": 1, "autoincrement": False},
        ],
        "another_table": [
            {"name": "id", "type": "INTEGER", "nullable": False, "default": None, "autoincrement": True},
            {"name": "description", "type": "TEXT", "nullable": True, "default": None, "autoincrement": False},
        ],
    }

    PK = {
        "test_table": {"constrained_columns": ["id"]},
        "another_table": {"constrained_columns": ["id"]},
    }

    FKS = {
        "test_table": [],
        "another_table": [],
    }

    @classmethod
    def make_inspector(cls):
        inspector = Mock()
        inspector.get_table_names.return_value = cls.TABLES
        inspector.get_view_names.return_value = cls.VIEWS
        inspector.get_columns.side_effect = lambda table: cls.COLUMNS[table]
        inspector.get_pk_constraint.side_effect = lambda table, schema=None: cls.PK[table]
        inspector.get_foreign_keys.side_effect = lambda table, schema=None: cls.FKS[table]
        return inspector

class D1EngineSpecTestSuite(unittest.TestCase):
    
    def setUp(self):
        self.inspector = DummyD1MetaData.make_inspector()

    def test_convert_dttm_none(self):
        self.assertIsNone(D1EngineSpec.convert_dttm("TEXT", None))

    def test_convert_dttm_datetime_formats_correctly(self):
        date = datetime(2025, 1, 2, 3, 4, 5)

        self.assertEqual(D1EngineSpec.convert_dttm("DATETIME", date), "'2025-01-02 03:04:05'")
        self.assertEqual(D1EngineSpec.convert_dttm("TEXT", date), "'2025-01-02 03:04:05'")
        self.assertEqual(D1EngineSpec.convert_dttm("TIMESTAMP", date), "'2025-01-02 03:04:05'")

    def test_convert_dttm_unsupported_type(self):
        date = datetime(2025, 1, 2, 3, 4, 5)
        self.assertIsNone(D1EngineSpec.convert_dttm("INTEGER", date))

    def test_get_table_names_filtered(self):
        tables = D1EngineSpec.get_table_names(None, self.inspector, schema=None)
        self.assertEqual(tables, {"test_table", "another_table"})
    
    def test_get_view_names_filtered(self):
        views = D1EngineSpec.get_view_names(None, self.inspector, schema=None)
        self.assertEqual(views, {"test_view"})

    def test_get_columns_correct_structure(self):
        mock_table = Mock()
        mock_table.table = "test_table"
        cols = D1EngineSpec.get_columns(self.inspector, mock_table)

        self.assertIn("column_name", cols[0])
        self.assertNotIn("name", cols[0])
        self.assertEqual(cols[0]["column_name"], "id")
        self.assertEqual(cols[1]["column_name"], "name")

    def test_get_function_names(self):
        funcs = D1EngineSpec.get_function_names(None)

        self.assertIn("abs", funcs)
        self.assertIn("json_array_length", funcs)
        self.assertNotIn("not_a_valid_function", funcs)
        self.assertEqual(funcs, sorted(funcs))

    def test_get_virtual_table_context(self):
        mock_table = Mock()
        mock_table.table = "virtual_table"

        context = D1EngineSpec.get_virtual_table_context(mock_table, None)
        self.assertIs(context, mock_table)

    def test_custom_error_regex(self):
        error_message = "Error: no such column: non_existent_col"        
        regex = list(D1EngineSpec.custom_errors.keys())[0]
        match = regex.search(error_message)

        self.assertIsNotNone(match)
        self.assertEqual(match.group("column_name"), "non_existent_col")
        error_type = D1EngineSpec.custom_errors[regex][1]
        self.assertEqual(error_type, SupersetErrorType.COLUMN_DOES_NOT_EXIST_ERROR)

if __name__ == "__main__":
    unittest.main()
