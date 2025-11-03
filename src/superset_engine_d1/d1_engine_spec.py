# superset/db_engine_specs/d1.py
from superset.db_engine_specs.base import BaseEngineSpec
from sqlalchemy.engine.reflection import Inspector
import sqlalchemy_d1
from typing import Optional, List, Dict


class D1EngineSpec(BaseEngineSpec):
    """Superset EngineSpec for Cloudflare D1"""

    engine = "d1"
    sqlalchemy_dialect = "d1"
    engine_name = "Cloudflare D1"
    allows_subquery = True
    max_column_name_length = 128
    time_grain_expressions = {
        "second": "strftime('%Y-%m-%d %H:%M:%S', {col})",
        "minute": "strftime('%Y-%m-%d %H:%M:00', {col})",
        "hour": "strftime('%Y-%m-%d %H:00:00', {col})",
        "day": "strftime('%Y-%m-%d 00:00:00', {col})",
        "week": "strftime('%Y-%m-%d 00:00:00', {col})",  # SQLite/D1 does not have week trunc
        "month": "strftime('%Y-%m-01 00:00:00', {col})",
        "quarter": "strftime('%Y-%m-01 00:00:00', {col})",
        "year": "strftime('%Y-01-01 00:00:00', {col})",
    }
    allows_virtual_table_explore = True

    @classmethod
    def get_table_names(
        cls, inspector: Inspector, schema: Optional[str] = None
    ) -> List[str]:
        """
        Return list of table names using SQLAlchemy Inspector.
        """
        try:
            return inspector.get_table_names(schema=schema)
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch table names: {e}"
            )

    @classmethod
    def get_columns(
        cls,
        inspector: Inspector,
        table_name: str,
        schema: Optional[str] = None,
    ) -> List[Dict]:
        """
        Return list of column definitions as dicts using SQLAlchemy Inspector.
        Each dict should have keys: name, type, nullable, default.
        """
        try:
            cols = inspector.get_columns(table_name, schema=schema)
            return cols
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch columns for {table_name}: {e}"
            )

    @classmethod
    def get_pk_constraint(
        cls,
        inspector: Inspector,
        table_name: str,
        schema: Optional[str] = None,
    ) -> Dict:
        """
        Return the primary key constraint as a dict.
        Example: {"constrained_columns": ["id"], "name": "pk_table_id"}
        """
        try:
            return inspector.get_pk_constraint(table_name, schema=schema)
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch PK for {table_name}: {e}"
            )

    @classmethod
    def get_foreign_keys(
        cls,
        inspector: Inspector,
        table_name: str,
        schema: Optional[str] = None,
    ) -> List[Dict]:
        """
        Return foreign key constraints.
        Each dict: {"constrained_columns": [...], "referred_schema": ..., "referred_table": ..., "referred_columns": [...]}
        """
        try:
            return inspector.get_foreign_keys(table_name, schema=schema)
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch FKs for {table_name}: {e}"
            )
