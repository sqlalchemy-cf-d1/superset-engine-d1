# d1_engine_spec.py
from __future__ import annotations

import re
from datetime import datetime
from re import Pattern
from typing import Any, TYPE_CHECKING, Dict, List, Optional

from flask_babel import gettext as __
from sqlalchemy import types
from sqlalchemy.engine.reflection import Inspector

from superset.constants import TimeGrain
from superset.db_engine_specs.base import (
    BaseEngineSpec,
    ResultSetColumnType,
    Table,
)
from superset.errors import SupersetErrorType

if TYPE_CHECKING:
    from superset.models.core import Database


COLUMN_DOES_NOT_EXIST_REGEX = re.compile("no such column: (?P<column_name>.+)")


class D1EngineSpec(BaseEngineSpec):
    """Superset EngineSpec for Cloudflare D1 (SQLite-compatible)."""

    engine = "d1"
    sqlalchemy_dialect = "d1"
    engine_name = "Cloudflare D1"

    # D1 is SQLite-compatible but remote — disable tunneling and local assumptions
    disable_ssh_tunneling = True
    allows_subquery = True
    allows_virtual_table_explore = True
    supports_multivalues_insert = True
    max_column_name_length = 128

    # Same base time grains as SQLite
    _time_grain_expressions = {
        None: "{col}",
        TimeGrain.SECOND: "DATETIME(STRFTIME('%Y-%m-%dT%H:%M:%S', {col}))",
        TimeGrain.MINUTE: "DATETIME(STRFTIME('%Y-%m-%dT%H:%M:00', {col}))",
        TimeGrain.HOUR: "DATETIME(STRFTIME('%Y-%m-%dT%H:00:00', {col}))",
        TimeGrain.DAY: "DATETIME({col}, 'start of day')",
        TimeGrain.WEEK: "DATETIME({col}, 'start of day', -strftime('%w', {col}) || ' days')",
        TimeGrain.MONTH: "DATETIME({col}, 'start of month')",
        TimeGrain.QUARTER: (
            "DATETIME({col}, 'start of month', "
            "printf('-%d month', (strftime('%m', {col}) - 1) % 3))"
        ),
        TimeGrain.YEAR: "DATETIME({col}, 'start of year')",
    }

    custom_errors: dict[
        Pattern[str], tuple[str, SupersetErrorType, dict[str, Any]]
    ] = {
        COLUMN_DOES_NOT_EXIST_REGEX: (
            __("We can’t seem to resolve the column “%(column_name)s”"),
            SupersetErrorType.COLUMN_DOES_NOT_EXIST_ERROR,
            {},
        ),
    }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        """D1 follows SQLite’s unixepoch semantics."""
        return "datetime({col}, 'unixepoch')"

    @classmethod
    def convert_dttm(
        cls,
        target_type: str,
        dttm: datetime,
        db_extra: dict[str, Any] | None = None,
    ) -> str | None:
        """Convert Python datetime to SQL literal."""
        if dttm is None:
            return None
        sqla_type = cls.get_sqla_column_type(target_type)
        if isinstance(sqla_type, (types.String, types.DateTime)):
            return f"""'{dttm.isoformat(sep=" ", timespec="seconds")}'"""
        return None

    # ----------------------------------------------------------
    # Reflection helpers
    # ----------------------------------------------------------

    @classmethod
    def get_table_names(
        cls,
        database: Database,
        inspector: Inspector,
        schema: Optional[str] = None,
    ) -> set[str]:
        """
        Return visible table names. D1 forbids editing certain system tables,
        so filter out those prefixed with `_cf`.
        """
        try:
            tables = inspector.get_table_names(schema=schema)
            return {t for t in tables if not t.startswith("_cf")}
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch table names: {e}"
            )

    @classmethod
    def get_view_names(
        cls,
        database: Database,
        inspector: Inspector,
        schema: Optional[str] = None,
    ) -> set[str]:
        """
        Return visible view names, filtering out restricted system ones.
        """
        try:
            views = inspector.get_view_names(schema=schema)
            return {v for v in views if not v.startswith("_cf")}
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch view names: {e}"
            )

    @classmethod
    def get_columns(
        cls,
        inspector: Inspector,
        table: Table,
        options: dict[str, Any] | None = None,
    ) -> list[ResultSetColumnType]:
        """
        Return list of columns for a given table.
        """
        try:
            cols = [
                {
                    "column_name": col["name"],  # map here
                    "type": col["type"],
                    "nullable": col.get("nullable", True),
                    "default": col.get("default"),
                    "autoincrement": col.get("autoincrement", False),
                }
                for col in inspector.get_columns(table.table)
            ]
            return cols
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch columns for {table}: {e}"
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
        """
        try:
            return inspector.get_foreign_keys(table_name, schema=schema)
        except Exception as e:
            raise RuntimeError(
                f"D1EngineSpec: Failed to fetch FKs for {table_name}: {e}"
            )

    @classmethod
    def get_function_names(cls, database: Database) -> list[str]:
        """
        D1 mirrors SQLite’s built-in SQL function set, except for load_extension.
        """
        functions = [
            "abs",
            "acos",
            "asin",
            "atan",
            "avg",
            "ceil",
            "coalesce",
            "cos",
            "count",
            "date",
            "datetime",
            "exp",
            "floor",
            "hex",
            "ifnull",
            "iif",
            "instr",
            "json",
            "json_extract",
            "json_object",
            "json_array",
            "json_array_length",
            "json_valid",
            "length",
            "like",
            "log",
            "lower",
            "ltrim",
            "max",
            "min",
            "mod",
            "nullif",
            "pi",
            "pow",
            "power",
            "printf",
            "quote",
            "random",
            "replace",
            "round",
            "rtrim",
            "sign",
            "sin",
            "sqrt",
            "strftime",
            "substr",
            "sum",
            "tan",
            "time",
            "total_changes",
            "trim",
            "typeof",
            "unixepoch",
            "upper",
            "zeroblob",
        ]
        return sorted(functions)

    @classmethod
    def get_virtual_table_context(
        cls,
        virtual_table,
        database: Database,
        schema=None
    ) -> Table:
        """
        Return a Table object for virtual table exploration.       
        """
        return virtual_table