"""Backtick-quote Spark / Unity Catalog identifiers (hyphens, reserved words)."""


def quote_sql_ident(name: str) -> str:
    """Return a backtick-quoted SQL identifier."""
    if not name:
        msg = "SQL identifier must be non-empty"
        raise ValueError(msg)
    return "`" + name.replace("`", "``") + "`"


def catalog_schema_sql(catalog: str, schema: str) -> str:
    """Two-part qualified name (e.g. ``CREATE SCHEMA``, three-part table parents)."""
    return f"{quote_sql_ident(catalog)}.{quote_sql_ident(schema)}"


def table_fqn(catalog: str, schema: str, table: str) -> str:
    """Three-part table name for ``spark.table`` / ``saveAsTable``."""
    return f"{quote_sql_ident(catalog)}.{quote_sql_ident(schema)}.{quote_sql_ident(table)}"
