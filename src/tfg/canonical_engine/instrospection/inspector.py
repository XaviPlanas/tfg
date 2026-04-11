
from sqlalchemy import create_engine, inspect, text
from .type_mapper import TypeMapper

class SchemaInspector:
    """
    Inspecciona el esquema de una tabla remota y devuelve
    los tipos canónicos inferidos para cada columna.
    No descarga datos: usa solo los metadatos del motor.
    """

    def __init__(self, connection_uri: str):
        self.engine  = create_engine(connection_uri)
        self.mapper  = TypeMapper()
        self.dialect = self._detect_dialect()

    def _detect_dialect(self):
        from ..dialect.registry import DialectRegistry
        dialect_name = self.engine.dialect.name  # 'mysql', 'postgresql', etc.
        return DialectRegistry.get(dialect_name)

    def inspect_table(self, table_name: str) -> dict:
        """
        Devuelve un diccionario {column_name: CanonicalType}
        con los tipos canónicos inferidos para cada columna.
        """
        inspector = inspect(self.engine)
        columns   = inspector.get_columns(table_name)

        canonical_types = {}
        for col in columns:
            col_name    = col["name"]
            sql_type    = col["type"]
            nullable    = col["nullable"]
            canonical   = self.mapper.map(
                col_name  = col_name,
                sql_type  = sql_type,
                nullable  = nullable,
                dialect   = self.dialect,
            )
            canonical_types[col_name] = canonical

        return canonical_types

    def get_sample(self, table_name: str, n: int = 5) -> list:
        """
        Descarga una muestra mínima de datos para validar
        los tipos inferidos. Es la única descarga de datos
        que realiza el motor de canonización.
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT * FROM {table_name} LIMIT {n}")
            )
            return [dict(row) for row in result]