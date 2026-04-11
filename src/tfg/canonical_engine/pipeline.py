from sqlalchemy import create_engine, text
from .introspection.inspector  import SchemaInspector
from .dialect.base             import UnsupportedTransformation
from .engine                   import PythonFallback
from dataclasses               import dataclass
from typing                    import Dict, List

@dataclass
class CanonicalColumn:
    name:             str
    sql_expression:   str           # Expresión SQL remota (si es posible)
    python_fallback:  callable      # Función Python (si SQL no es posible)
    requires_download: bool         # True si se necesita descargar el dato
    information_loss: str

@dataclass
class CanonicalPlan:
    """
    Plan de canonización para una tabla.
    Documenta qué ocurre con cada columna y dónde se canoniza.
    """
    table_name:       str
    dialect_name:     str
    columns:          Dict[str, CanonicalColumn]
    view_sql:         str           # SQL de la vista canónica completa
    download_columns: List[str]     # Columnas que requieren fallback Python

    def report(self) -> str:
        lines = [
            f"Plan de canonización: {self.table_name} ({self.dialect_name})",
            f"{'─' * 60}",
        ]
        for col_name, col in self.columns.items():
            location = "Python (descarga)" if col.requires_download \
                       else "SQL (remoto)"
            lines.append(f"  {col_name:<20} → {location}")
            if col.information_loss:
                lines.append(f"  {'':20}   ⚠ {col.information_loss}")
        lines.append(f"{'─' * 60}")
        lines.append(
            f"Columnas remotas:  "
            f"{len(self.columns) - len(self.download_columns)}"
        )
        lines.append(
            f"Columnas descarga: {len(self.download_columns)}"
        )
        return "\n".join(lines)


class CanonicalPipeline:

    def __init__(self, connection_uri: str, table_name: str):
        self.inspector  = SchemaInspector(connection_uri)
        self.dialect    = self.inspector.dialect
        self.table      = table_name
        self.engine     = create_engine(connection_uri)

    def build_plan(self) -> CanonicalPlan:
        """
        Construye el plan de canonización inspeccionando el esquema
        remoto y compilando las expresiones SQL para cada columna.
        No ejecuta ninguna transformación todavía.
        """
        canonical_types   = self.inspector.inspect_table(self.table)
        columns           = {}
        download_columns  = []

        for col_name, canonical_type in canonical_types.items():
            try:
                sql_expr = canonical_type.to_sql(self.dialect)
                columns[col_name] = CanonicalColumn(
                    name              = col_name,
                    sql_expression    = f"{sql_expr} AS {col_name}",
                    python_fallback   = None,
                    requires_download = False,
                    information_loss  = canonical_type.information_loss,
                )

            except UnsupportedTransformation as e:
                # El motor no soporta esta transformación de forma nativa.
                # Se marca para fallback Python y se documenta.
                fallback_fn = self._resolve_fallback(e.transformation)
                columns[col_name] = CanonicalColumn(
                    name              = col_name,
                    sql_expression    = f"{col_name}",  # Sin transformar
                    python_fallback   = fallback_fn,
                    requires_download = True,
                    information_loss  = canonical_type.information_loss,
                )
                download_columns.append(col_name)

        view_sql = self._build_view_sql(columns)

        return CanonicalPlan(
            table_name        = self.table,
            dialect_name      = self.dialect.name,
            columns           = columns,
            view_sql          = view_sql,
            download_columns  = download_columns,
        )

    def _resolve_fallback(self, transformation: str) -> callable:
        mapping = {
            "NORMALIZE(NFC)":  lambda v: PythonFallback.normalize_unicode(v, "NFC"),
            "NORMALIZE(NFKC)": lambda v: PythonFallback.normalize_unicode(v, "NFKC"),
            "ascii_fold":      PythonFallback.ascii_fold,
            "collapse_spaces": PythonFallback.collapse_spaces,
        }
        return mapping.get(transformation, lambda v: v)

    def _build_view_sql(self, columns: dict) -> str:
        expressions = [col.sql_expression for col in columns.values()]
        return (
            f"SELECT\n"
            f"    {','.join(chr(10)+'    ' for _ in [''])}"
            f"{(chr(10)+',    ').join(expressions)}\n"
            f"FROM {self.table}"
        )

    def apply_plan(self, plan: CanonicalPlan) -> None:
        """
        Materializa el plan creando una vista en el motor remoto
        para las columnas que pueden canonizarse en SQL,
        y aplicando los fallbacks Python al resultado para
        las columnas que requieren descarga.
        """
        view_name = f"{self.table}_canonical"

        with self.engine.connect() as conn:
            conn.execute(
                text(f"DROP VIEW IF EXISTS {view_name}")
            )
            conn.execute(
                text(f"CREATE VIEW {view_name} AS {plan.view_sql}")
            )
            conn.commit()

        print(f"Vista canónica creada: {view_name}")
        if plan.download_columns:
            print(
                f"⚠ Las siguientes columnas requieren fallback Python "
                f"(se descargarán los valores): {plan.download_columns}"
            )