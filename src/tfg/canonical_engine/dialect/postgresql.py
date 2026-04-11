from .base import BaseDialect

class PostgreSQLDialect(BaseDialect):

    name = "postgresql"

    def round_numeric(self, col, precision, scale):
        # Cast explícito a NUMERIC para evitar artefactos de DOUBLE PRECISION
        return f"ROUND({col}::NUMERIC({scale},{precision}), {precision})"

    def cast_integer(self, col):
        return f"{col}::INTEGER"

    def ensure_encoding(self, col, encoding):
        # PostgreSQL gestiona el encoding a nivel de base de datos.
        # No se necesita conversión explícita si la BD está en UTF-8.
        return col

    def trim(self, expr):
        return f"TRIM({expr})"

    def lowercase(self, expr):
        return f"LOWER({expr})"

    def normalize_unicode(self, expr, form):
        # NORMALIZE() disponible desde PostgreSQL 13
        supported = {"NFC", "NFD", "NFKC", "NFKD"}
        if form not in supported:
            raise ValueError(f"Forma Unicode no soportada: {form}")
        return f"NORMALIZE({expr}, {form})"

    def ascii_fold(self, expr):
        # PostgreSQL no tiene ascii_fold nativo sin extensión unaccent
        # Se registra la extensión si está disponible
        return f"unaccent({expr})"  # Requiere: CREATE EXTENSION unaccent

    def collapse_spaces(self, expr):
        return f"REGEXP_REPLACE({expr}, '\\s+', ' ', 'g')"

    def remove_punct(self, expr):
        return f"REGEXP_REPLACE({expr}, '[^\\w\\s]', '', 'g')"

    def to_utc(self, expr):
        return f"{expr} AT TIME ZONE 'UTC'"

    def truncate_timestamp(self, expr, precision):
        pg_precision = {
            "microsecond": "microseconds",
            "second":      "second",
            "minute":      "minute",
            "hour":        "hour",
            "day":         "day",
        }
        return f"DATE_TRUNC('{pg_precision[precision]}', {expr})"

    def coalesce(self, expr, default):
        return f"COALESCE({expr}, {default})"

    def null_replacement(self, canonical_type):
        from ..types.numeric  import NumericCanonical
        from ..types.text     import TextCanonical
        from ..types.temporal import TimestampCanonical
        if isinstance(canonical_type, NumericCanonical):  return "0"
        if isinstance(canonical_type, TextCanonical):     return "''"
        if isinstance(canonical_type, TimestampCanonical):
            return "'1970-01-01 00:00:00+00'"
        return "NULL"