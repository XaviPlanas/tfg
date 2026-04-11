from sqlalchemy import types as sa_types
from ..types.numeric  import NumericCanonical, IntegerCanonical
from ..types.text     import TextCanonical
from ..types.temporal import TimestampCanonical
from ..types.boolean  import BooleanCanonical

class TypeMapper:
    """
    Mapea tipos SQLAlchemy (inferidos del motor) a tipos canónicos.
    Este mapeo es la capa de traducción entre la representación
    física del dato y su semántica canónica.
    """

    def map(self, col_name, sql_type, nullable, dialect) -> "CanonicalType":

        # ── Numéricos con coma flotante ──────────────────────────
        if isinstance(sql_type, (sa_types.Float, sa_types.REAL)):
            return NumericCanonical(
                column_name = col_name,
                nullable    = nullable,
                precision   = 3,   # Default conservador
                scale       = 10,
            )

        if isinstance(sql_type, (sa_types.Numeric, sa_types.DECIMAL)):
            return NumericCanonical(
                column_name = col_name,
                nullable    = nullable,
                precision   = sql_type.scale or 3,
                scale       = sql_type.precision or 10,
            )

        # ── Enteros y booleanos ───────────────────────────────────
        if isinstance(sql_type, sa_types.Boolean):
            return BooleanCanonical(
                column_name = col_name,
                nullable    = nullable,
            )

        if isinstance(sql_type, (sa_types.Integer, sa_types.SmallInteger,
                                  sa_types.BigInteger)):
            return IntegerCanonical(
                column_name = col_name,
                nullable    = nullable,
            )

        # ── Texto ─────────────────────────────────────────────────
        if isinstance(sql_type, (sa_types.String, sa_types.Text,
                                  sa_types.Unicode, sa_types.UnicodeText)):
            return TextCanonical(
                column_name = col_name,
                nullable    = nullable,
            )

        # ── Temporal ──────────────────────────────────────────────
        if isinstance(sql_type, (sa_types.DateTime, sa_types.TIMESTAMP)):
            return TimestampCanonical(
                column_name = col_name,
                nullable    = nullable,
                force_utc   = True,
                precision   = "second",
            )

        if isinstance(sql_type, sa_types.Date):
            return TimestampCanonical(
                column_name = col_name,
                nullable    = nullable,
                force_utc   = False,
                precision   = "day",
            )

        # ── Fallback: tratar como texto ───────────────────────────
        return TextCanonical(
            column_name = col_name,
            nullable    = nullable,
        )