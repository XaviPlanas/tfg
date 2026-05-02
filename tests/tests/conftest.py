"""
tests/conftest.py

Fixtures y utilidades compartidas por todos los módulos de test.

Convenciones del proyecto:
  - Dialectos: mysql_d / pg_d  (instancias de MySQLDialect / PostgreSQLDialect)
  - Tipos:     make_text() / make_numeric() / ...  (factories con defaults)
  - DiffRows:  diff_update() / diff_insert() / diff_delete()
  - Planes:    make_plan()  (constructor de CanonicalPlan con columnas arbitrarias)

Los tests de integración (requieren BD real) se marcan con:
    @unittest.skipUnless(DB_AVAILABLE, "requiere BD real")
y se agrupan en tests/integration/.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from canonical_engine.dialect.mysql      import MySQLDialect
from canonical_engine.dialect.postgresql import PostgreSQLDialect
from canonical_engine.types.text         import TextCanonical, TextTransformation
from canonical_engine.types.numeric      import NumericCanonical, IntegerCanonical
from canonical_engine.types.boolean      import BooleanCanonical
from canonical_engine.types.temporal     import TimestampCanonical, TemporalPrecision
from canonical_engine.plan               import CanonicalColumn, CanonicalPlan, Layer
from datadiff_classifier.models          import (
    DiffRow, DiffCategory, DiffAction, DiffEvent, SegmentStructure
)

# ── Dialectos compartidos ─────────────────────────────────────────

MYSQL = MySQLDialect()
PG    = PostgreSQLDialect()

# ── Factories de tipos canónicos ──────────────────────────────────

def make_text(col="name", transforms=None, nullable=True, encoding="utf8mb4"):
    return TextCanonical(
        column_name     = col,
        nullable        = nullable,
        transformations = transforms or [
            TextTransformation.TRIM,
            TextTransformation.LOWERCASE,
        ],
        encoding        = encoding,
    )

def make_text_nfc(col="name", nullable=True):
    return TextCanonical(
        column_name     = col,
        nullable        = nullable,
        transformations = [
            TextTransformation.TRIM,
            TextTransformation.LOWERCASE,
            TextTransformation.NFC,
        ],
    )

def make_numeric(col="fare", precision=3, scale=10, nullable=True):
    return NumericCanonical(
        column_name = col,
        nullable    = nullable,
        precision   = precision,
        scale       = scale,
    )

def make_integer(col="age", nullable=True):
    return IntegerCanonical(column_name=col, nullable=nullable)

def make_boolean(col="survived", nullable=True):
    return BooleanCanonical(column_name=col, nullable=nullable)

def make_timestamp(col="embarked_at", precision="second",
                   force_utc=True, nullable=True):
    return TimestampCanonical(
        column_name = col,
        nullable    = nullable,
        precision   = precision,
        force_utc   = force_utc,
    )

# ── Factories de DiffRow ─────────────────────────────────────────

def diff_update(key, row_a: dict, row_b: dict):
    return DiffRow(
        key      = key,
        row_a    = row_a,
        row_b    = row_b,
        source_a = "mysql://test/titanic",
        source_b = "postgresql://test/titanic",
        accion   = DiffAction.UPDATE,
    )

def diff_insert(key, row_b: dict):
    """Fila presente solo en B (INSERT desde perspectiva de A)."""
    return DiffRow(
        key      = key,
        row_a    = None,
        row_b    = row_b,
        source_a = "mysql://test/titanic",
        source_b = "postgresql://test/titanic",
        accion   = DiffAction.INSERT,
    )

def diff_delete(key, row_a: dict):
    """Fila presente solo en A (DELETE desde perspectiva de A)."""
    return DiffRow(
        key      = key,
        row_a    = row_a,
        row_b    = None,
        source_a = "mysql://test/titanic",
        source_b = "postgresql://test/titanic",
        accion   = DiffAction.DELETE,
    )

# ── Factory de CanonicalPlan ─────────────────────────────────────

def make_plan(columns: dict, table="titanic", dialect="mysql") -> CanonicalPlan:
    """
    columns: {col_name: (layer, sql_expr, post_fn, pending)}
    Ejemplo:
        make_plan({
            "name": (Layer.SPLIT, "LOWER(TRIM(name)) AS name", fn_nfc, ["unicode_nfc"]),
            "fare": (Layer.PRE,   "ROUND(fare, 3) AS fare",    None,   []),
        })
    """
    cols = {}
    for col_name, (layer, sql_expr, post_fn, pending) in columns.items():
        cols[col_name] = CanonicalColumn(
            name               = col_name,
            layer              = layer,
            sql_expression     = sql_expr,
            post_fn            = post_fn,
            pending_transforms = pending,
            information_loss   = "",
        )
    return CanonicalPlan(
        table_name   = table,
        dialect_name = dialect,
        columns      = cols,
    )

# ── Disponibilidad de BD para tests de integración ───────────────

DB_AVAILABLE = False   # sin BD real en CI — los tests de integración se saltan
