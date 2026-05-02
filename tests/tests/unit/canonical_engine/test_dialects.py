"""
tests/unit/canonical_engine/test_dialects.py

Tests de generación SQL para MySQLDialect y PostgreSQLDialect.

Estrategia:
  - Verificar la forma exacta del SQL generado (no ejecutarlo).
  - Probar que UnsupportedTransformation se lanza donde corresponde.
  - Probar que la forma SQL varía correctamente entre dialectos.
  - Cubrir todos los métodos de BaseDialect.

Por qué testear el SQL generado y no solo "que no lanza":
  Un cambio accidental en el template SQL (e.g. DECIMAL → NUMERIC en MySQL)
  pasaría desapercibido sin aserciones sobre la cadena resultante.
  En producción, ese cambio alteraría la semántica de la canonización.
"""

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from canonical_engine.dialect.mysql      import MySQLDialect
from canonical_engine.dialect.postgresql import PostgreSQLDialect
from canonical_engine.dialect.base       import UnsupportedTransformation
from canonical_engine.types.numeric      import NumericCanonical
from canonical_engine.types.text         import TextCanonical
from canonical_engine.types.temporal     import TimestampCanonical


class TestMySQLDialect(unittest.TestCase):
    """Tests de MySQLDialect — generación SQL y UnsupportedTransformation."""

    def setUp(self):
        self.d = MySQLDialect()

    # ── Numéricos ────────────────────────────────────────────────

    def test_round_numeric_usa_decimal(self):
        sql = self.d.round_numeric("fare", precision=3, scale=10)
        self.assertIn("DECIMAL", sql)
        self.assertIn("fare", sql)
        self.assertIn("10", sql)
        self.assertIn("3", sql)

    def test_round_numeric_formato(self):
        sql = self.d.round_numeric("fare", precision=3, scale=10)
        # Forma esperada: ROUND(CAST(fare AS DECIMAL(10,3)), 3)
        self.assertTrue(sql.startswith("ROUND("))
        self.assertIn("CAST(", sql)

    def test_cast_integer_usa_signed(self):
        sql = self.d.cast_integer("age")
        self.assertEqual(sql, "CAST(age AS SIGNED)")

    # ── Texto ────────────────────────────────────────────────────

    def test_ensure_encoding_convierte_usando(self):
        sql = self.d.ensure_encoding("name", "utf8mb4")
        self.assertEqual(sql, "CONVERT(name USING utf8mb4)")

    def test_trim(self):
        self.assertEqual(self.d.trim("name"), "TRIM(name)")

    def test_lowercase(self):
        self.assertEqual(self.d.lowercase("name"), "LOWER(name)")

    def test_normalize_unicode_lanza_unsupported(self):
        """MySQL no soporta NORMALIZE() → debe lanzar UnsupportedTransformation."""
        with self.assertRaises(UnsupportedTransformation) as ctx:
            self.d.normalize_unicode("name", "NFC")
        self.assertIn("mysql", ctx.exception.dialect.lower())
        self.assertIn("NFC", ctx.exception.transformation)

    def test_normalize_unicode_nfkc_tambien_lanza(self):
        with self.assertRaises(UnsupportedTransformation):
            self.d.normalize_unicode("name", "NFKC")

    def test_ascii_fold_lanza_unsupported(self):
        with self.assertRaises(UnsupportedTransformation):
            self.d.ascii_fold("name")

    def test_collapse_spaces_usa_regexp_replace(self):
        sql = self.d.collapse_spaces("name")
        self.assertIn("REGEXP_REPLACE", sql)
        self.assertIn("name", sql)

    def test_remove_punct(self):
        sql = self.d.remove_punct("name")
        self.assertIn("REGEXP_REPLACE", sql)
        self.assertIn("name", sql)

    # ── Temporal ─────────────────────────────────────────────────

    def test_to_utc_usa_convert_tz(self):
        sql = self.d.to_utc("created_at")
        self.assertIn("CONVERT_TZ", sql)
        self.assertIn("created_at", sql)
        self.assertIn("+00:00", sql)

    def test_truncate_timestamp_second(self):
        sql = self.d.truncate_timestamp("created_at", "second")
        self.assertIn("DATE_FORMAT", sql)
        self.assertIn("%Y-%m-%d %H:%i:%s", sql)

    def test_truncate_timestamp_day(self):
        sql = self.d.truncate_timestamp("created_at", "day")
        self.assertIn("00:00:00", sql)

    def test_truncate_timestamp_microsecond_passthrough(self):
        # MySQL soporta microsegundos nativamente: no transforma
        sql = self.d.truncate_timestamp("ts", "microsecond")
        self.assertEqual(sql, "ts")

    # ── Nulos ────────────────────────────────────────────────────

    def test_coalesce(self):
        self.assertEqual(self.d.coalesce("col", "''"), "COALESCE(col, '')")

    def test_null_replacement_numeric(self):
        t = NumericCanonical(column_name="x")
        self.assertEqual(self.d.null_replacement(t), "0")

    def test_null_replacement_text(self):
        t = TextCanonical(column_name="x")
        self.assertEqual(self.d.null_replacement(t), "''")

    def test_null_replacement_timestamp(self):
        t = TimestampCanonical(column_name="x")
        r = self.d.null_replacement(t)
        self.assertIn("1970-01-01", r)
        # MySQL no usa +00 en el sentinel
        self.assertNotIn("+00", r)

    # ── Booleanos ────────────────────────────────────────────────

    def test_normalize_boolean_cubre_true_textual(self):
        sql = self.d.normalize_boolean("survived")
        self.assertIn("true", sql.lower())
        self.assertIn("survived", sql)
        self.assertIn("CASE", sql)

    def test_normalize_boolean_cubre_false_textual(self):
        sql = self.d.normalize_boolean("survived")
        self.assertIn("false", sql.lower())


class TestPostgreSQLDialect(unittest.TestCase):
    """Tests de PostgreSQLDialect."""

    def setUp(self):
        self.d = PostgreSQLDialect()

    # ── Numéricos ────────────────────────────────────────────────

    def test_round_numeric_usa_cast_pg(self):
        sql = self.d.round_numeric("fare", precision=3, scale=10)
        self.assertIn("::NUMERIC", sql)
        self.assertIn("ROUND(", sql)

    def test_cast_integer_usa_cast_pg(self):
        sql = self.d.cast_integer("age")
        self.assertIn("::INTEGER", sql)

    # ── Texto ────────────────────────────────────────────────────

    def test_ensure_encoding_es_passthrough(self):
        # PG gestiona encoding a nivel de BD; no necesita conversión
        sql = self.d.ensure_encoding("name", "utf8mb4")
        self.assertEqual(sql, "name")

    def test_normalize_unicode_nfc(self):
        sql = self.d.normalize_unicode("name", "NFC")
        self.assertIn("NORMALIZE", sql)
        self.assertIn("NFC", sql)

    def test_normalize_unicode_nfkc(self):
        sql = self.d.normalize_unicode("name", "NFKC")
        self.assertIn("NFKC", sql)

    def test_normalize_unicode_forma_invalida_lanza(self):
        with self.assertRaises(ValueError):
            self.d.normalize_unicode("name", "XYZ")

    def test_ascii_fold_usa_unaccent(self):
        sql = self.d.ascii_fold("name")
        self.assertIn("unaccent", sql.lower())

    def test_collapse_spaces_usa_g_flag(self):
        sql = self.d.collapse_spaces("name")
        self.assertIn("'g'", sql)    # PG necesita flag 'g' en REGEXP_REPLACE

    # ── Temporal ─────────────────────────────────────────────────

    def test_to_utc_usa_at_time_zone(self):
        sql = self.d.to_utc("created_at")
        self.assertIn("AT TIME ZONE", sql)
        self.assertIn("UTC", sql)

    def test_truncate_timestamp_usa_date_trunc(self):
        sql = self.d.truncate_timestamp("created_at", "second")
        self.assertIn("DATE_TRUNC", sql)
        self.assertIn("second", sql)

    def test_null_replacement_timestamp_incluye_utc(self):
        t = TimestampCanonical(column_name="x")
        r = self.d.null_replacement(t)
        self.assertIn("+00", r)    # PG usa offset explícito

    # ── Booleanos ────────────────────────────────────────────────

    def test_normalize_boolean_usa_ilike(self):
        sql = self.d.normalize_boolean("survived")
        self.assertIn("ILIKE", sql)

    def test_normalize_boolean_usa_cast_integer(self):
        sql = self.d.normalize_boolean("survived")
        self.assertIn("::integer", sql.lower())


class TestDialectDivergencias(unittest.TestCase):
    """
    Tests de divergencia entre dialectos: misma semántica, SQL diferente.
    Estos casos son exactamente los falsos positivos que el sistema evita.
    """

    def setUp(self):
        self.mysql = MySQLDialect()
        self.pg    = PostgreSQLDialect()

    def test_round_numeric_sql_diferente(self):
        """DECIMAL (MySQL) vs ::NUMERIC (PG) — mismo resultado semántico."""
        sql_m = self.mysql.round_numeric("fare", 3, 10)
        sql_p = self.pg.round_numeric("fare", 3, 10)
        self.assertNotEqual(sql_m, sql_p)
        self.assertIn("DECIMAL", sql_m)
        self.assertIn("NUMERIC", sql_p)

    def test_encoding_solo_mysql(self):
        """CONVERT USING solo se emite en MySQL, no en PG."""
        sql_m = self.mysql.ensure_encoding("name", "utf8mb4")
        sql_p = self.pg.ensure_encoding("name", "utf8mb4")
        self.assertIn("CONVERT", sql_m)
        self.assertEqual(sql_p, "name")

    def test_unicode_normalize_solo_pg(self):
        """NFC solo soportada nativamente en PG."""
        with self.assertRaises(UnsupportedTransformation):
            self.mysql.normalize_unicode("name", "NFC")
        sql_p = self.pg.normalize_unicode("name", "NFC")
        self.assertIn("NORMALIZE", sql_p)

    def test_utc_conversion_sql_diferente(self):
        sql_m = self.mysql.to_utc("ts")
        sql_p = self.pg.to_utc("ts")
        self.assertIn("CONVERT_TZ", sql_m)
        self.assertIn("AT TIME ZONE", sql_p)


if __name__ == "__main__":
    unittest.main(verbosity=2)
