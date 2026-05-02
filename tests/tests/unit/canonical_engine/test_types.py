"""
tests/unit/canonical_engine/test_types.py

Tests de los tipos canónicos: TextCanonical, NumericCanonical,
IntegerCanonical, BooleanCanonical, TimestampCanonical.

Cada tipo se testa en tres dimensiones:
  1. to_sql(dialect)      → SQL generado para MySQL y PG
  2. to_python()()        → callable Python equivalente
  3. Paridad SQL/Python   → mismo resultado semántico en ambas rutas
  4. Gestión de nulos     → COALESCE en SQL / sentinel en Python
  5. validate()           → contrato de validación de muestras

La paridad SQL/Python (dimensión 3) es el test más valioso del sistema:
garantiza que la capa POST produce resultados equivalentes a la capa PRE,
lo cual es el invariante fundamental del split PRE/POST.
"""

import sys, os, unittest, unicodedata
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from tests.conftest import MYSQL, PG, make_text, make_text_nfc, make_numeric, \
                           make_integer, make_boolean, make_timestamp
from canonical_engine.dialect.base  import UnsupportedTransformation
from canonical_engine.types.text    import TextTransformation


# ─────────────────────────────────────────────────────────────────
# TextCanonical
# ─────────────────────────────────────────────────────────────────

class TestTextCanonicalSQL(unittest.TestCase):
    """SQL generado por TextCanonical."""

    def test_trim_lowercase_mysql(self):
        t   = make_text("name", [TextTransformation.TRIM, TextTransformation.LOWERCASE])
        sql = t.to_sql(MYSQL)
        self.assertIn("LOWER", sql)
        self.assertIn("TRIM", sql)
        self.assertIn("CONVERT", sql)   # encoding MySQL

    def test_trim_lowercase_pg(self):
        t   = make_text("name", [TextTransformation.TRIM, TextTransformation.LOWERCASE])
        sql = t.to_sql(PG)
        self.assertIn("LOWER", sql)
        self.assertIn("TRIM", sql)
        self.assertNotIn("CONVERT", sql)   # PG no necesita CONVERT

    def test_orden_aplicacion_anidado(self):
        """Las transformaciones se aplican de fuera hacia dentro en el SQL."""
        t   = make_text("name", [TextTransformation.TRIM, TextTransformation.LOWERCASE])
        sql = t.to_sql(PG)
        # LOWER debe envolver a TRIM
        self.assertLess(sql.index("LOWER"), sql.index("TRIM"))

    def test_nfc_mysql_lanza(self):
        t = make_text_nfc("name")
        with self.assertRaises(UnsupportedTransformation):
            t.to_sql(MYSQL)

    def test_nfc_pg_ok(self):
        t   = make_text_nfc("name")
        sql = t.to_sql(PG)
        self.assertIn("NORMALIZE", sql)
        self.assertIn("NFC", sql)

    def test_nullable_agrega_coalesce(self):
        t   = make_text("name", nullable=True)
        sql = t.to_sql(PG)
        self.assertIn("COALESCE", sql)

    def test_not_nullable_sin_coalesce(self):
        t   = make_text("name", nullable=False)
        sql = t.to_sql(PG)
        self.assertNotIn("COALESCE", sql)

    def test_collapse_spaces_mysql(self):
        t   = make_text("bio", [TextTransformation.COLLAPSE_SPACES])
        sql = t.to_sql(MYSQL)
        self.assertIn("REGEXP_REPLACE", sql)

    def test_collapse_spaces_pg(self):
        t   = make_text("bio", [TextTransformation.COLLAPSE_SPACES])
        sql = t.to_sql(PG)
        self.assertIn("REGEXP_REPLACE", sql)
        self.assertIn("'g'", sql)


class TestTextCanonicalPython(unittest.TestCase):
    """Callable to_python() de TextCanonical."""

    def test_trim(self):
        fn = make_text("n", [TextTransformation.TRIM]).to_python()
        self.assertEqual(fn("  hola  "), "hola")

    def test_lowercase(self):
        fn = make_text("n", [TextTransformation.LOWERCASE]).to_python()
        self.assertEqual(fn("JACK"), "jack")

    def test_trim_then_lowercase(self):
        fn = make_text("n", [TextTransformation.TRIM, TextTransformation.LOWERCASE]).to_python()
        self.assertEqual(fn("  ROSE  "), "rose")

    def test_nfc_normaliza(self):
        fn = make_text("n", [TextTransformation.NFC]).to_python()
        # "café" en NFD (a + combining accent) vs NFC
        nfd_cafe = unicodedata.normalize("NFD", "café")
        self.assertEqual(fn(nfd_cafe), "café")   # NFC canónico

    def test_nfkc_normaliza_ligaduras(self):
        fn = make_text("n", [TextTransformation.NFKC]).to_python()
        # ﬁ (ligatura fi) → fi
        self.assertEqual(fn("ﬁn"), "fin")

    def test_ascii_fold_elimina_acentos(self):
        fn = make_text("n", [TextTransformation.ASCII_FOLD]).to_python()
        self.assertEqual(fn("café"), "cafe")
        self.assertEqual(fn("niño"), "nino")
        self.assertEqual(fn("naïve"), "naive")

    def test_collapse_spaces(self):
        fn = make_text("n", [TextTransformation.COLLAPSE_SPACES]).to_python()
        self.assertEqual(fn("  hola   mundo  "), "hola mundo")

    def test_remove_punct(self):
        fn = make_text("n", [TextTransformation.REMOVE_PUNCT]).to_python()
        result = fn("hello, world!")
        self.assertNotIn(",", result)
        self.assertNotIn("!", result)

    def test_none_nullable_devuelve_vacio(self):
        fn = make_text("n", nullable=True).to_python()
        self.assertEqual(fn(None), "")

    def test_none_not_nullable_devuelve_none(self):
        fn = make_text("n", nullable=False).to_python()
        self.assertIsNone(fn(None))

    def test_pipeline_completo(self):
        fn = make_text_nfc("n").to_python()  # TRIM + LOWER + NFC
        nfd_cafe = unicodedata.normalize("NFD", "  CAFÉ  ")
        self.assertEqual(fn(nfd_cafe), "café")


class TestTextCanonicalSQLPartial(unittest.TestCase):
    """to_sql_partial(): split PRE/POST para dialectos con soporte desigual."""

    def test_trim_lower_ambos_soportan_sin_pending(self):
        t = make_text("n", [TextTransformation.TRIM, TextTransformation.LOWERCASE])
        sql, pending = t.to_sql_partial(MYSQL, PG)
        self.assertEqual(pending, [])
        self.assertIn("LOWER", sql)

    def test_nfc_mysql_no_soporta_pending_nfc(self):
        t = make_text_nfc("n")   # TRIM + LOWER + NFC
        sql, pending = t.to_sql_partial(MYSQL, PG)
        self.assertEqual(pending, [TextTransformation.NFC])
        # SQL tiene TRIM y LOWER pero no NORMALIZE
        self.assertIn("LOWER", sql)
        self.assertNotIn("NORMALIZE", sql)

    def test_nfc_con_peer_mysql_sql_incluye_trim_lower(self):
        """Cuando peer es MySQL, PG también detiene antes de NFC."""
        t = make_text_nfc("n")
        sql, pending = t.to_sql_partial(PG, MYSQL)
        self.assertEqual(pending, [TextTransformation.NFC])
        self.assertIn("LOWER", sql)

    def test_to_python_for_solo_aplica_pendientes(self):
        t = make_text_nfc("n")
        _, pending = t.to_sql_partial(MYSQL, PG)
        fn = t.to_python_for(pending)
        # Solo aplica NFC, no TRIM/LOWER
        self.assertEqual(fn("CAFÉ"), "CAFÉ")              # no baja a minúsculas
        nfd = unicodedata.normalize("NFD", "café")
        self.assertEqual(fn(nfd), "café")                 # sí normaliza NFC


class TestTextCanonicalValidate(unittest.TestCase):

    def test_validate_string(self):
        self.assertTrue(make_text("n").validate("hola"))

    def test_validate_no_string(self):
        self.assertFalse(make_text("n").validate(42))


# ─────────────────────────────────────────────────────────────────
# NumericCanonical
# ─────────────────────────────────────────────────────────────────

class TestNumericCanonical(unittest.TestCase):

    def test_sql_mysql_usa_decimal(self):
        sql = make_numeric("fare", 3, 10).to_sql(MYSQL)
        self.assertIn("DECIMAL", sql)
        self.assertIn("fare", sql)

    def test_sql_pg_usa_numeric(self):
        sql = make_numeric("fare", 3, 10).to_sql(PG)
        self.assertIn("NUMERIC", sql)

    def test_python_redondea(self):
        fn = make_numeric("fare", precision=2).to_python()
        self.assertEqual(fn(3.14159), 3.14)

    def test_python_float_string(self):
        fn = make_numeric("fare", precision=2).to_python()
        self.assertEqual(fn("3.14159"), 3.14)

    def test_python_none_nullable(self):
        fn = make_numeric("fare", nullable=True).to_python()
        self.assertEqual(fn(None), 0)

    def test_python_none_not_nullable(self):
        fn = make_numeric("fare", nullable=False).to_python()
        self.assertIsNone(fn(None))

    def test_paridad_precision_2(self):
        """58.004999 redondeado a 2 decimales: SQL y Python deben coincidir."""
        fn = make_numeric("fare", precision=2).to_python()
        self.assertEqual(fn(58.004999), round(58.004999, 2))

    def test_validate_float(self):
        self.assertTrue(make_numeric("fare").validate(3.14))

    def test_validate_string_numerica(self):
        self.assertTrue(make_numeric("fare").validate("3.14"))

    def test_validate_no_numerico(self):
        self.assertFalse(make_numeric("fare").validate("abc"))


class TestIntegerCanonical(unittest.TestCase):

    def test_sql_mysql(self):
        sql = make_integer("age").to_sql(MYSQL)
        self.assertIn("SIGNED", sql)

    def test_sql_pg(self):
        sql = make_integer("age").to_sql(PG)
        self.assertIn("INTEGER", sql)

    def test_python_entero(self):
        fn = make_integer("age").to_python()
        self.assertEqual(fn(5), 5)
        self.assertEqual(fn("42"), 42)

    def test_python_none_nullable(self):
        fn = make_integer("age", nullable=True).to_python()
        self.assertEqual(fn(None), 0)

    def test_validate_entero(self):
        self.assertTrue(make_integer("age").validate(5))
        self.assertFalse(make_integer("age").validate("abc"))


# ─────────────────────────────────────────────────────────────────
# BooleanCanonical
# ─────────────────────────────────────────────────────────────────

class TestBooleanCanonical(unittest.TestCase):

    TRUTHY = [True, 1, "true", "TRUE", "yes", "YES", "on", "1", "t", "T"]
    FALSY  = [False, 0, "false", "FALSE", "no", "NO", "off", "0", "f", "F"]

    def test_sql_mysql_tiene_case(self):
        sql = make_boolean("survived").to_sql(MYSQL)
        self.assertIn("CASE", sql)

    def test_sql_pg_tiene_ilike(self):
        sql = make_boolean("survived").to_sql(PG)
        self.assertIn("ILIKE", sql)

    def test_python_truthy_devuelve_1(self):
        fn = make_boolean("survived").to_python()
        for v in self.TRUTHY:
            with self.subTest(v=v):
                self.assertEqual(fn(v), 1, f"Esperado 1 para {v!r}")

    def test_python_falsy_devuelve_0(self):
        fn = make_boolean("survived").to_python()
        for v in self.FALSY:
            with self.subTest(v=v):
                self.assertEqual(fn(v), 0, f"Esperado 0 para {v!r}")

    def test_python_none_devuelve_none(self):
        fn = make_boolean("survived", nullable=True).to_python()
        self.assertIsNone(fn(None))

    def test_validate_bool_nativo(self):
        b = make_boolean("survived")
        self.assertTrue(b.validate(True))
        self.assertTrue(b.validate(False))

    def test_validate_texto(self):
        b = make_boolean("survived")
        self.assertTrue(b.validate("true"))
        self.assertFalse(b.validate("quizas"))


# ─────────────────────────────────────────────────────────────────
# TimestampCanonical
# ─────────────────────────────────────────────────────────────────

class TestTimestampCanonical(unittest.TestCase):

    def test_sql_mysql_tiene_convert_tz(self):
        sql = make_timestamp("ts").to_sql(MYSQL)
        self.assertIn("CONVERT_TZ", sql)

    def test_sql_pg_tiene_at_time_zone(self):
        sql = make_timestamp("ts").to_sql(PG)
        self.assertIn("AT TIME ZONE", sql)

    def test_sql_sin_force_utc_no_convert(self):
        t   = make_timestamp("ts", force_utc=False)
        sql = t.to_sql(PG)
        self.assertNotIn("TIME ZONE", sql)

    def test_sql_precision_day_pg(self):
        t   = make_timestamp("ts", precision="day")
        sql = t.to_sql(PG)
        self.assertIn("day", sql)

    def test_python_convierte_a_utc(self):
        fn = make_timestamp("ts", force_utc=True, precision="second").to_python()
        result = fn("2024-01-15T12:00:00+02:00")
        self.assertIn("10:00:00", result)   # UTC = +02:00 - 2h

    def test_python_trunca_a_segundo(self):
        fn = make_timestamp("ts", precision="second").to_python()
        result = fn("2024-01-15T12:00:00.987654+00:00")
        self.assertNotIn("987654", result)

    def test_python_trunca_a_dia(self):
        fn = make_timestamp("ts", precision="day", force_utc=False).to_python()
        result = fn("2024-01-15T14:30:00")
        self.assertIn("2024-01-15", result)
        self.assertIn("00:00:00", result)

    def test_python_none_nullable_devuelve_sentinel(self):
        fn = make_timestamp("ts", nullable=True).to_python()
        result = fn(None)
        self.assertIn("1970", result)

    def test_python_acepta_datetime_object(self):
        from datetime import datetime, timezone
        fn = make_timestamp("ts").to_python()
        dt = datetime(2024, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        self.assertIsNotNone(fn(dt))

    def test_validate_string(self):
        self.assertTrue(make_timestamp("ts").validate("2024-01-01"))

    def test_validate_datetime(self):
        from datetime import datetime
        self.assertTrue(make_timestamp("ts").validate(datetime.now()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
