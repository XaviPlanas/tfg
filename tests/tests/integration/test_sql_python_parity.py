"""
tests/integration/test_sql_python_parity.py

Tests de paridad SQL ↔ Python: verifican que la capa POST (Python)
produce resultados semánticamente equivalentes a la capa PRE (SQL).

Estos tests son cruciales para la arquitectura PRE/POST:
el invariante fundamental del sistema es que to_python() y to_sql()
producen el mismo resultado sobre el mismo valor de entrada.

Sin acceso a una BD real, usamos SQLite (stdlib) para ejecutar
el SQL directamente y comparar con el callable Python.
SQLite no soporta todas las funciones, pero sí TRIM, LOWER, ROUND,
CAST y COALESCE, que cubre las transformaciones PRE más comunes.

Para NFC y otras transforms no soportadas en SQLite,
la paridad se verifica a nivel lógico (la propiedad matemática).

Marcados como 'integration' pero no requieren MySQL/PG.
"""

import sys, os, unittest, sqlite3, unicodedata
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from canonical_engine.types.text    import TextCanonical, TextTransformation
from canonical_engine.types.numeric import NumericCanonical
from canonical_engine.types.boolean import BooleanCanonical
from canonical_engine.post_canonicalizer import PostCanonicalizer
from canonical_engine.plan               import CanonicalColumn, CanonicalPlan, Layer
from tests.conftest import MYSQL, PG, make_text, make_numeric, make_boolean


def _sqlite_eval(expr: str, value, col="col") -> any:
    """Evalúa una expresión SQL en SQLite con el valor dado."""
    conn   = sqlite3.connect(":memory:")
    cursor = conn.execute(f"SELECT {expr} FROM (SELECT ? AS {col})", (value,))
    result = cursor.fetchone()[0]
    conn.close()
    return result


class TestParidadTrimLower(unittest.TestCase):
    """
    TRIM + LOWER: soportados en SQLite → paridad directamente verificable.
    """

    CASES = [
        "  JACK DAWSON  ",
        "ROSE DEWITT BUKATER",
        "  mr. thomas andrews  ",
        "",
        "   ",
    ]

    def test_trim_paridad(self):
        t  = make_text("col", [TextTransformation.TRIM])
        fn = t.to_python()
        for val in self.CASES:
            with self.subTest(val=repr(val)):
                sql_result = _sqlite_eval("TRIM(col)", val)
                py_result  = fn(val)
                self.assertEqual(py_result, sql_result,
                    f"TRIM diverge: SQL={sql_result!r} Python={py_result!r}")

    def test_lower_paridad(self):
        t  = make_text("col", [TextTransformation.LOWERCASE])
        fn = t.to_python()
        for val in ["JACK", "Rose", "Mr. SMITH", "123abc"]:
            with self.subTest(val=val):
                sql_result = _sqlite_eval("LOWER(col)", val)
                py_result  = fn(val)
                self.assertEqual(py_result, sql_result)

    def test_trim_lower_compuesto_paridad(self):
        t   = make_text("col", [TextTransformation.TRIM, TextTransformation.LOWERCASE])
        fn  = t.to_python()
        for val in self.CASES:
            with self.subTest(val=repr(val)):
                sql_result = _sqlite_eval("LOWER(TRIM(col))", val)
                py_result  = fn(val)
                self.assertEqual(py_result, sql_result)


class TestParidadNFC(unittest.TestCase):
    """
    NFC no se puede verificar en SQLite (sin NORMALIZE).
    Verificamos las propiedades matemáticas que garantizan paridad:
      1. Idempotencia: NFC(NFC(x)) == NFC(x)
      2. Invarianza ASCII: NFC no cambia texto ASCII
      3. Equivalencia: NFC(NFD(x)) == NFC(x) para todo x
    """

    NFC_VALUES = [
        "café", "naïve", "résumé", "Ångström", "niño",
        "hello", "123", "Mr. Jack Dawson",
    ]

    def test_nfc_idempotencia(self):
        fn = make_text("n", [TextTransformation.NFC]).to_python()
        for val in self.NFC_VALUES:
            with self.subTest(val=val):
                r1 = fn(val)
                r2 = fn(r1)
                self.assertEqual(r1, r2,
                    f"NFC no es idempotente para {val!r}")

    def test_nfc_invarianza_ascii(self):
        fn = make_text("n", [TextTransformation.NFC]).to_python()
        ascii_vals = ["hello", "world", "123 abc", "Mr. Smith"]
        for val in ascii_vals:
            with self.subTest(val=val):
                self.assertEqual(fn(val), val)

    def test_nfc_nfd_equivalencia(self):
        """NFC(NFD(x)) debe ser igual a NFC(x)."""
        fn = make_text("n", [TextTransformation.NFC]).to_python()
        for val in self.NFC_VALUES:
            with self.subTest(val=val):
                nfd = unicodedata.normalize("NFD", val)
                self.assertEqual(fn(nfd), fn(val))

    def test_nfc_elimina_diferencias_representacion(self):
        """
        El caso de uso central: dos representaciones del mismo
        carácter se unifican a una sola.
        Este es el falso positivo que el sistema elimina.
        """
        fn  = make_text("n", [TextTransformation.NFC]).to_python()
        nfc = "café"                                   # NFC: 4 chars
        nfd = unicodedata.normalize("NFD", "café")     # NFD: 5 chars
        self.assertNotEqual(nfc, nfd)    # Son distintos en raw
        self.assertEqual(fn(nfc), fn(nfd))  # Iguales tras NFC


class TestParidadNumeric(unittest.TestCase):
    """ROUND: soportado en SQLite → paridad directamente verificable."""

    CASES = [
        (58.0, 2),
        (58.999, 2),
        (3.14159265, 3),
        (0.0, 3),
        (100.0, 2),
        (-3.145, 2),
    ]

    def test_round_paridad_sqlite(self):
        for val, prec in self.CASES:
            with self.subTest(val=val, prec=prec):
                t           = make_numeric("col", precision=prec)
                fn          = t.to_python()
                sql_result  = _sqlite_eval(f"ROUND(col, {prec})", val)
                py_result   = fn(val)
                self.assertEqual(py_result, sql_result,
                    f"ROUND({val},{prec}): SQL={sql_result} Python={py_result}")

    def test_round_string_como_float(self):
        fn = make_numeric("col", precision=2).to_python()
        self.assertEqual(fn("3.14159"), 3.14)

    def test_round_none_nullable(self):
        fn = make_numeric("col", nullable=True).to_python()
        self.assertEqual(fn(None), 0)


class TestPostCanonicalizerEliminaFalsosPositivos(unittest.TestCase):
    """
    Test de integración del flujo completo:
    PRE (TRIM+LOWER en SQL) + POST (NFC en Python)
    elimina el falso positivo canónico del sistema.
    """

    def _make_post_can_nfc(self):
        def nfc_fn(v):
            return unicodedata.normalize("NFC", v) if v else v
        nfc_fn.__name__ = "TextPost(unicode_nfc)"

        col = CanonicalColumn(
            name="name", layer=Layer.SPLIT,
            sql_expression="LOWER(TRIM(name)) AS name",
            post_fn=nfc_fn, pending_transforms=["unicode_nfc"],
            information_loss="",
        )
        plan = CanonicalPlan(
            table_name="t", dialect_name="mysql",
            columns={"name": col}
        )
        return PostCanonicalizer(plan, plan)

    def test_falso_positivo_nfc_resuelto(self):
        """
        Escenario real:
          MySQL  devuelve "cafe\u0301" (NFD, sin transformar)
          PG     devuelve "caf\u00e9"  (NFC, NORMALIZE en SQL)
          → data-diff reporta diferencia
          → PostCanonicalizer aplica NFC a ambos
          → diferencia desaparece
        """
        from tests.conftest import diff_update
        pc  = self._make_post_can_nfc()
        nfd = "cafe\u0301"   # NFD: e + combining accent
        nfc = "caf\u00e9"    # NFC: é precompuesto

        # Esto es lo que data-diff reportaría
        row = diff_update(1, {"name": nfd}, {"name": nfc})
        remaining, resolved = pc.apply_batch([row])

        self.assertEqual(len(resolved),  1,
            "El falso positivo NFC no fue resuelto por PostCanonicalizer")
        self.assertEqual(len(remaining), 0,
            "No debería haber diferencias restantes")

    def test_diferencia_real_no_resuelta(self):
        """Una diferencia semántica real no debe ser eliminada."""
        from tests.conftest import diff_update
        pc  = self._make_post_can_nfc()
        row = diff_update(2, {"name": "jack"}, {"name": "rose"})
        remaining, resolved = pc.apply_batch([row])
        self.assertEqual(len(remaining), 1)
        self.assertEqual(len(resolved),  0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
