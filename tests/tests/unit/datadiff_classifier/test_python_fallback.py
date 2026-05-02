"""
tests/unit/datadiff_classifier/test_python_fallback.py

Tests de PythonFallback: las transformaciones Python para
motores que no soportan operaciones SQL nativas.

PythonFallback es el mecanismo de compatibilidad que garantiza
que MySQL puede participar en comparaciones que requieren
Unicode NFC, NFKC o ascii_fold, operaciones no disponibles
en MySQL ≤ 8.0 de forma nativa.

Estos tests verifican la corrección semántica de las
transformaciones Python, que deben producir el mismo
resultado que la función SQL equivalente en PostgreSQL.
"""

import sys, os, unittest, unicodedata
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from canonical_engine.engine import PythonFallback


class TestNormalizeUnicode(unittest.TestCase):
    """PythonFallback.normalize_unicode() — equivalente a NORMALIZE(expr, form) en PG."""

    NFC_CASES = [
        # (input_en_NFD, expected_en_NFC)
        (unicodedata.normalize("NFD", "café"),   "café"),
        (unicodedata.normalize("NFD", "naïve"),  "naïve"),
        (unicodedata.normalize("NFD", "Ångström"), "Ångström"),
    ]

    NFKC_CASES = [
        # Ligaduras que NFKC expande
        ("ﬁ",  "fi"),    # fi ligature
        ("ﬀ",  "ff"),    # ff ligature
        ("ﬃ",  "ffi"),   # ffi ligature
        # Números en superíndice
        ("²",  "2"),
        ("³",  "3"),
    ]

    def test_nfc_normaliza_correctamente(self):
        for nfd_in, expected in self.NFC_CASES:
            with self.subTest(input=nfd_in):
                result = PythonFallback.normalize_unicode(nfd_in, "NFC")
                self.assertEqual(result, expected)
                self.assertEqual(unicodedata.is_normalized("NFC", result), True)

    def test_nfkc_expande_ligaduras(self):
        for raw, expected in self.NFKC_CASES:
            with self.subTest(input=raw):
                result = PythonFallback.normalize_unicode(raw, "NFKC")
                self.assertEqual(result, expected)

    def test_none_devuelve_none(self):
        self.assertIsNone(PythonFallback.normalize_unicode(None, "NFC"))

    def test_ya_nfc_no_cambia(self):
        """Una cadena ya en NFC no debe cambiar."""
        s = "café"   # ya en NFC
        self.assertEqual(PythonFallback.normalize_unicode(s, "NFC"), s)

    def test_ascii_no_cambia(self):
        """ASCII puro es invariante bajo cualquier forma Unicode."""
        s = "hello world 123"
        self.assertEqual(PythonFallback.normalize_unicode(s, "NFC"),  s)
        self.assertEqual(PythonFallback.normalize_unicode(s, "NFKC"), s)

    def test_nfc_idempotente(self):
        """Aplicar NFC dos veces produce el mismo resultado que una."""
        s = unicodedata.normalize("NFD", "café")
        r1 = PythonFallback.normalize_unicode(s, "NFC")
        r2 = PythonFallback.normalize_unicode(r1, "NFC")
        self.assertEqual(r1, r2)


class TestAsciiFold(unittest.TestCase):
    """PythonFallback.ascii_fold() — elimina marcas diacríticas."""

    CASES = [
        ("café",    "cafe"),
        ("niño",    "nino"),
        ("naïve",   "naive"),
        ("Ångström","Angstrom"),
        ("über",    "uber"),
        ("résumé",  "resume"),
        ("façade",  "facade"),
    ]

    def test_elimina_acentos(self):
        for original, expected in self.CASES:
            with self.subTest(original=original):
                result = PythonFallback.ascii_fold(original)
                self.assertEqual(result, expected)

    def test_none_devuelve_none(self):
        self.assertIsNone(PythonFallback.ascii_fold(None))

    def test_ascii_puro_no_cambia(self):
        s = "hello world 123"
        self.assertEqual(PythonFallback.ascii_fold(s), s)

    def test_mayusculas_se_preservan(self):
        """ascii_fold elimina acentos pero NO convierte a minúsculas."""
        result = PythonFallback.ascii_fold("CAFÉ")
        self.assertEqual(result, "CAFE")

    def test_idempotente(self):
        s = "café"
        r1 = PythonFallback.ascii_fold(s)
        r2 = PythonFallback.ascii_fold(r1)
        self.assertEqual(r1, r2)

    def test_caracteres_no_ascii_sin_diacritico_intactos(self):
        """Caracteres como ñ quedan como n; ß puede quedar como ß o ss."""
        result = PythonFallback.ascii_fold("niño")
        self.assertNotIn("ñ", result)


class TestCollapseSpaces(unittest.TestCase):
    """PythonFallback.collapse_spaces() — equivalente a REGEXP_REPLACE(s, '\\s+', ' ')."""

    def test_colapsa_espacios_multiples(self):
        self.assertEqual(
            PythonFallback.collapse_spaces("hola   mundo"),
            "hola mundo"
        )

    def test_elimina_espacios_iniciales_y_finales(self):
        self.assertEqual(
            PythonFallback.collapse_spaces("  hola  "),
            "hola"
        )

    def test_tabs_y_newlines(self):
        self.assertEqual(
            PythonFallback.collapse_spaces("hola\t\nmundo"),
            "hola mundo"
        )

    def test_cadena_sin_espacios_extra(self):
        self.assertEqual(
            PythonFallback.collapse_spaces("hola mundo"),
            "hola mundo"
        )

    def test_none_devuelve_none(self):
        self.assertIsNone(PythonFallback.collapse_spaces(None))

    def test_idempotente(self):
        s  = "  hola   mundo  "
        r1 = PythonFallback.collapse_spaces(s)
        r2 = PythonFallback.collapse_spaces(r1)
        self.assertEqual(r1, r2)

    def test_solo_espacios(self):
        """Una cadena de solo espacios colapsa a cadena vacía o espacio."""
        result = PythonFallback.collapse_spaces("   ")
        self.assertEqual(result.strip(), "")


class TestPythonFallbackParidadConPG(unittest.TestCase):
    """
    Tests de paridad: PythonFallback debe producir el mismo
    resultado semántico que NORMALIZE/unaccent en PostgreSQL.

    Sin acceso a PG real, verificamos que la salida Python
    cumple las propiedades que PostgreSQL garantiza:
      - Resultado en forma NFC verificable con unicodedata
      - ascii_fold: sin marcas combinantes (categoría Mn)
    """

    def test_nfc_result_es_nfc(self):
        """La salida de normalize_unicode(..., NFC) es válida NFC."""
        casos = ["café", "naïve", "résumé", "Ångström"]
        for s in casos:
            nfd = unicodedata.normalize("NFD", s)
            result = PythonFallback.normalize_unicode(nfd, "NFC")
            with self.subTest(s=s):
                self.assertTrue(
                    unicodedata.is_normalized("NFC", result),
                    f"{result!r} no está en NFC"
                )

    def test_ascii_fold_sin_marcas_combinantes(self):
        """La salida de ascii_fold no debe contener marcas combinantes."""
        casos = ["café", "naïve", "niño"]
        for s in casos:
            result = PythonFallback.ascii_fold(s)
            nfd    = unicodedata.normalize("NFD", result)
            marcas = [c for c in nfd if unicodedata.category(c) == "Mn"]
            with self.subTest(s=s):
                self.assertEqual(marcas, [],
                    f"Marcas combinantes encontradas en {result!r}: {marcas}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
