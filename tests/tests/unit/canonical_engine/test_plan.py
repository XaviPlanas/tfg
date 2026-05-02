"""
tests/unit/canonical_engine/test_plan.py

Tests de CanonicalPlan y CanonicalColumn.

Verifica:
  - pre_expressions() devuelve solo columnas PRE y SPLIT
  - post_callables()  devuelve solo columnas SPLIT y POST
  - passthrough_columns() identifica correctamente las POST
  - report() produce una cadena con la información correcta
  - Coherencia: una columna no puede estar en pre y post a la vez
"""

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from canonical_engine.plan    import CanonicalColumn, CanonicalPlan, Layer
from canonical_engine.types.text import TextTransformation
from tests.conftest import make_plan


def _identity(x):
    return x

def _nfc(x):
    import unicodedata
    return unicodedata.normalize("NFC", x) if x else x

_nfc.__name__ = "TextPost(unicode_nfc)"


class TestCanonicalColumn(unittest.TestCase):

    def _make(self, layer, post_fn=None, pending=None):
        return CanonicalColumn(
            name               = "name",
            layer              = layer,
            sql_expression     = "LOWER(TRIM(name)) AS name",
            post_fn            = post_fn,
            pending_transforms = pending or [],
            information_loss   = "test",
        )

    def test_pre_sin_post_fn(self):
        col = self._make(Layer.PRE)
        self.assertIsNone(col.post_fn)
        self.assertEqual(col.pending_transforms, [])

    def test_split_con_post_fn(self):
        col = self._make(Layer.SPLIT, post_fn=_nfc, pending=[TextTransformation.NFC])
        self.assertIsNotNone(col.post_fn)
        self.assertIn(TextTransformation.NFC, col.pending_transforms)

    def test_post_con_post_fn(self):
        col = self._make(Layer.POST, post_fn=_identity)
        self.assertEqual(col.layer, Layer.POST)


class TestCanonicalPlanAccesores(unittest.TestCase):
    """
    pre_expressions(), post_callables(), passthrough_columns()
    deben reflejar fielmente la capa de cada columna.
    """

    def setUp(self):
        self.plan = make_plan({
            "fare":     (Layer.PRE,   "ROUND(fare,3) AS fare",       None,  []),
            "survived": (Layer.PRE,   "CASE ... AS survived",        None,  []),
            "name":     (Layer.SPLIT, "LOWER(TRIM(name)) AS name",   _nfc,  ["unicode_nfc"]),
            "cabin":    (Layer.POST,  "cabin",                       _identity, []),
        })

    def test_pre_expressions_incluye_pre_y_split(self):
        exprs = self.plan.pre_expressions()
        self.assertIn("fare",     exprs)
        self.assertIn("survived", exprs)
        self.assertIn("name",     exprs)

    def test_pre_expressions_excluye_post(self):
        exprs = self.plan.pre_expressions()
        self.assertNotIn("cabin", exprs)

    def test_post_callables_incluye_split_y_post(self):
        callables = self.plan.post_callables()
        self.assertIn("name",  callables)
        self.assertIn("cabin", callables)

    def test_post_callables_excluye_pre(self):
        callables = self.plan.post_callables()
        self.assertNotIn("fare",     callables)
        self.assertNotIn("survived", callables)

    def test_passthrough_solo_post(self):
        pt = self.plan.passthrough_columns()
        self.assertEqual(pt, ["cabin"])

    def test_post_callables_son_callable(self):
        for col, fn in self.plan.post_callables().items():
            with self.subTest(col=col):
                self.assertTrue(callable(fn), f"{col}.post_fn no es callable")

    def test_split_callable_aplica_nfc(self):
        """El callable de la columna SPLIT aplica NFC correctamente."""
        import unicodedata
        callables = self.plan.post_callables()
        nfd_cafe  = unicodedata.normalize("NFD", "café")
        result    = callables["name"](nfd_cafe)
        self.assertEqual(result, "café")


class TestCanonicalPlanSinPost(unittest.TestCase):
    """Plan donde todo está en PRE: sin columnas POST ni SPLIT."""

    def setUp(self):
        self.plan = make_plan({
            "fare": (Layer.PRE, "ROUND(fare,3) AS fare", None, []),
            "age":  (Layer.PRE, "CAST(age AS INTEGER) AS age", None, []),
        })

    def test_post_callables_vacio(self):
        self.assertEqual(self.plan.post_callables(), {})

    def test_passthrough_vacio(self):
        self.assertEqual(self.plan.passthrough_columns(), [])

    def test_pre_expressions_todas(self):
        exprs = self.plan.pre_expressions()
        self.assertEqual(set(exprs.keys()), {"fare", "age"})


class TestCanonicalPlanReport(unittest.TestCase):
    """report() debe contener la información relevante."""

    def setUp(self):
        self.plan = make_plan({
            "fare": (Layer.PRE,   "ROUND(fare,3) AS fare",     None,  []),
            "name": (Layer.SPLIT, "LOWER(name) AS name",       _nfc,  ["unicode_nfc"]),
            "cabin":(Layer.POST,  "cabin",                     _identity, []),
        }, table="titanic", dialect="mysql")

    def test_report_contiene_nombre_tabla(self):
        r = self.plan.report()
        self.assertIn("titanic", r)

    def test_report_contiene_dialecto(self):
        r = self.plan.report()
        self.assertIn("mysql", r)

    def test_report_contiene_capas(self):
        r = self.plan.report()
        self.assertIn("PRE",   r)
        self.assertIn("SPLIT", r)
        self.assertIn("POST",  r)

    def test_report_contiene_nombres_columnas(self):
        r = self.plan.report()
        self.assertIn("fare",  r)
        self.assertIn("name",  r)
        self.assertIn("cabin", r)

    def test_report_es_string(self):
        self.assertIsInstance(self.plan.report(), str)


if __name__ == "__main__":
    unittest.main(verbosity=2)
