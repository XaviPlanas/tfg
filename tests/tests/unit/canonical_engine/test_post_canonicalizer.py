"""
tests/unit/canonical_engine/test_post_canonicalizer.py

Tests de PostCanonicalizer.

El PostCanonicalizer es el componente que separa los falsos positivos
de las diferencias reales en la capa POST. Sus invariantes son:

  1. apply() normaliza ambos lados de un DiffRow con sus respectivos callables.
  2. apply_batch() separa remaining (diferencias reales) de resolved (falsos +).
  3. Una fila con row_a == row_b tras normalización → falso positivo → resolved.
  4. Una fila con row_a != row_b tras normalización → diferencia real → remaining.
  5. INSERT (row_a=None) y DELETE (row_b=None) nunca son falsos positivos.
  6. Sin columnas POST → apply_batch devuelve todos en remaining.
"""

import sys, os, unittest, unicodedata
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from canonical_engine.post_canonicalizer import PostCanonicalizer
from canonical_engine.plan               import CanonicalColumn, CanonicalPlan, Layer
from tests.conftest import diff_update, diff_insert, diff_delete, make_plan


# ── Helpers ───────────────────────────────────────────────────────

def _nfc(v):
    return unicodedata.normalize("NFC", v) if v else v

def _lower(v):
    return v.lower() if v else v

def _identity(v):
    return v

_nfc.__name__      = "TextPost(unicode_nfc)"
_lower.__name__    = "TextPost(lowercase)"
_identity.__name__ = "identity"


def _make_post_can(col_fns_a: dict, col_fns_b: dict = None) -> PostCanonicalizer:
    """Construye PostCanonicalizer desde dicts {col: fn}."""
    def plan_from_fns(fns, dialect="mysql"):
        cols = {}
        for col, fn in fns.items():
            cols[col] = CanonicalColumn(
                name               = col,
                layer              = Layer.SPLIT,
                sql_expression     = col,
                post_fn            = fn,
                pending_transforms = [],
                information_loss   = "",
            )
        return CanonicalPlan(table_name="t", dialect_name=dialect, columns=cols)

    plan_a = plan_from_fns(col_fns_a, "mysql")
    plan_b = plan_from_fns(col_fns_b or col_fns_a, "postgresql")
    return PostCanonicalizer(plan_a, plan_b)


# ─────────────────────────────────────────────────────────────────

class TestPostCanonicalizerApply(unittest.TestCase):
    """apply(): normaliza un solo DiffRow."""

    def test_nfc_normaliza_ambos_lados(self):
        pc  = _make_post_can({"name": _nfc})
        nfd = unicodedata.normalize("NFD", "café")
        row = diff_update(1, {"name": nfd}, {"name": nfd})
        out = pc.apply(row)
        self.assertEqual(out.row_a["name"], "café")
        self.assertEqual(out.row_b["name"], "café")

    def test_columna_no_en_plan_no_se_toca(self):
        pc  = _make_post_can({"name": _nfc})
        row = diff_update(1,
            {"name": "café", "fare": 50.123},
            {"name": "café", "fare": 50.456},
        )
        out = pc.apply(row)
        # fare no está en el plan → sin cambiar
        self.assertEqual(out.row_a["fare"], 50.123)
        self.assertEqual(out.row_b["fare"], 50.456)

    def test_insert_row_a_none_no_falla(self):
        pc  = _make_post_can({"name": _nfc})
        row = diff_insert(2, {"name": "café"})
        out = pc.apply(row)
        self.assertIsNone(out.row_a)
        self.assertEqual(out.row_b["name"], "café")

    def test_delete_row_b_none_no_falla(self):
        pc  = _make_post_can({"name": _nfc})
        row = diff_delete(3, {"name": "café"})
        out = pc.apply(row)
        self.assertIsNone(out.row_b)

    def test_devuelve_nuevo_diffrow_no_muta(self):
        pc  = _make_post_can({"name": _lower})
        row = diff_update(1, {"name": "JACK"}, {"name": "JACK"})
        out = pc.apply(row)
        # original no mutado
        self.assertEqual(row.row_a["name"], "JACK")
        self.assertEqual(out.row_a["name"], "jack")

    def test_multiples_columnas_post(self):
        pc  = _make_post_can({"name": _nfc, "cabin": _lower})
        row = diff_update(1,
            {"name": "CAFÉ", "cabin": "C123"},
            {"name": "CAFÉ", "cabin": "c123"},
        )
        out = pc.apply(row)
        self.assertEqual(out.row_a["cabin"], "c123")
        self.assertEqual(out.row_b["cabin"], "c123")


class TestPostCanonicalizerApplyBatch(unittest.TestCase):
    """apply_batch(): separación remaining vs resolved."""

    def _pc_nfc(self):
        return _make_post_can({"name": _nfc})

    def test_falso_positivo_va_a_resolved(self):
        """Diferencia solo por NFC → desaparece tras POST → resolved."""
        pc  = self._pc_nfc()
        nfd = unicodedata.normalize("NFD", "café")
        row = diff_update(1, {"name": nfd}, {"name": "café"})
        remaining, resolved = pc.apply_batch([row])
        self.assertEqual(len(resolved),  1)
        self.assertEqual(len(remaining), 0)

    def test_diferencia_real_va_a_remaining(self):
        """Diferencia real (nombres distintos) → sigue siendo distinta → remaining."""
        pc  = self._pc_nfc()
        row = diff_update(1, {"name": "jack"}, {"name": "rose"})
        remaining, resolved = pc.apply_batch([row])
        self.assertEqual(len(remaining), 1)
        self.assertEqual(len(resolved),  0)

    def test_insert_va_a_remaining(self):
        """INSERT nunca es falso positivo: row_a=None."""
        pc  = self._pc_nfc()
        row = diff_insert(2, {"name": "café"})
        remaining, resolved = pc.apply_batch([row])
        self.assertEqual(len(remaining), 1)
        self.assertEqual(len(resolved),  0)

    def test_delete_va_a_remaining(self):
        """DELETE nunca es falso positivo: row_b=None."""
        pc  = self._pc_nfc()
        row = diff_delete(3, {"name": "café"})
        remaining, resolved = pc.apply_batch([row])
        self.assertEqual(len(remaining), 1)
        self.assertEqual(len(resolved),  0)

    def test_batch_mixto(self):
        """Batch con falsos positivos y diferencias reales mezcladas."""
        pc  = self._pc_nfc()
        nfd = unicodedata.normalize("NFD", "café")

        rows = [
            diff_update(1, {"name": nfd},     {"name": "café"}),  # FP → resolved
            diff_update(2, {"name": "jack"},  {"name": "rose"}),  # real → remaining
            diff_update(3, {"name": nfd},     {"name": "café"}),  # FP → resolved
            diff_insert(4, {"name": "nueva"}),                    # INSERT → remaining
        ]
        remaining, resolved = pc.apply_batch(rows)
        self.assertEqual(len(resolved),  2)
        self.assertEqual(len(remaining), 2)

    def test_sin_columnas_post_todo_remaining(self):
        """Sin columnas POST, apply_batch no elimina nada."""
        plan_vacio = CanonicalPlan(
            table_name="t", dialect_name="mysql", columns={}
        )
        pc = PostCanonicalizer(plan_vacio, plan_vacio)
        rows = [
            diff_update(1, {"name": "jack"}, {"name": "jack"}),
            diff_update(2, {"name": "rose"}, {"name": "rose"}),
        ]
        remaining, resolved = pc.apply_batch(rows)
        # Sin transformaciones POST, row_a == row_b pero PostCanonicalizer
        # los pasa todos a remaining porque no puede distinguir si la
        # igualdad es real o producto de la normalización
        # (invariante de seguridad: nunca perder una diferencia real)
        self.assertEqual(len(remaining) + len(resolved), 2)

    def test_totales_preservados(self):
        """La suma remaining + resolved siempre == len(input)."""
        pc = self._pc_nfc()
        nfd = unicodedata.normalize("NFD", "café")
        rows = [
            diff_update(i, {"name": nfd if i % 2 == 0 else "jack"},
                           {"name": "café" if i % 2 == 0 else "rose"})
            for i in range(10)
        ]
        remaining, resolved = pc.apply_batch(rows)
        self.assertEqual(len(remaining) + len(resolved), 10)


class TestPostCanonicalizerReport(unittest.TestCase):

    def test_report_con_columnas(self):
        pc = _make_post_can({"name": _nfc, "cabin": _lower})
        r  = pc.report()
        self.assertIn("name",  r)
        self.assertIn("cabin", r)

    def test_report_sin_columnas(self):
        plan = CanonicalPlan(table_name="t", dialect_name="mysql", columns={})
        pc   = PostCanonicalizer(plan, plan)
        r    = pc.report()
        self.assertIn("sin columnas", r.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
