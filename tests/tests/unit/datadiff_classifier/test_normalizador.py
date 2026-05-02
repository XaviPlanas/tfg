"""
tests/unit/datadiff_classifier/test_normalizador.py

Tests del normalizador determinista del DiffClassifier.

El normalizador resuelve casos obvios SIN llamar al LLM:
  - row_a=None → SEMANTICALLY_DIFFERENT, confianza=1
  - row_b=None → SEMANTICALLY_DIFFERENT, confianza=1
  - ambos presentes → None (pasa al LLM)

Estrategia de mock:
  DiffClassifier importa anthropic y ollama en su módulo y los
  instancia en __init__. Para evitar dependencias de red:
    1. Se inyectan stubs en sys.modules antes de cualquier import.
    2. Se construye la instancia con __new__ sin ejecutar __init__.
    3. Los atributos necesarios se asignan directamente.
  Esto permite testear __normalizador vía name mangling sin
  credenciales ni servicios externos.
"""

import sys, os, unittest
from unittest.mock import MagicMock

# ── Paths: necesitamos tanto tfg-new/ como tfg-main/src/ ─────────
_HERE = os.path.dirname(__file__)
_NEW  = os.path.abspath(os.path.join(_HERE, "../../.."))
_SRC  = os.path.abspath(os.path.join(_HERE, "../../../../tfg-main/src"))
for p in (_NEW, _SRC):
    if p not in sys.path:
        sys.path.insert(0, p)

# ── Stubs antes de importar el clasificador ───────────────────────
sys.modules.setdefault("anthropic", MagicMock())
sys.modules.setdefault("ollama",    MagicMock())

from datadiff_classifier.models import DiffCategory, DiffAction
from tests.conftest              import diff_update, diff_insert, diff_delete

# ── Disponibilidad del clasificador ───────────────────────────────
try:
    from tfg.datadiff_classifier.classifier import DiffClassifier
    CLASSIFIER_AVAILABLE = True
except Exception as e:
    CLASSIFIER_AVAILABLE = False
    _SKIP_REASON = f"DiffClassifier no importable: {e}"


def _make_classifier():
    dc = DiffClassifier.__new__(DiffClassifier)
    dc.schema_context        = ""
    dc.llm_provider          = "anthropic"
    dc.model                 = "claude-haiku-4-5"
    dc.temperature           = 0.0
    dc.api_key               = "test-key"
    dc.prompt_template       = ""
    dc.uncertainty_threshold = 0.7
    dc.ollama_use_chat       = False
    dc.client                = MagicMock()
    return dc


@unittest.skipUnless(CLASSIFIER_AVAILABLE,
                     "DiffClassifier requiere tfg-main/src en PYTHONPATH")
class TestNormalizadorDeterministico(unittest.TestCase):

    def setUp(self):
        self.dc = _make_classifier()
        self.norm = self.dc._DiffClassifier__normalizador

    def test_insert_devuelve_clasificacion(self):
        result = self.norm(diff_insert(1, {"name": "nueva"}))
        self.assertIsNotNone(result)

    def test_insert_categoria_different(self):
        result = self.norm(diff_insert(1, {"name": "nueva"}))
        self.assertEqual(result.categoria, DiffCategory.SEMANTICALLY_DIFFERENT)

    def test_insert_confianza_maxima(self):
        result = self.norm(diff_insert(1, {"name": "nueva"}))
        self.assertEqual(result.confianza, 1)

    def test_insert_accion_valida(self):
        result = self.norm(diff_insert(1, {"name": "nueva"}))
        self.assertIn(result.accion, (DiffAction.INSERT, DiffAction.DELETE))

    def test_insert_key_preservado(self):
        result = self.norm(diff_insert(42, {"name": "test"}))
        self.assertEqual(result.key, 42)

    def test_insert_explicacion_no_vacia(self):
        result = self.norm(diff_insert(1, {"name": "test"}))
        self.assertGreater(len(result.explicacion), 0)

    def test_delete_devuelve_clasificacion(self):
        result = self.norm(diff_delete(2, {"name": "eliminada"}))
        self.assertIsNotNone(result)

    def test_delete_categoria_different(self):
        result = self.norm(diff_delete(2, {"name": "eliminada"}))
        self.assertEqual(result.categoria, DiffCategory.SEMANTICALLY_DIFFERENT)

    def test_delete_confianza_maxima(self):
        result = self.norm(diff_delete(2, {"name": "eliminada"}))
        self.assertEqual(result.confianza, 1)

    def test_delete_key_preservado(self):
        result = self.norm(diff_delete(99, {"name": "eliminada"}))
        self.assertEqual(result.key, 99)

    def test_update_devuelve_none(self):
        result = self.norm(diff_update(3, {"name": "jack"}, {"name": "JACK"}))
        self.assertIsNone(result)

    def test_update_identico_devuelve_none(self):
        result = self.norm(diff_update(4, {"name": "jack"}, {"name": "jack"}))
        self.assertIsNone(result)

    def test_insert_no_invoca_llm(self):
        self.norm(diff_insert(1, {"name": "test"}))
        self.dc.client.messages.create.assert_not_called()

    def test_delete_no_invoca_llm(self):
        self.norm(diff_delete(1, {"name": "test"}))
        self.dc.client.messages.create.assert_not_called()


@unittest.skipUnless(CLASSIFIER_AVAILABLE,
                     "DiffClassifier requiere tfg-main/src en PYTHONPATH")
class TestNormalizadorEdgeCases(unittest.TestCase):

    def setUp(self):
        self.dc   = _make_classifier()
        self.norm = self.dc._DiffClassifier__normalizador

    def test_update_valores_none_en_columnas(self):
        result = self.norm(diff_update(1, {"name": None}, {"name": None}))
        self.assertIsNone(result)

    def test_update_filas_vacias(self):
        result = self.norm(diff_update(1, {}, {}))
        self.assertIsNone(result)

    def test_insert_fila_vacia(self):
        result = self.norm(diff_insert(1, {}))
        self.assertIsNotNone(result)
        self.assertEqual(result.categoria, DiffCategory.SEMANTICALLY_DIFFERENT)

    def test_delete_fila_vacia(self):
        result = self.norm(diff_delete(1, {}))
        self.assertIsNotNone(result)
        self.assertEqual(result.categoria, DiffCategory.SEMANTICALLY_DIFFERENT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
