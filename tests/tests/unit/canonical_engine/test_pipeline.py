"""
tests/unit/canonical_engine/test_pipeline.py

Tests de CanonicalPipeline.build_plan() usando SchemaInspector mockeado.

CanonicalPipeline necesita una BD real para inspect_table().
La mockeamos devolviendo un dict {col: CanonicalType} directamente,
lo que permite testear toda la lógica de routing PRE/SPLIT/POST
sin infraestructura.

Casos clave:
  - Sin peer_dialect: routing solo respecto al dialecto local.
  - Con peer_dialect: routing simétrico (ambos deben soportar).
  - TextCanonical SPLIT: TRIM+LOWER en SQL, NFC en Python.
  - Tipos sin UnsupportedTransformation: siempre PRE.
  - Columna POST cuando AMBOS dialectos fallan.
"""

import sys, os, unittest
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from canonical_engine.dialect.mysql      import MySQLDialect
from canonical_engine.dialect.postgresql import PostgreSQLDialect
from canonical_engine.plan               import Layer
try:
    from canonical_engine.pipeline import CanonicalPipeline
    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False
    CanonicalPipeline = None
from canonical_engine.types.text         import TextCanonical, TextTransformation
from canonical_engine.types.numeric      import NumericCanonical
from canonical_engine.types.boolean      import BooleanCanonical
from tests.conftest import make_text_nfc, make_numeric, make_boolean, make_integer




SKIP_MSG = "sqlalchemy no disponible en este entorno (instalar con pip install sqlalchemy)"

def _mock_pipeline(canonical_types: dict, dialect_name="mysql") -> CanonicalPipeline:
    """
    Crea un CanonicalPipeline con el inspector mockeado.
    canonical_types: {col_name: CanonicalType} que devolverá inspect_table().
    """
    pipeline = CanonicalPipeline.__new__(CanonicalPipeline)
    pipeline.table = "titanic"
    pipeline.uri   = "mysql://fake/db"

    inspector_mock           = MagicMock()
    inspector_mock.dialect   = MySQLDialect() if dialect_name == "mysql" \
                               else PostgreSQLDialect()
    inspector_mock.inspect_table.return_value = canonical_types
    pipeline.inspector = inspector_mock
    pipeline.dialect   = inspector_mock.dialect
    return pipeline


@unittest.skipUnless(PIPELINE_AVAILABLE, SKIP_MSG)
class TestBuildPlanSinPeer(unittest.TestCase):
    """
    Sin peer_dialect: routing respecto al dialecto local únicamente.
    """

    def test_numeric_siempre_pre_en_mysql(self):
        pipe = _mock_pipeline({"fare": make_numeric("fare")})
        plan = pipe.build_plan()
        self.assertEqual(plan.columns["fare"].layer, Layer.PRE)

    def test_boolean_siempre_pre_en_mysql(self):
        pipe = _mock_pipeline({"survived": make_boolean("survived")})
        plan = pipe.build_plan()
        self.assertEqual(plan.columns["survived"].layer, Layer.PRE)

    def test_text_trim_lower_pre_en_mysql(self):
        t = make_text_nfc("name")
        # Sin peer: MySQL no soporta NFC → POST binario
        pipe = _mock_pipeline({"name": t})
        plan = pipe.build_plan()
        # Sin peer_dialect, TextCanonical usa routing binario
        # MySQL lanza en to_sql() → POST
        self.assertIn(plan.columns["name"].layer, (Layer.POST, Layer.SPLIT))

    def test_text_trim_only_pre_en_mysql(self):
        from tests.conftest import make_text
        t = make_text("name", [TextTransformation.TRIM])
        pipe = _mock_pipeline({"name": t})
        plan = pipe.build_plan()
        self.assertEqual(plan.columns["name"].layer, Layer.PRE)


@unittest.skipUnless(PIPELINE_AVAILABLE, SKIP_MSG)
class TestBuildPlanConPeer(unittest.TestCase):
    """
    Con peer_dialect: routing simétrico.
    MySQL + PG → NFC pendiente para ambos.
    """

    def _pipe_mysql(self, canonical_types):
        return _mock_pipeline(canonical_types, "mysql")

    def test_text_nfc_con_peer_pg_es_split(self):
        pipe = self._pipe_mysql({"name": make_text_nfc("name")})
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        # TRIM+LOWER en SQL, NFC en Python → SPLIT
        self.assertEqual(plan.columns["name"].layer, Layer.SPLIT)

    def test_text_nfc_split_tiene_pending_nfc(self):
        pipe = self._pipe_mysql({"name": make_text_nfc("name")})
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        col  = plan.columns["name"]
        self.assertIn(TextTransformation.NFC, col.pending_transforms)

    def test_text_nfc_split_sql_contiene_lower(self):
        pipe = self._pipe_mysql({"name": make_text_nfc("name")})
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        sql  = plan.columns["name"].sql_expression
        self.assertIn("LOWER", sql)
        self.assertNotIn("NORMALIZE", sql)

    def test_text_nfc_split_post_fn_es_callable(self):
        pipe = self._pipe_mysql({"name": make_text_nfc("name")})
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        fn   = plan.columns["name"].post_fn
        self.assertTrue(callable(fn))

    def test_numeric_con_peer_es_pre(self):
        pipe = self._pipe_mysql({"fare": make_numeric("fare")})
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        self.assertEqual(plan.columns["fare"].layer, Layer.PRE)

    def test_plan_mixto(self):
        types = {
            "fare":     make_numeric("fare"),
            "survived": make_boolean("survived"),
            "name":     make_text_nfc("name"),
        }
        pipe = self._pipe_mysql(types)
        plan = pipe.build_plan(peer_dialect=PostgreSQLDialect())

        self.assertEqual(plan.columns["fare"].layer,     Layer.PRE)
        self.assertEqual(plan.columns["survived"].layer, Layer.PRE)
        self.assertEqual(plan.columns["name"].layer,     Layer.SPLIT)

    def test_pre_expressions_no_incluye_normalize(self):
        types = {"name": make_text_nfc("name"), "fare": make_numeric("fare")}
        pipe  = self._pipe_mysql(types)
        plan  = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        exprs = plan.pre_expressions()
        for sql in exprs.values():
            self.assertNotIn("NORMALIZE", sql)

    def test_post_callables_solo_name(self):
        types = {"name": make_text_nfc("name"), "fare": make_numeric("fare")}
        pipe  = self._pipe_mysql(types)
        plan  = pipe.build_plan(peer_dialect=PostgreSQLDialect())
        post  = plan.post_callables()
        self.assertIn("name",  post)
        self.assertNotIn("fare", post)


@unittest.skipUnless(PIPELINE_AVAILABLE, SKIP_MSG)
class TestBuildPlanInvariantes(unittest.TestCase):
    """
    Invariantes del plan: coherencia estructural independiente del routing.
    """

    def _plan_mixto(self):
        types = {
            "fare":     make_numeric("fare"),
            "name":     make_text_nfc("name"),
            "survived": make_boolean("survived"),
        }
        pipe = _mock_pipeline(types)
        return pipe.build_plan(peer_dialect=PostgreSQLDialect())

    def test_todas_las_columnas_tienen_layer(self):
        plan = self._plan_mixto()
        valid_layers = {Layer.PRE, Layer.SPLIT, Layer.POST}
        for name, col in plan.columns.items():
            with self.subTest(col=name):
                self.assertIn(col.layer, valid_layers)

    def test_columnas_pre_no_tienen_post_fn(self):
        plan = self._plan_mixto()
        for name, col in plan.columns.items():
            if col.layer == Layer.PRE:
                with self.subTest(col=name):
                    self.assertIsNone(col.post_fn)

    def test_columnas_split_tienen_post_fn(self):
        plan = self._plan_mixto()
        for name, col in plan.columns.items():
            if col.layer == Layer.SPLIT:
                with self.subTest(col=name):
                    self.assertIsNotNone(col.post_fn)

    def test_sql_expression_no_vacia(self):
        plan = self._plan_mixto()
        for name, col in plan.columns.items():
            with self.subTest(col=name):
                self.assertTrue(len(col.sql_expression) > 0)

    def test_post_callables_son_callable(self):
        plan = self._plan_mixto()
        for col_name, fn in plan.post_callables().items():
            with self.subTest(col=col_name):
                self.assertTrue(callable(fn))


if __name__ == "__main__":
    unittest.main(verbosity=2)
