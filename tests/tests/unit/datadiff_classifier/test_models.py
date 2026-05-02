"""
tests/unit/datadiff_classifier/test_models.py

Tests de los dataclasses del módulo datadiff_classifier/models.py:
  - DiffRow        : estructura de par de filas divergentes
  - DiffClassification : resultado de clasificación
  - SegmentStructure   : fingerprint SHA-256 de esquema
  - DiffEvent          : diferencia a nivel de columna

Énfasis en SegmentStructure.schema_version():
  El fingerprint es el mecanismo de detección de drift de esquema.
  Sus invariantes son: determinismo, sensibilidad a cambios,
  independencia del orden de columnas declaradas.
"""

import sys, os, unittest, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from datadiff_classifier.models import (
    DiffRow, DiffClassification, DiffCategory, DiffAction,
    DiffEvent, SegmentStructure,
)
from tests.conftest import diff_update, diff_insert, diff_delete


class TestDiffRow(unittest.TestCase):

    def test_construccion_update(self):
        row = diff_update(1, {"name": "jack"}, {"name": "JACK"})
        self.assertEqual(row.key, 1)
        self.assertIsNotNone(row.row_a)
        self.assertIsNotNone(row.row_b)

    def test_construccion_insert(self):
        row = diff_insert(2, {"name": "rose"})
        self.assertIsNone(row.row_a)
        self.assertIsNotNone(row.row_b)

    def test_construccion_delete(self):
        row = diff_delete(3, {"name": "smith"})
        self.assertIsNotNone(row.row_a)
        self.assertIsNone(row.row_b)

    def test_source_a_y_b_distintos(self):
        row = diff_update(1, {}, {})
        self.assertNotEqual(row.source_a, row.source_b)


class TestDiffClassification(unittest.TestCase):

    def _make(self, categoria=DiffCategory.SEMANTICALLY_EQUIVALENT):
        return DiffClassification(
            key                 = 1,
            accion              = DiffAction.UPDATE,
            categoria           = categoria,
            confianza           = 0.95,
            columnas_afectadas  = ["name"],
            explicacion         = "Diferencia de capitalización.",
            normalizacion_sugerida = "LOWER(name)",
            row_a               = {"name": "jack"},
            row_b               = {"name": "JACK"},
        )

    def test_to_dict_serializable(self):
        d = self._make().to_dict()
        # Debe ser serializable sin errores
        s = json.dumps(d)
        self.assertIsInstance(s, str)

    def test_to_dict_categoria_es_string(self):
        d = self._make(DiffCategory.SEMANTICALLY_DIFFERENT).to_dict()
        self.assertIsInstance(d["categoria"], str)
        self.assertEqual(d["categoria"], "SEMANTICALLY_DIFFERENT")

    def test_to_dict_accion_es_string(self):
        d = self._make().to_dict()
        self.assertIsInstance(d["accion"], str)

    def test_to_json_es_json_valido(self):
        j = self._make().to_json()
        parsed = json.loads(j)
        self.assertIn("categoria", parsed)

    def test_todas_las_categorias(self):
        for cat in DiffCategory:
            with self.subTest(cat=cat):
                d = self._make(cat)
                self.assertEqual(d.categoria, cat)


class TestSegmentStructure(unittest.TestCase):
    """Tests del fingerprint SHA-256 de esquema."""

    def _seg(self, cols, pk="PassengerId"):
        return SegmentStructure(columnas=cols, pk=pk)

    def test_schema_version_es_hex_64(self):
        v = self._seg(["a", "b", "c"]).schema_version()
        self.assertEqual(len(v), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in v))

    def test_determinismo(self):
        """El mismo esquema siempre produce el mismo hash."""
        cols = ["PassengerId", "Name", "Fare", "Survived"]
        v1   = self._seg(cols).schema_version()
        v2   = self._seg(cols).schema_version()
        self.assertEqual(v1, v2)

    def test_orden_columnas_irrelevante(self):
        """El hash no depende del orden de declaración de columnas."""
        v1 = self._seg(["a", "b", "c"]).schema_version()
        v2 = self._seg(["c", "a", "b"]).schema_version()
        self.assertEqual(v1, v2)

    def test_sensible_a_nueva_columna(self):
        """Añadir una columna cambia el hash."""
        v1 = self._seg(["a", "b"]).schema_version()
        v2 = self._seg(["a", "b", "c"]).schema_version()
        self.assertNotEqual(v1, v2)

    def test_sensible_a_cambio_pk(self):
        """Cambiar la PK cambia el hash."""
        v1 = SegmentStructure(["a", "b"], pk="id").schema_version()
        v2 = SegmentStructure(["a", "b"], pk="uuid").schema_version()
        self.assertNotEqual(v1, v2)

    def test_sensible_a_nombre_columna(self):
        """Un carácter distinto en el nombre cambia el hash."""
        v1 = self._seg(["name"]).schema_version()
        v2 = self._seg(["NAME"]).schema_version()
        self.assertNotEqual(v1, v2)

    def test_deriva_detectable(self):
        """
        Simulación de drift: el esquema en producción añade una columna.
        El sistema puede detectarlo comparando hashes entre ejecuciones.
        """
        hash_v1 = self._seg(["a", "b", "c"]).schema_version()
        # ... tiempo después, se añade columna 'd' ...
        hash_v2 = self._seg(["a", "b", "c", "d"]).schema_version()
        # Detección: hashes distintos → schema drift
        self.assertNotEqual(hash_v1, hash_v2)


class TestDiffEvent(unittest.TestCase):

    def test_construccion(self):
        ev = DiffEvent(
            key      = 1,
            columna  = "name",
            valor_a  = "jack",
            valor_b  = "JACK",
        )
        self.assertEqual(ev.columna, "name")
        self.assertEqual(ev.valor_a, "jack")
        self.assertEqual(ev.valor_b, "JACK")


if __name__ == "__main__":
    unittest.main(verbosity=2)
