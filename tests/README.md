# Sistema de Tests — tfg-canonical-engine

## Ejecución rápida

```bash
# Desde la raíz del proyecto
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"

# Con verbosidad completa
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py" -v

# Un fichero concreto
PYTHONPATH=src python3 -m unittest tests.unit.canonical_engine.test_dialects
```

## Estructura

```
tests/
├── conftest.py                              # Fixtures y factories compartidas
├── unit/
│   ├── canonical_engine/
│   │   ├── test_dialects.py                # MySQLDialect, PostgreSQLDialect, divergencias
│   │   ├── test_types.py                   # Tipos canónicos: to_sql, to_python, paridad
│   │   ├── test_plan.py                    # CanonicalPlan: PRE/SPLIT/POST accesores
│   │   ├── test_pipeline.py                # CanonicalPipeline.build_plan (mock inspector)
│   │   └── test_post_canonicalizer.py      # PostCanonicalizer: apply, apply_batch
│   └── datadiff_classifier/
│       ├── test_models.py                  # DiffRow, SegmentStructure, schema_version
│       ├── test_normalizador.py            # Normalizador determinista sin LLM
│       └── test_python_fallback.py         # PythonFallback: NFC, NFKC, ascii_fold
└── integration/
    └── test_sql_python_parity.py           # Paridad SQL↔Python con SQLite
```

## Dependencias de tests por módulo

| Módulo de test              | Requiere                      | Sin red |
|-----------------------------|-------------------------------|---------|
| test_dialects               | solo stdlib                   | ✓       |
| test_types                  | solo stdlib + unicodedata      | ✓       |
| test_plan                   | solo stdlib                   | ✓       |
| test_pipeline               | sqlalchemy (skipUnless)        | ✓ skip  |
| test_post_canonicalizer     | solo stdlib                   | ✓       |
| test_models                 | solo stdlib + hashlib          | ✓       |
| test_normalizador           | tfg-main/src (skipUnless)      | ✓ skip  |
| test_python_fallback        | solo stdlib + unicodedata      | ✓       |
| test_sql_python_parity      | sqlite3 (stdlib)               | ✓       |

## Resultados esperados (entorno sin BD)

```
Ran 215 tests
OK (skipped=35)
```
