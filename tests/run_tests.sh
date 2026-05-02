#!/bin/bash
# Ejecutar suite de tests del TFG
# Uso: ./run_tests.sh [verbose]
#
# Variables de entorno opcionales:
#   PYTHONPATH : debe incluir la raíz del proyecto (src/)
#
# Ejemplos:
#   PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"
#   PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py" -v
#   PYTHONPATH=src python3 -m unittest tests.unit.canonical_engine.test_dialects

set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
VERBOSE=${1:-""}

export PYTHONPATH="${ROOT}/src:${PYTHONPATH}"

if [ -n "$VERBOSE" ]; then
    python3 -m unittest discover -s "${ROOT}/tests" -p "test_*.py" -v
else
    python3 -m unittest discover -s "${ROOT}/tests" -p "test_*.py"
fi
