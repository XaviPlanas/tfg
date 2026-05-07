###################################
# Escenario: 
#   Pipeline completo de canonización y comparación.
#   Ejecuta los cuatro pasos en secuencia:
#       paso 1. Cargar configuración canónica
#       paso 2. Construir y aplicar el plan de canonización en cada motor
#       paso 3. PRE (Canonización): Comparar con data-diff sobre las vistas canónicas
#       paso 4. POST (Normalización) : Aplicar transformaciones pendientes. 
#       paso 5. Clasificar las diferencias con el clasificador IA
#       paso 6. Generar informe detallado de resultados
###################################

# import json
# import sys

from data_diff import connect_to_table, diff_tables

# from sqlalchemy import inspect, Engine

# from tfg.datadiff_classifier.classifier import DiffClassifier, DiffRow
# from tfg.datadiff_classifier.models        import DiffRow, SegmentStructure

from tfg.canonical_engine.pipeline      import CanonicalPipeline
from tfg.canonical_engine.config.loader import CanonicalConfigLoader
# from tfg.canonical_engine.segment     import CanonicalSegment  
# from tfg.canonical_engine.post_canonicalizer import PostCanonicalizer
from tfg.canonical_engine.plan        import CanonicalPlan
from tfg.canonical_engine.launcher    import ( canonize_pipeline ,
                                              compare_plans,
                                              normalize_plans,
                                              classify_diffs,
)
from tfg.datadiff_classifier.classifier import DiffClassifier

from .titanic_utils import Config

import logging
from tfg.logging_config import setup_logging, timed

###################################
# Globales y configuración
###################################

setup_logging(level="DEBUG")

# Usar siempre el nombre explícito del módulo, no __name__
# porque si se ejecuta como __main__, el logger queda fuera del árbol
logger = logging.getLogger("tfg.titanic_poc.titanic_canonical")
logger.debug("Cargando locales y configuración")

SEPARATOR = "─" * 60

conf = Config()
MYSQL_URI = conf.getConnectionString(Config.MYSQL)
PG_URI    = conf.getConnectionString(Config.POSTGRES)

MYSQL_URI_DDIFF = conf.getConnectionString(Config.MYSQL, datadiff = True)
PG_URI_DDIFF = conf.getConnectionString(Config.POSTGRES, datadiff = True)

CFG_FILE  = "titanic_poc/titanic_canonical.yaml" # ejecutando desde tfg/src


def main():

    logger.debug("  MOTOR DE CANONIZACIÓN — CASO DE USO TITANIC")
    
    #       paso 1. Cargar configuración canónica
    with timed (logger, "Cargar configuración YAML") : 
        loaded = CanonicalConfigLoader.from_file(CFG_FILE)
    logger.info(loaded.report())

    #       paso 2. Construir y aplicar el plan de canonización en cada motor  
    
    with timed (logger, "[PRE] Canonizador"):
        pipeline_mysql = CanonicalPipeline( connection_uri = MYSQL_URI, table_name     = "titanic" )
        pipeline_pg = CanonicalPipeline( connection_uri = PG_URI, table_name     = "titanic_modified" ) 
        plan_mysql, plan_pg = canonize_pipeline(pipeline_mysql,pipeline_pg)

    # Paso 3: data-diff con vistas efímeras (CanonicalSegment)
    with timed (logger, "Comparación con data-diff"):
        diffs_raw, diffs_pre = compare_plans(plan_mysql, plan_pg)
    
    with timed (logger, "[POST] Normalizador") :
        # Paso 4: PostCanonicalizer — elimina falsos positivos POST
        # Necesitamos los nombres de columna para parse_diff_results
        schema_cols = list( connect_to_table(MYSQL_URI_DDIFF, "titanic", "PassengerId").get_schema().keys() )

        diff_rows = normalize_plans( diffs_pre, plan_mysql, plan_pg, schema_cols )
        
    with timed ( logger, "Clasificación de diferencias") :
        # Paso 5: LLM solo recibe diferencias reales
        clasificador = DiffClassifier()
        clasificaciones = clasificador.classify_row_by_row(diff_rows)
        
    with timed (logger, "Reporting de resultados") : 
        # paso 6. Generar informe detallado de resultados
        clasificador.report_classifications(clasificaciones)
    
    # # Resumen de reducción total
    # total_reduction = len(diffs_raw) - len(diff_rows)
    # print(f"\n{'=' * 60}")
    # print("  RESUMEN DE REDUCCIÓN DE FALSOS POSITIVOS")
    # print(f"{'─' * 60}")
    # print(f"  Sin canonización (baseline):  {len(diffs_raw):4d} diffs")
    # print(f"  Tras PRE  (SQL, data-diff):   {len(diffs_pre):4d} diffs")
    # print(f"  Tras POST (Python, pre-LLM):  {len(diff_rows):4d} diffs")
    # print(f"  Reducción total:              "
    #       f"{total_reduction} diffs "
    #       f"({total_reduction/len(diffs_raw)*100:.1f}%)"
    #       if diffs_raw else "  (sin baseline)")
    # print(f"{'=' * 60}\n")

    # logger.info(
    #     "Pipeline completado: raw=%d pre=%d post=%d",
    #     len(diffs_raw), len(diffs_pre), len(diff_rows),
    # )
    # logger.debug(f"{SEPARATOR}")
    # logger.debug("  EJECUCIÓN COMPLETADA")
    # logger.debug(f"{SEPARATOR}")

if __name__ == "__main__":
    main()