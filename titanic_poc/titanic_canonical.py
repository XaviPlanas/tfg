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

import json
import sys

from data_diff import connect_to_table, diff_tables

from sqlalchemy import inspect, Engine

from tfg.datadiff_classifier.classifier import DiffClassifier, DiffRow
from tfg.datadiff_classifier.models        import DiffRow, SegmentStructure

from tfg.datadiff_classifier.report     import generate_report

from tfg.canonical_engine.pipeline      import CanonicalPipeline
from tfg.canonical_engine.config.loader import CanonicalConfigLoader
from tfg.canonical_engine.segment     import CanonicalSegment  
from tfg.canonical_engine.post_canonicalizer import PostCanonicalizer
from tfg.canonical_engine.plan        import CanonicalPlan

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

###################################
# Métodos con los pasos del pipeline
###################################
def __header_print(title):
    logger.debug(f"{SEPARATOR}")
    logger.info(f"  {title}")
    logger.debug(f"{SEPARATOR}")

def paso1_load_config():
    __header_print("PASO 1/6 — Cargar configuración del canonizador")

    with timed (logger, "Cargar configuración YAML") : 
        loaded = CanonicalConfigLoader.from_file(CFG_FILE)
    logger.info(loaded.report())
    return loaded

def paso2_canonizacion()-> tuple[CanonicalPlan, CanonicalPlan]:
    __header_print("PASO 2/6 — Construir y aplicar planes de canonización")
    logger.info("Creación pipelines de motores y peers")
    pipeline_mysql = CanonicalPipeline( connection_uri = MYSQL_URI, table_name     = "titanic" )
    pipeline_pg = CanonicalPipeline( connection_uri = PG_URI, table_name     = "titanic_modified" ) 
    # Cada pipeline resuelve el dialecto del otro como peer
    peer_pg    = pipeline_mysql.resolve_peer_dialect(PG_URI)
    peer_mysql = pipeline_pg.resolve_peer_dialect(MYSQL_URI)
    
    logger.info("MySQL:")
    
    with timed (logger, "MySQL Cannonical Pipeline", level="INFO") : 
     
        plan_mysql = pipeline_mysql.build_plan(peer_dialect=peer_pg)
        plan_mysql.report()
        #pipeline_mysql.apply_plan(plan_mysql)

    logger.info("PostgreSQL:")
    with timed (logger, "PostgreSQL Cannonical Pipeline") : 

        plan_pg = pipeline_pg.build_plan(peer_dialect=peer_mysql)
        logger.info(plan_pg.report())
        #pipeline_pg.apply_plan(plan_pg)
    logger.info(
        "Planes construidos: mysql_post=%d  pg_post=%d",
        len(plan_mysql.post_callables()),
        len(plan_pg.post_callables()),
    )
    return plan_mysql, plan_pg

def paso3_compare(plan_mysql, plan_pg) -> tuple[list, list]:
    __header_print("PASO 3/6 — Comparación con data-diff sobre vistas canónicas")

    with timed ( logger, "[Baseline] Resultado comparación sin canonización:", level = 'INFO') :
        table1_raw = connect_to_table(
            MYSQL_URI_DDIFF, "titanic", "PassengerId" #TODO: la PK es el valor _normalizado! 
                                                    #Debería ser el valor anterior y que el sistema lo detectara
        )
        table2_raw = connect_to_table(PG_URI_DDIFF, "titanic_modified", "PassengerId")
    
    #TODO: Validación cruzada de cols ANTES de empezar con los esquemas
    #      def_validate_schema_match(table1.cols, table2.cols):
    #      ¿comparar nombres o sólo igualdad de elementos?
    # Mientras asumimos que table1.cols == table2.cols y tomamos table1.cols
   
    #table1_cols = conf.get_all_column_names( conf.get_mysql_engine(), 'titanic' )
        table1_cols = table1_raw.get_schema().keys() # Queremos usar TableSegment en lugar de inspección SQLAlchemy

        diffs_raw = list(diff_tables(table1 = table1_raw,
                                    table2 = table2_raw,
                                    extra_columns= table1_cols                    
                                    ))
    logger.info(f"Diferencias detectadas (diffdata de tablas sin canonizar): {len(diffs_raw)}")

    # with timed (logger, "[Canonizado] Con vistas canónicas:", level = 'INFO') :
    #     table_mysql_can = connect_to_table(
    #         MYSQL_URI_DDIFF, "titanic_canonical", "passenger_id"
    #     )
    #     table_pg_can = connect_to_table(
    #         PG_URI_DDIFF, "titanic_modified_canonical", "passenger_id"
    #     )
    #     diffs_can = list(diff_tables(table_mysql_can, table_pg_can))
    #with timed (logger, "[CanonicalSegment] - Fase PRE:", level = 'INFO') :
    cs_mysql = CanonicalSegment(plan_mysql, MYSQL_URI_DDIFF, "titanic", "PassengerId")
    cs_pg    = CanonicalSegment(plan_pg,    PG_URI_DDIFF,    "titanic_modified", "PassengerId")
    
    diffs_pre = list(diff_tables(cs_mysql.table_segment, cs_pg.table_segment ))
    
    logger.info(f"Número de diferencias detectadas (PRE):   {len(diffs_pre)}")

    if diffs_raw:
        pct = (len(diffs_raw) - len(diffs_pre)) / len(diffs_raw) * 100
    logger.info(f"Reducción de falsos positivos por la canonización PRE : {len(diffs_raw)-len(diffs_pre)} ({pct:.1f}%)")

    return diffs_raw, diffs_pre

def paso4_normalizacion(
    diffs_pre: list,
    plan_mysql,
    plan_pg,
    all_cols:  list,
) -> list[DiffRow]:
    """
    Convierte las tuplas raw de data-diff en DiffRow y aplica
    las transformaciones POST (Python) a ambos lados.

    Los DiffRow cuyas diferencias desaparecen tras POST son
    falsos positivos de la capa SQL (e.g. NFC no soportado en MySQL).
    Se eliminan antes de llegar al clasificador LLM.
    """
    __header_print("PASO 4/6 — PostCanonicalizer (transforms Python post-diff)")

    post_can = PostCanonicalizer(plan_mysql, plan_pg)
    print(post_can.report())

    metadata = SegmentStructure(columnas=all_cols, pk="PassengerId")
    classifier = DiffClassifier()
    diff_rows  = classifier.parse_diff_results(
        diffs    = iter(diffs_pre),
        metadata = metadata,
    )

    print(f"\n  DiffRows entrada PostCanonicalizer: {len(diff_rows)}")
    remaining, resolved = post_can.apply_batch(diff_rows)
    print(f"  Falsos positivos eliminados en POST: {len(resolved)}")
    print(f"  Diferencias reales para el LLM:     {len(remaining)}")

    if len(diff_rows):
        pct = len(resolved) / len(diff_rows) * 100
        print(f"  Reducción POST: {len(resolved)} ({pct:.1f}%)")

    logger.info(
        "POST: entrada=%d  resueltos=%d  restantes=%d",
        len(diff_rows), len(resolved), len(remaining),
    )
    return remaining

def paso5_classify(diffs_can):
    __header_print("PASO 4 — Clasificación IA de diferencias restantes")

    if not diffs_can:
        logger.info("\n  No hay diferencias que clasificar: la canonización ha eliminado todos los falsos positivos.")
        return []
    
    schema_context = """
    Tabla: titanic (dataset Kaggle, 891 pasajeros del Titanic)
    Motor A: MySQL 8.0
    Motor B: PostgreSQL 15
    Columnas canonizadas: fare (ROUND 3 dec), name (NFC+lower+trim),
                          sex (lower+trim), survived (0/1), cabin (lower+trim)
    """

    classifier = DiffClassifier(schema_context=schema_context)

    diff_rows = [
        DiffRow(
            key      = diff.key,
            row_a    = diff.row_a,
            row_b    = diff.row_b,
            source_a = "mysql://pocdb/titanic_canonical",
            source_b = "postgresql://tfgdb/titanic_canonical",
        )
        for diff in diffs_can
    ]

    logger.info(f"\n  Clasificando {len(diff_rows)} diferencias...")
    results = classifier.classify_batch(diff_rows)
    report  = generate_report(results)

    print(f"\n  Resumen de clasificación:")
    for category, count in report["resumen"].items():
        print(f"    {category:<35} {count} filas")

    print(f"\n  Tasa de falsos positivos residual: "
          f"{report['tasa_falsos_positivos']:.1%}")

    if report["requieren_revision"]:
        print(f"\n  Requieren revisión manual:")
        for item in report["requieren_revision"]:
            print(f"    PassengerId {item['key']}: {item['explicacion']}")

    if report["normalization_hints"]:
        print(f"\n  Sugerencias de normalización adicional:")
        for col, hints in report["normalization_hints"].items():
            for hint in hints:
                print(f"    {col}: {hint}")

    # Guardar informe completo
    with open("output_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"Informe guardado en: output_report.json")

    return results

#----- main ----------------------------
def main():
    logger.debug(f"{SEPARATOR}")
    logger.debug("  MOTOR DE CANONIZACIÓN — CASO DE USO TITANIC")
    logger.debug(f"{SEPARATOR}")

    #loaded                  = paso1_load_config()   # Paso 1 - Cargar Configuración 
    #plan_mysql, plan_pg     = paso2_build_and_apply_plans(loaded) # Paso 2 - Canonizar en cada motor
    #diffs_raw, diffs_can    = paso3_compare_with_datadiff() # Paso 3 - data-diff contra datasets canonizados
    #results                 = paso4_classify_differences(diffs_can) # Paso 4 - clasificador IA

    # Paso 1
    paso1_load_config()

    # Paso 2: planes con split PRE/POST simétrico
    plan_mysql, plan_pg = paso2_canonizacion()

    # Paso 3: data-diff con vistas efímeras (CanonicalSegment)
    diffs_raw, diffs_pre = paso3_compare(plan_mysql, plan_pg)

    # Necesitamos los nombres de columna para parse_diff_results

    schema_cols = list( connect_to_table(MYSQL_URI_DDIFF, "titanic", "PassengerId").get_schema().keys() )

    # Paso 4: PostCanonicalizer — elimina falsos positivos POST
    diff_rows = paso4_normalizacion( diffs_pre, plan_mysql, plan_pg, schema_cols )

    # Paso 5: LLM solo recibe diferencias reales
    paso5_classify(diff_rows)

    # Resumen de reducción total
    total_reduction = len(diffs_raw) - len(diff_rows)
    print(f"\n{'=' * 60}")
    print("  RESUMEN DE REDUCCIÓN DE FALSOS POSITIVOS")
    print(f"{'─' * 60}")
    print(f"  Sin canonización (baseline):  {len(diffs_raw):4d} diffs")
    print(f"  Tras PRE  (SQL, data-diff):   {len(diffs_pre):4d} diffs")
    print(f"  Tras POST (Python, pre-LLM):  {len(diff_rows):4d} diffs")
    print(f"  Reducción total:              "
          f"{total_reduction} diffs "
          f"({total_reduction/len(diffs_raw)*100:.1f}%)"
          if diffs_raw else "  (sin baseline)")
    print(f"{'=' * 60}\n")

    logger.info(
        "Pipeline completado: raw=%d pre=%d post=%d",
        len(diffs_raw), len(diffs_pre), len(diff_rows),
    )
    logger.debug(f"{SEPARATOR}")
    logger.debug("  EJECUCIÓN COMPLETADA")
    logger.debug(f"{SEPARATOR}")

if __name__ == "__main__":
    main()