import json

from data_diff import connect_to_table, diff_tables

from sqlalchemy import inspect, Engine

from tfg.datadiff_classifier.classifier import DiffClassifier, DiffRow
from tfg.datadiff_classifier.models        import DiffRow, SegmentStructure

#from tfg.datadiff_classifier.report     import generate_report

from tfg.canonical_engine.pipeline      import CanonicalPipeline
from tfg.canonical_engine.config.loader import CanonicalConfigLoader
from tfg.canonical_engine.segment     import CanonicalSegment  
from tfg.canonical_engine.post_canonicalizer import PostCanonicalizer
from tfg.canonical_engine.plan        import CanonicalPlan

import logging
from tfg.logging_config import setup_logging, timed

from tfg.canonical_engine.post_canonicalizer import PostCanonicalizer
logger = logging.getLogger("tfg.canonical_engine.canonize")

from titanic_poc.titanic_utils import Config
conf = Config()

MYSQL_URI = conf.getConnectionString(Config.MYSQL)
PG_URI    = conf.getConnectionString(Config.POSTGRES)

MYSQL_URI_DDIFF = conf.getConnectionString(Config.MYSQL, datadiff = True)
PG_URI_DDIFF = conf.getConnectionString(Config.POSTGRES, datadiff = True)


def canonize_pipeline(pipeline_mysql, pipeline_pg)->[CanonicalPlan, CanonicalPlan]:
    
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

def compare_plans(plan_mysql, plan_pg) -> tuple[list, list]:
    # Aquí se podrían comparar los planes para detectar si hay reglas de canonización comunes
    # o si uno de los planes es un subconjunto del otro, lo que indicaría que una canonización
    # es más completa que la otra.
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

def normalize_plans(diffs_pre, plan_mysql, plan_pg, all_cols) -> list[DiffRow]:
    # Aquí se podrían aplicar las transformaciones POST (Python) a ambos lados
    # para resolver diferencias que no pueden ser canonizadas en SQL.
    # Por ejemplo, si MySQL no soporta NFC y eso genera falsos positivos, podríamos aplicar una transformación de normalización Unicode en Python después de obtener los resultados de data-diff.
    # Los DiffRow cuyas diferencias desaparecen tras POST son falsos positivos de la capa

    post_can = PostCanonicalizer(plan_mysql, plan_pg)
    print(post_can.report())

    metadata = SegmentStructure(columnas=all_cols, pk="PassengerId")
    classifier = DiffClassifier()
    diff_rows  = classifier.parse_to_diffrows(
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

def classify_diffs(diffs_can):
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
    results = classifier.classify_row_by_row(diff_rows)
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