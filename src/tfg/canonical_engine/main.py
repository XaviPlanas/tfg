from canonical_engine.pipeline import CanonicalPipeline
from data_diff import connect_to_table, diff_tables

# ── 1. Construir y aplicar el plan canónico en cada motor ─────────

pipeline_mysql = CanonicalPipeline(
    connection_uri = "mysql://poc:XXX@localhost/pocdb",
    table_name     = "titanic"
)
pipeline_pg = CanonicalPipeline(
    connection_uri = "postgresql://poc:XXX@localhost/tfgdb",
    table_name     = "titanic"
)

plan_mysql = pipeline_mysql.build_plan()
plan_pg    = pipeline_pg.build_plan()

# Revisar el plan antes de aplicarlo
print(plan_mysql.report())
print(plan_pg.report())

# Aplicar: crea vistas canónicas en cada motor
pipeline_mysql.apply_plan(plan_mysql)
pipeline_pg.apply_plan(plan_pg)

# ── 2. Comparar con data-diff sobre las vistas canónicas ──────────

table_mysql = connect_to_table(
    "mysql://poc:XXX@localhost/pocdb",
    "titanic_canonical",
    "PassengerId",
    columns=["%"]
)
table_pg = connect_to_table(
    "postgresql://poc:XXX@localhost/tfgdb",
    "titanic_canonical",
    "passengerid",
    columns=["%"]
)

diffs = list(diff_tables(table_mysql, table_pg))
print(f"Diferencias tras canonización: {len(diffs)}")
# Resultado esperado para datasets idénticos: 0