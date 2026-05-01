# Ejemplo de conexión
```   python
from canonical_engine.pipeline import CanonicalPipeline
from data_diff import connect_to_table, diff_tables

# ── 1. Construir y aplicar el plan canónico en cada motor ─────────

pipeline_mysql = CanonicalPipeline(
    connection_uri = "mysql://poc:XXX@localhost/pocdb",
    table_name     = "titanic"
)
pipeline_pg = CanonicalPipeline(
    connection_uri = "postgresql://poc:XXX@localhost/tfgdb",
    table_name     = "titanic")

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
```

diffs = list(diff_tables(table_mysql, table_pg))
print(f"Diferencias tras canonización: {len(diffs)}")
# Resultado esperado para datasets idénticos: 0
"""

# Reglas de canonización
| ID | Regla | Compatibilidad | Relevancia | Pérdida_datos | Metadato_adicional |
|----|------|---------------|------------|---------------|--------------------|
| 1 | Cast boolean a entero 0/1 | 9 | 10 | 0 | Sí |
| 2 | Cast boolean a string 'true'/'false' | 8 | 9 | 0 | No |
| 3 | Normalizar TINYINT/BIT a BOOLEAN | 10 | 9 | 0 | Sí |
| 4 | TIMESTAMP a UTC | 10 | 10 | 1 | Sí |
| 5 | Fechas a ISO 8601 string | 9 | 10 | 2 | Sí |
| 6 | DATE a YYYY-MM-DD | 9 | 8 | 0 | No |
| 7 | Unificar precisión TIMESTAMP | 7 | 8 | 3 | Sí |
| 8 | YEAR a INTEGER | 8 | 7 | 0 | No |
| 9 | INTERVAL a ISO 8601 duration | 6 | 7 | 1 | Sí |
| 10 | Enteros a BIGINT | 9 | 9 | 0 | Sí |
| 11 | DECIMAL a precisión fija | 8 | 8 | 4 | Sí |
| 12 | FLOAT a DOUBLE | 7 | 7 | 5 | No |
| 13 | UNSIGNED a signed | 8 | 6 | 2 | Sí |
| 14 | BIT a entero | 9 | 8 | 0 | No |
| 15 | TRIM y normalizar whitespace | 8 | 9 | 1 | Sí |
| 16 | LOWERCASE | 9 | 8 | 6 | Sí |
| 17 | Normalizar collation | 7 | 9 | 5 | Sí |
| 18 | '' a NULL | 10 | 9 | 3 | Sí |
| 19 | Encoding a UTF-8 | 6 | 7 | 4 | Sí |
| 20 | UUID a string lowercase | 9 | 9 | 0 | No |
| 21 | JSON canonizado | 7 | 9 | 2 | Sí |
| 22 | ARRAY a JSON ordenado | 6 | 8 | 1 | Sí |
| 23 | BINARY a HEX | 8 | 7 | 0 | No |
| 24 | ENUM a string | 9 | 8 | 0 | Sí |
| 25 | GEOMETRY a WKT | 5 | 6 | 2 | Sí |
| 26 | XML canonizado | 6 | 5 | 3 | Sí |
| 27 | COALESCE NULL | 8 | 4 | 7 | Sí |
| 28 | Hash fila pre-canonización | 10 | 10 | 0 | Sí |
| 29 | Normalizar comillas/escaping | 7 | 6 | 1 | No |
| 30 | Columnas a snake_case | 9 | 8 | 0 | Sí |

# v2
La canonización se separa en dos fases antes y después del datadiff:
	- PRE (canonización)
	- POST ( estandarización ) : se aplican las funciones que no pueden resolver remotamente los dos motores comparados y que por lo tanto requieren de un fallback a Python
# v1 
**Propósito**: ( por inferencia ) -> convertimos a un modelo estandard los tipos
**Módulo**: src/canonical_engine/
**Variantes**: autotipado y fichero de configuración
El autotipado se utiliza como autoasistente previo al fichero de configuración 
canonical_engine/ : materializar una vista con la versión canónica de las columnas.
	==pipeline.py== : Colección de clases de datos Column, Plan y Pipeline
	==engine.py:==  implementa funciones python de fallback.

Pasos:
 1. Construimos planes canónicos por motor : CanonicalPipeline().build_plan()
	 1. ./introspection/ : Se detecta tipo (type.py) y se abstrae a tipo canónico (type_mapper.py)
	 2. cada tipo tiene asociados funciones base (base.py)
	 3. [[Transformaciones]]: para cada dialecto ./dialect/ se definen las SQL equivalentes a las funciones base, cuando no existe el equivalente se genera un *UnsupportedTransformation* y se vincula a una fallback en python que se gestiona desde engine.py.
 2. Revisamos el plan CanonicalPipeline().report()
 3. Creamos las vistas en cada motor CanonicalPipeline().apply_plan()
 4. Buscamos diferencias con diff-data  sobre las vistas canónicas : diff_tables()

Crear un paso que no acceda externamente si no desde el TableSegment.

![[Canonización en 2 etapas (PRE y POST).png]]



