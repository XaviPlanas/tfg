from .titanic_utils import Config
from data_diff import connect_to_table, diff_tables
from sqlalchemy import inspect, Engine 

from tfg.datadiff_classifier.classifier import DiffClassifier
from tfg.datadiff_classifier.models import DiffRow

cfg = Config()

table_raw = Config.dataset["raw"]["table"]
table_modified = Config.dataset["modified"]["table"]
primary_key = "PassengerId"

mysql_metadata = inspect(Config.mysql_engine)
mysql_all_columns=[col["name"] for col in mysql_metadata.get_columns(table_raw)]
#mysql_all_columns = []

table_mysql = connect_to_table(
    #f"{Config.MYSQL["dialect"]}://{Config.mysql_engine_url}",
    cfg.getURL(Config.MYSQL),
    table_name=table_raw,
    key_columns=primary_key,
    extra_columns = mysql_all_columns
)

table_pg = connect_to_table(
    #f"{Config.MYSQL["dialect"]}://{Config.postgres_engine_url}",
    cfg.getURL(Config.POSTGRES),
    table_name=table_modified,
    key_columns=("PassengerId"),
    extra_columns = mysql_all_columns
)

added = 0
removed = 0
total = 0
left = {}
right = {}

#for diff in diff_tables(table_mysql, table_pg, extra_columns=mysql_all_columns):
for diff in diff_tables(table_mysql, table_pg):
   total+=1
   d = dict(zip(mysql_all_columns, diff[1][1:]))
   if diff[0] == '+' :
      added+=1
      right[d[primary_key]]=d 
      Config.DEBUG and print(f"postgreSQL {d}")
   elif diff[0] == '-' :
      removed+=1
      left[d[primary_key]]=d 
      Config.DEBUG and print(f"mySQL {d}")

   Config.DEBUG and print(diff)

if Config.DEBUG : 
   print(60*'=')
   print("Cambios detectados en total:", total)
   print("Insertados:", added)
   print("Eliminados:", removed)
   print(60*'-')
   print(f"\nLeft: {left}\nRight: {right}")
   print(60*'=')

filas_a_comparar = DiffRow(primary_key, left, right, "mysql", "postgresql")
clasificador_diff = DiffClassifier("diff 1")
resultado = clasificador_diff.classify(filas_a_comparar)
print(resultado)

# filas_a_comparar = DiffRow(primary_key, left["269"], right["269"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])

# filas_a_comparar = DiffRow(primary_key, left["838"], right["838"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])

# filas_a_comparar = DiffRow(primary_key, left["84"], right["84"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])