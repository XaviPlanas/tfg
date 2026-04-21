from .titanic_utils import Config
from data_diff import connect_to_table, diff_tables
from sqlalchemy import inspect 

from tfg.datadiff_classifier.classifier import DiffClassifier
from tfg.datadiff_classifier.models import DiffRow

cfg = Config()
IA = True

table_raw = Config.DATASET["raw"]["table"]
table_modified = Config.DATASET["modified"]["table"]
primary_key = "PassengerId"

mysql_metadata = inspect(cfg.mysql_engine)
mysql_all_columns=[col["name"] for col in mysql_metadata.get_columns(table_raw)]
#mysql_all_columns = []

table_mysql = connect_to_table(
    #f"{Config.MYSQL["dialect"]}://{Config.mysql_engine_url}",
    cfg.getConnectionString(Config.MYSQL,datadiff=True ),
    table_name=table_raw,
    key_columns=primary_key,
    extra_columns = mysql_all_columns
)

table_pg = connect_to_table(
    #f"{Config.MYSQL["dialect"]}://{Config.postgres_engine_url}",
    cfg.getConnectionString(Config.POSTGRES,datadiff=True ),
    table_name=table_modified,
    key_columns=("PassengerId"),
    extra_columns = mysql_all_columns
)

total = 0
left = {}
right = {}

#for diff in diff_tables(table_mysql, table_pg, extra_columns=mysql_all_columns):
for diff in diff_tables(table_mysql, table_pg):
   total+=1
   d = dict(zip(mysql_all_columns, diff[1][1:])) # [1::] es necesario si antes se extrae all_columns - pk?
   if diff[0] == '+' :
      right[d[primary_key]]=d 
   elif diff[0] == '-' :
      left[d[primary_key]]=d 

   #Config.DEBUG and print(diff)

right_idx = set(right.keys())
left_idx = set(left.keys())
insert = right_idx - left_idx
delete = left_idx  - right_idx
update = right_idx & left_idx
all_pk = right_idx | left_idx

if Config.DEBUG : 
   print(60*'=')

   print(f"Insertados   (INS) [{len(insert)}] : {insert}"  )
   print(f"Eliminados   (DEL) [{len(delete)}] : {delete}"  )
   print(f"Actualizados (UPD) [{len(update)}] : {update}"  )
   print(60*'-')
   print(f"Cambios detectados por data-diff en total (2UPD + DEL + INS): {total}")
   print(60*'=')

if IA:
   for pk in all_pk:
      row = DiffRow(
         key = pk,
         row_a = right.get(pk),
         row_b = left.get(pk),
         source_a = 'mysql',
         source_b = 'postgresql'
      )
      clasificador = DiffClassifier()
      clasificador.classify_one_row(row)
   
   #filas_a_comparar = DiffRow(primary_key, left, right, "mysql", "postgresql")
   #clasificador_diff = DiffClassifier("diff 1")
   #resultado = clasificador_diff.classify_row_by_row(filas_a_comparar)
   #print(resultado)

# filas_a_comparar = DiffRow(primary_key, left["269"], right["269"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])

# filas_a_comparar = DiffRow(primary_key, left["838"], right["838"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])

# filas_a_comparar = DiffRow(primary_key, left["84"], right["84"], "mysql", "postgresql")
# resultado = clasificador_diff.classify(filas_a_comparar)
# print(resultado["response"])