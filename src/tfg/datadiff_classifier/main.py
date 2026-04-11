from tfg.datadiff_classifier.classifier import DiffClassifier
from tfg.datadiff_classifier.models import DiffRow


mysql_row=('-', ('269', '269', '1', '1', 'Graham, Mrs. William Thompson (Edith Junkins)', 'female', '58.000', '0', '1', 'PC 17582', '153.463', 'C125', 'S'))
pgsql_row=('+', ('269', '269', '1', '1', 'Graham, Mrs. William Thompson (Edith Junkins)_mod', 'female', '58.000', '0', '1', 'PC 17582', '153.463', 'C125', 'S'))

clasificador_diff = DiffClassifier("diff 1")
clasificador_diff.classify( DiffRow(mysql_row[1], pgsql_row[1],'mySQL', 'postgreSQL'))