from sqlalchemy import create_engine
from data_diff import disable_tracking

class Config:
    DEBUG = True

    # Dataset
    DATASET = {
        'raw': {
            'file': 'data/raw/titanic.csv',
            'table': 'titanic'
        },
        'modified': {
            'file': 'data/modified/titanic_modified.csv',
            'table': 'titanic_modified'
        }
    }

    # MySQL
    MYSQL = {
        "user": "poc",
        "password": "poc3306",
        "host": "localhost",
        "port": "3306",
        "db": "pocdb"
    }

    # PostgreSQL
    POSTGRES = {
        "user": "poc",
        "password": "poc5432",
        "host": "localhost",
        "port": "5432",
        "db": "tfgdb"
    }

    def __init__(self):
        disable_tracking()  # evita tracking de API por defecto

        self.mysql_engine = self._create_mysql_engine()
        self.postgres_engine = self._create_postgres_engine()

    # ---------------------------
    # Creación de engines
    # ---------------------------
    def _create_mysql_engine(self):
        cfg = self.MYSQL
        url = f"{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['db']}"
        return create_engine(f"mysql+mysqlconnector://{url}")

    def _create_postgres_engine(self):
        cfg = self.POSTGRES
        url = f"{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['db']}"
        return create_engine(f"postgresql+psycopg2://{url}")

    # ---------------------------
    # Test conexiones
    # ---------------------------
    def test_connections(self):
        try:
            with self.mysql_engine.connect():
                print(f"Conexión MySQL OK: {self.MYSQL['db']} en {self.MYSQL['host']}")
            with self.postgres_engine.connect():
                print(f"Conexión PostgreSQL OK: {self.POSTGRES['db']} en {self.POSTGRES['host']}")
        except Exception as e:
            print(f"Error de conexión: {e}")