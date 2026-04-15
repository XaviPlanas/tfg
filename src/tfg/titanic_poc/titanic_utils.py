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
    url_mysql = f"{MYSQL['user']}:{MYSQL['password']}@{MYSQL['host']}:{MYSQL['port']}/{MYSQL['db']}"
    
    # PostgreSQL
    POSTGRES = {
        "user": "poc",
        "password": "poc5432",
        "host": "localhost",
        "port": "5432",
        "db": "tfgdb"
    }
    url_postgres = f"{POSTGRES['user']}:{POSTGRES['password']}@{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['db']}"
    
    def __init__(self):
        disable_tracking()  # evita tracking de API por defecto

        self.mysql_engine = self._create_mysql_engine()
        self.postgresql_engine = self._create_postgres_engine()

    # ---------------------------
    # Creación de engines
    # ---------------------------
    def _create_mysql_engine(self):

        return create_engine(f"mysql+mysqlconnector://{self.url_mysql}")

    def _create_postgres_engine(self):
        return create_engine(f"postgresql+psycopg2://{self.url_postgres}")

    # ---------------------------
    # Test conexiones
    # ---------------------------
    def test_connections(self):
        try:
            with self.mysql_engine.connect():
                print(f"Conexión MySQL OK: {self.MYSQL['db']} en {self.MYSQL['host']}")
            with self.postgresql_engin.connect():
                print(f"Conexión PostgreSQL OK: {self.POSTGRES['db']} en {self.POSTGRES['host']}")
        except Exception as e:
            print(f"Error de conexión: {e}")