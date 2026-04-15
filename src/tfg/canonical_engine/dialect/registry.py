from .base import BaseDialect, UnsupportedTransformation
from .mysql import MySQLDialect
from .postgresql import PostgreSQLDialect


class DialectRegistry:
    """
    Registro central de dialectos disponibles.
    Mapea el nombre de dialecto que devuelve SQLAlchemy
    (engine.dialect.name) a su implementación concreta.

    El registro es el único lugar donde se instancian los dialectos,
    lo que garantiza que el resto del sistema nunca depende de una
    clase concreta sino siempre de la interfaz BaseDialect.
    """

    _registry: dict[str, type[BaseDialect]] = {
        "mysql":             MySQLDialect,
        "postgresql":        PostgreSQLDialect,
    }

    @classmethod
    def get(cls, dialect_name: str) -> BaseDialect:
        """
        Devuelve una instancia del dialecto correspondiente
        al nombre recibido. Lanza DialectNotFound si el motor
        no tiene implementación registrada.
        """
        dialect_class = cls._registry.get(dialect_name.lower())

        if dialect_class is None:
            available = ", ".join(cls._registry.keys())
            raise DialectNotFound(
                f"Dialecto '{dialect_name}' no registrado. "
                f"Dialectos disponibles: {available}"
            )

        return dialect_class()

    @classmethod
    def register(cls, dialect_name: str, dialect_class: type[BaseDialect]):
        """
        Registra un dialecto externo sin modificar el código base.
        Permite extender el sistema con nuevos motores en tiempo
        de ejecución, por ejemplo desde un plugin o desde los
        scripts de configuración de la PoC.

        Uso:
            from canonical_engine.dialect.registry import DialectRegistry
            from my_dialects import SnowflakeDialect

            DialectRegistry.register("snowflake", SnowflakeDialect)
        """
        if not issubclass(dialect_class, BaseDialect):
            raise TypeError(
                f"{dialect_class.__name__} debe heredar de BaseDialect"
            )
        cls._registry[dialect_name.lower()] = dialect_class

    @classmethod
    def available(cls) -> list[str]:
        """
        Devuelve la lista de dialectos registrados.
        Útil para el informe del CanonicalPlan y para
        la validación de configuraciones YAML.
        """
        return list(cls._registry.keys())


class DialectNotFound(Exception):
    """
    Se lanza cuando el motor detectado por SQLAlchemy no tiene
    implementación en el registro. La solución es implementar
    BaseDialect para ese motor y registrarlo con
    DialectRegistry.register().
    """
    pass