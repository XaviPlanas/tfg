from dataclasses import dataclass
from .base import CanonicalType

@dataclass
class NumericCanonical(CanonicalType):
    """
    Tipo numérico con precisión fija. Resuelve los falsos positivos
    por diferencias de representación entre FLOAT, DOUBLE PRECISION,
    REAL y NUMERIC entre motores distintos.
    """
    precision: int = 3
    scale:     int = 10

    information_loss: str = (
        "Se pierden los dígitos a partir del decimal {precision}. "
        "Diferencias menores a 10^-{precision} se tratan como iguales."
    )

    def to_sql(self, dialect) -> str:
        col  = self.column_name
        expr = dialect.round_numeric(col, self.precision, self.scale)
        return self.with_null_handling(expr, dialect)

    def validate(self, value) -> bool:
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False


@dataclass
class IntegerCanonical(CanonicalType):
    """
    Tipo entero. Normaliza representaciones de booleanos
    como TINYINT(1) en MySQL.
    """
    information_loss: str = "Ninguna para enteros bien formados."

    def to_sql(self, dialect) -> str:
        col  = self.column_name
        expr = dialect.cast_integer(col)
        return self.with_null_handling(expr, dialect)

    def validate(self, value) -> bool:
        try:
            int(value)
            return True
        except (TypeError, ValueError):
            return False