from dataclasses import dataclass
from .base import CanonicalType

class TemporalPrecision:
    MICROSECOND = "microsecond"
    SECOND      = "second"
    MINUTE      = "minute"
    HOUR        = "hour"
    DAY         = "day"

@dataclass
class TimestampCanonical(CanonicalType):
    """
    Tipo temporal normalizado a UTC con precisión configurable.
    Resuelve diferencias por zona horaria y por precisión
    en subsegundos entre motores.
    """
    precision:  str  = TemporalPrecision.SECOND
    force_utc:  bool = True

    information_loss: str = (
        "Se pierde la información de zona horaria original "
        "al normalizar a UTC. Se trunca la precisión temporal "
        "al nivel especificado."
    )

    def to_sql(self, dialect) -> str:
        col  = self.column_name
        expr = col
        if self.force_utc:
            expr = dialect.to_utc(expr)
        expr = dialect.truncate_timestamp(expr, self.precision)
        return self.with_null_handling(expr, dialect)

    def validate(self, value) -> bool:
        from datetime import datetime
        return isinstance(value, (datetime, str))