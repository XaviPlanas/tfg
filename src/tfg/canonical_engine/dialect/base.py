from abc import ABC, abstractmethod

class BaseDialect(ABC):
    """
    Interfaz de dialecto SQL. Define las primitivas que cada motor
    debe implementar. Los tipos canónicos usan estas primitivas
    para construir sus expresiones sin conocer el motor destino.
    """
    name: str = ""

    # ── Numéricos ─────────────────────────────────────────────────

    @abstractmethod
    def round_numeric(self, col: str, precision: int, scale: int) -> str:
        """ROUND con tipo explícito para evitar artefactos de float."""
        ...

    @abstractmethod
    def cast_integer(self, col: str) -> str:
        ...

    # ── Texto ─────────────────────────────────────────────────────

    @abstractmethod
    def ensure_encoding(self, col: str, encoding: str) -> str:
        """Fuerza el encoding de la columna antes de operar sobre ella."""
        ...

    @abstractmethod
    def trim(self, expr: str) -> str:
        ...

    @abstractmethod
    def lowercase(self, expr: str) -> str:
        ...

    @abstractmethod
    def normalize_unicode(self, expr: str, form: str) -> str:
        """
        Normalización Unicode. Si el motor no la soporta de forma
        nativa, debe indicarlo levantando UnsupportedTransformation.
        """
        ...

    @abstractmethod
    def ascii_fold(self, expr: str) -> str:
        """Convierte caracteres acentuados a su equivalente ASCII."""
        ...

    @abstractmethod
    def collapse_spaces(self, expr: str) -> str:
        ...

    @abstractmethod
    def remove_punct(self, expr: str) -> str:
        ...

    # ── Temporal ──────────────────────────────────────────────────

    @abstractmethod
    def to_utc(self, expr: str) -> str:
        ...

    @abstractmethod
    def truncate_timestamp(self, expr: str, precision: str) -> str:
        ...

    # ── Nulos ─────────────────────────────────────────────────────

    @abstractmethod
    def coalesce(self, expr: str, default: str) -> str:
        ...

    @abstractmethod
    def null_replacement(self, canonical_type) -> str:
        """Valor de reemplazo para NULL según el tipo canónico."""
        ...

    # ── Boolean ────────────────────────────────────────────────────

    @abstractmethod
    def normalize_boolean(self, col: str) -> str:
        """
        Normaliza cualquier representación de booleano a un entero
        0 o 1. El resultado es comparable entre motores independientemente
        de si el tipo original era BOOLEAN, TINYINT(1) o una cadena.
        """
        ...

class UnsupportedTransformation(Exception):
    """
    Se lanza cuando un motor no soporta una transformación nativa.
    El motor de canonización la captura y delega al fallback Python.
    """
    def __init__(self, dialect: str, transformation: str):
        self.dialect        = dialect
        self.transformation = transformation
        super().__init__(
            f"El dialecto '{dialect}' no soporta '{transformation}' "
            f"de forma nativa. Se usará el fallback Python."  
            #FIXME: sólo si existe fallback definido ... se debería comprobar
        )