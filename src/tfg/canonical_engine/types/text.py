from dataclasses import dataclass, field
from typing import List
from .base import CanonicalType

class TextTransformation:
    TRIM            = "trim"
    LOWERCASE       = "lowercase"
    NFC             = "unicode_nfc"
    NFKC            = "unicode_nfkc"
    ASCII_FOLD      = "ascii_fold"
    REMOVE_PUNCT    = "remove_punct"
    COLLAPSE_SPACES = "collapse_spaces"

@dataclass
class TextCanonical(CanonicalType):
    """
    Tipo texto con pipeline de transformaciones configurable.
    Las transformaciones se aplican en el orden en que se declaran.
    Cada transformación se delega al dialecto para garantizar
    que la expresión SQL resultante es válida en el motor destino.
    """
    transformations: List[str] = field(default_factory=lambda: [
        TextTransformation.TRIM,
        TextTransformation.LOWERCASE,
        TextTransformation.NFC,
    ])
    encoding:        str = "utf8mb4"
    max_length:      int = 255

    information_loss: str = (
        "Se pierde la distinción de mayúsculas/minúsculas. "
        "La normalización NFC unifica representaciones Unicode "
        "equivalentes pero preserva caracteres especiales."
    )

    def to_sql(self, dialect) -> str:
        """
        Construye la expresión SQL aplicando las transformaciones
        en orden, cada una envolviendo a la anterior.
        El resultado es una expresión SQL anidada del tipo:
        LOWER(TRIM(NORMALIZE(col, NFC)))
        """
        # Punto de partida: referencia a la columna con encoding
        expr = dialect.ensure_encoding(self.column_name, self.encoding)

        # Aplicar transformaciones en orden
        for transform in self.transformations:
            if transform == TextTransformation.TRIM:
                expr = dialect.trim(expr)
            elif transform == TextTransformation.LOWERCASE:
                expr = dialect.lowercase(expr)
            elif transform == TextTransformation.NFC:
                expr = dialect.normalize_unicode(expr, "NFC")
            elif transform == TextTransformation.NFKC:
                expr = dialect.normalize_unicode(expr, "NFKC")
            elif transform == TextTransformation.ASCII_FOLD:
                expr = dialect.ascii_fold(expr)
            elif transform == TextTransformation.COLLAPSE_SPACES:
                expr = dialect.collapse_spaces(expr)
            elif transform == TextTransformation.REMOVE_PUNCT:
                expr = dialect.remove_punct(expr)

        return self.with_null_handling(expr, dialect)

    def validate(self, value) -> bool:
        return isinstance(value, str)