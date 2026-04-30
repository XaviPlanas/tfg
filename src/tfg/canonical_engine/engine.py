import unicodedata
from .dialect.base import UnsupportedTransformation
import logging
logger = logging.getLogger(__name__)

class PythonFallback:
    """
    Transformaciones Unicode implementadas en Python para motores
    que no las soportan de forma nativa.
    Estas transformaciones requieren descargar los valores afectados,
    lo que debe quedar documentado en el informe de canonización.
    """

    @staticmethod
    def normalize_unicode(value: str, form: str) -> str:
        if value is None:
            return None
        return unicodedata.normalize(form, value)

    @staticmethod
    def ascii_fold(value: str) -> str:
        if value is None:
            return None
        # Descomposición NFD + eliminación de marcas combinantes
        nfd = unicodedata.normalize('NFD', value)
        return ''.join(
            c for c in nfd
            if unicodedata.category(c) != 'Mn'  # Mn = Mark, Nonspacing
        )

    @staticmethod
    def collapse_spaces(value: str) -> str:
        if value is None:
            return None
        import re
        return re.sub(r'\s+', ' ', value).strip()