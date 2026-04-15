from dataclasses import dataclass, field
from typing import Optional
from ..dialect.registry import DialectRegistry
from ..types.text import TextTransformation

# ── Tipos de datos semánticos permitidos en el YAML ───────────────

VALID_SEMANTIC_TYPES = {
    "numeric",
    "integer",
    "text",
    "timestamp",
    "date",
    "boolean",
}

# ── Transformaciones de texto permitidas ──────────────────────────

VALID_TRANSFORMATIONS = {
    TextTransformation.TRIM,
    TextTransformation.LOWERCASE,
    TextTransformation.NFC,
    TextTransformation.NFKC,
    TextTransformation.ASCII_FOLD,
    TextTransformation.REMOVE_PUNCT,
    TextTransformation.COLLAPSE_SPACES,
}

# ── Dataclasses de configuración ──────────────────────────────────

@dataclass
class ColumnConfig:
    """
    Configuración canónica de una columna individual.
    Representa una entrada del bloque 'columns' en el YAML.
    """
    name:             str
    type:             str
    nullable:         bool                = True
    information_loss: str                 = ""
    transformations:  list[str]           = field(default_factory=list)
    precision:        Optional[int]       = None
    scale:            Optional[int]       = None
    encoding:         Optional[str]       = None
    force_utc:        Optional[bool]      = None
    temporal_precision: Optional[str]     = None
    dialects:         dict[str, str]      = field(default_factory=dict)

    # Expresiones SQL por dialecto generadas por el inferidor IA.
    # Si están presentes, tienen preferencia sobre las generadas
    # automáticamente por los tipos canónicos.


@dataclass
class TableConfig:
    """
    Configuración canónica de una tabla completa.
    Representa una entrada del bloque 'tables' en el YAML.
    """
    name:    str
    columns: dict[str, ColumnConfig] = field(default_factory=dict)


@dataclass
class CanonicalConfig:
    """
    Configuración canónica completa del sistema.
    Representa el fichero canonical.yaml en su totalidad.
    """
    dialects: dict[str, str]           = field(default_factory=dict)
    tables:   dict[str, TableConfig]   = field(default_factory=dict)


# ── Validadores ───────────────────────────────────────────────────

class ConfigValidationError(Exception):
    """
    Error de validación de la configuración YAML.
    Acumula todos los errores encontrados antes de lanzarse,
    para que el usuario los vea todos de una vez en lugar de
    corregirlos uno a uno.
    """
    def __init__(self, errors: list[str]):
        self.errors = errors
        bullet_list = "\n".join(f"  · {e}" for e in errors)
        super().__init__(
            f"La configuración canónica contiene {len(errors)} "
            f"error(es):\n{bullet_list}"
        )


def validate_column(
    col_name:   str,
    col_data:   dict,
    table_name: str,
    errors:     list[str]
) -> None:
    """
    Valida la configuración de una columna individual.
    Los errores se acumulan en la lista recibida como argumento.
    """
    prefix = f"[{table_name}.{col_name}]"

    # Tipo semántico obligatorio y reconocido
    col_type = col_data.get("type")
    if not col_type:
        errors.append(f"{prefix} falta el campo 'type'")
    elif col_type not in VALID_SEMANTIC_TYPES:
        errors.append(
            f"{prefix} tipo '{col_type}' no reconocido. "
            f"Válidos: {sorted(VALID_SEMANTIC_TYPES)}"
        )

    # information_loss obligatorio: no aceptamos silencios sobre
    # qué información se pierde en cada transformación
    if not col_data.get("information_loss", "").strip():
        errors.append(
            f"{prefix} falta 'information_loss'. "
            f"Toda transformación debe documentar qué información pierde."
        )

    # Transformaciones: deben ser del conjunto permitido
    for t in col_data.get("transformations", []):
        if t not in VALID_TRANSFORMATIONS:
            errors.append(
                f"{prefix} transformación '{t}' no reconocida. "
                f"Válidas: {sorted(VALID_TRANSFORMATIONS)}"
            )

    # Dialectos: si se declaran expresiones SQL por dialecto,
    # los dialectos deben estar registrados en el sistema
    available_dialects = DialectRegistry.available()
    for dialect_name in col_data.get("dialects", {}).keys():
        if dialect_name not in available_dialects:
            errors.append(
                f"{prefix} dialecto '{dialect_name}' no registrado. "
                f"Registrados: {available_dialects}"
            )

    # Coherencia de tipo numérico
    if col_type == "numeric":
        precision = col_data.get("precision")
        if precision is not None and not isinstance(precision, int):
            errors.append(
                f"{prefix} 'precision' debe ser un entero, "
                f"se recibió: {type(precision).__name__}"
            )
        if precision is not None and precision < 0:
            errors.append(
                f"{prefix} 'precision' no puede ser negativo"
            )

    # Coherencia de tipo temporal
    if col_type in ("timestamp", "date"):
        temporal_precision = col_data.get("temporal_precision")
        valid_precisions = {
            "microsecond", "second", "minute", "hour", "day"
        }
        if temporal_precision and \
           temporal_precision not in valid_precisions:
            errors.append(
                f"{prefix} 'temporal_precision' inválida: "
                f"'{temporal_precision}'. "
                f"Válidas: {sorted(valid_precisions)}"
            )

    # Si se declaran expresiones SQL por dialecto,
    # deben contener el placeholder {col}
    for dialect_name, expr in col_data.get("dialects", {}).items():
        if "{col}" not in expr:
            errors.append(
                f"{prefix} la expresión SQL para '{dialect_name}' "
                f"no contiene el placeholder {{col}}: '{expr}'"
            )


def validate_config(raw: dict) -> None:
    """
    Valida la estructura completa del YAML.
    Recorre todas las tablas y columnas __acumulando__ errores
    antes de lanzar ConfigValidationError si los hay.
    """
    errors: list[str] = []

    # Clave raíz obligatoria
    if "canonicalizacion" not in raw:
        raise ConfigValidationError(
            ["Falta la clave raíz 'canonicalizacion'"]
        )

    canon = raw["canonicalizacion"]

    # Sección tables obligatoria
    if "tables" not in canon:
        raise ConfigValidationError(
            ["Falta la sección 'canonicalizacion.tables'"]
        )

    # Dialectos declarados: si se declaran, deben estar registrados
    available_dialects = DialectRegistry.available()
    for declared_dialect in canon.get("dialects", {}).keys():
        if declared_dialect not in available_dialects:
            errors.append(
                f"[dialects] '{declared_dialect}' declarado pero no "
                f"registrado en DialectRegistry. "
                f"Registrados: {available_dialects}"
            )

    # Validación por tabla y columna
    for table_name, table_data in canon["tables"].items():
        if not isinstance(table_data, dict):
            errors.append(
                f"[{table_name}] debe ser un diccionario, "
                f"se recibió: {type(table_data).__name__}"
            )
            continue

        if "columns" not in table_data:
            errors.append(
                f"[{table_name}] falta la sección 'columns'"
            )
            continue

        for col_name, col_data in table_data["columns"].items():
            if not isinstance(col_data, dict):
                errors.append(
                    f"[{table_name}.{col_name}] debe ser un "
                    f"diccionario, se recibió: "
                    f"{type(col_data).__name__}"
                )
                continue
            validate_column(col_name, col_data, table_name, errors)

    if errors:
        raise ConfigValidationError(errors)