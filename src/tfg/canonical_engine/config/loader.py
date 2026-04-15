import yaml
from pathlib import Path
from typing import Union
from .schema import (
    CanonicalConfig,
    TableConfig,
    ColumnConfig,
    validate_config,
    ConfigValidationError,
)
from ..types.numeric  import NumericCanonical, IntegerCanonical
from ..types.text     import TextCanonical, TextTransformation
from ..types.temporal import TimestampCanonical
from ..types.boolean  import BooleanCanonical
from ..types.base     import CanonicalType

class CanonicalConfigLoader:
    """
    Carga, valida y materializa la configuración canónica
    desde un fichero YAML o desde un diccionario en memoria.

    El proceso tiene tres fases diferenciadas:
    1. Carga del YAML en crudo (raw dict)
    2. Validación estructural y semántica (schema.py)
    3. Materialización en dataclasses y tipos canónicos

    ¿Por qué separar entre validación (2) y materialización (3)?:
    .schema.validate_config() puede llamarse para comprobar un YAML
    sin instanciar ningún tipo canónico, lo que es útil en tests
    y en el inferidor IA antes de guardar la configuración generada.
    """

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "LoadedConfig":
        """
        Carga la configuración desde un fichero YAML en disco.
        Lanza FileNotFoundError si el fichero no existe y
        ConfigValidationError si la estructura es inválida.
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(
                f"Fichero de configuración no encontrado: {path}\nRuta absoluta: {path.resolve()}"
            )
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)   # ... nos aseguramos de cargar solo datos , no ejecución de cmd

        return cls._load(raw, source=str(path))

    @classmethod
    def from_dict(cls, raw: dict) -> "LoadedConfig":
        """
        Carga la configuración desde un diccionario en memoria.
        Útil cuando el YAML ha sido generado por el inferidor IA
        y aún no se ha persistido en disco.
        """
        return cls._load(raw, source="<dict>")

    @classmethod
    def _load(cls, raw: dict, source: str) -> "LoadedConfig":
        """
        Validación + materialización.
        """
        # Fase 1: validación estructural del fichero de configuración yaml
        try:
            validate_config(raw)
        except ConfigValidationError as e:
            raise ConfigValidationError(
                [f"[{source}] {err}" for err in e.errors]
            ) from e

        # Fase 2: materialización
        canon_raw = raw["canonicalizacion"]
        config    = cls._materialize_config(canon_raw)

        return LoadedConfig(
            config = config,
            source = source,
            raw    = raw,
        )

    @classmethod
    def _materialize_config(cls, canon_raw: dict) -> CanonicalConfig:
        tables = {}
        for table_name, table_data in canon_raw["tables"].items():
            columns = {}
            for col_name, col_data in table_data["columns"].items():
                col_config = cls._materialize_column(col_name, col_data)
                columns[col_name] = col_config

            tables[table_name] = TableConfig(
                name    = table_name,
                columns = columns,
            )

        return CanonicalConfig(
            dialects = canon_raw.get("dialects", {}),
            tables   = tables,
        )

    @classmethod
    def _materialize_column(
        cls,
        col_name: str,
        col_data: dict
    ) -> ColumnConfig:
        return ColumnConfig(
            name               = col_name,
            type               = col_data["type"],
            nullable           = col_data.get("nullable", True),
            information_loss   = col_data.get("information_loss", ""),
            transformations    = col_data.get("transformations", []),
            precision          = col_data.get("precision"),
            scale              = col_data.get("scale"),
            encoding           = col_data.get("encoding"),
            force_utc          = col_data.get("force_utc"),
            temporal_precision = col_data.get("temporal_precision"),
            dialects           = col_data.get("dialects", {}),
        )


class LoadedConfig:
    """
    Resultado de la carga de configuración.
    Expone tanto la configuración materializada como el raw dict
    y el origen, útiles para depuración y para regenerar el YAML.
    """

    def __init__(self, config: CanonicalConfig, source: str, raw: dict):
        self.config = config
        self.source = source
        self.raw    = raw

    def to_canonical_types(
        self,
        table_name: str
    ) -> dict[str, "CanonicalType"]:
        """
        Convierte la configuración de una tabla en tipos canónicos
        instanciados, listos para ser usados por el CanonicalPipeline.

        Si una columna tiene expresiones SQL declaradas explícitamente
        en el YAML (campo 'dialects'), esas expresiones tienen
        preferencia sobre las generadas automáticamente por el tipo
        canónico. Esto permite que la configuración generada por el
        inferidor IA sobreescriba el comportamiento por defecto.
        """
        if table_name not in self.config.tables:
            raise KeyError(
                f"Tabla '{table_name}' no encontrada en la "
                f"configuración. Tablas disponibles: "
                f"{list(self.config.tables.keys())}"
            )

        table   = self.config.tables[table_name]
        result  = {}

        for col_name, col_config in table.columns.items():
            canonical_type = _build_canonical_type(col_config)
            result[col_name] = canonical_type

        return result

    def save(self, path: Union[str, Path]) -> None:
        """
        Persiste la configuración en disco como YAML.
        Útil para guardar la configuración generada por el inferidor IA
        tras revisión manual.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(
                self.raw,
                f,
                allow_unicode  = True,
                sort_keys      = False,
                default_flow_style = False,
            )
        print(f"Configuración canónica guardada en: {path}")

    def report(self) -> str:
        """
        Genera un resumen legible de la configuración cargada,
        útil para incluir en el informe del CanonicalPlan.
        """
        lines = [
            f"Configuración canónica [{self.source}]",
            f"{'─' * 50}",
            f"Dialectos declarados: "
            f"{list(self.config.dialects.keys()) or 'ninguno'}",
            f"Tablas configuradas:  "
            f"{list(self.config.tables.keys())}",
            f"{'─' * 50}",
        ]

        for table_name, table in self.config.tables.items():
            lines.append(f"  Tabla: {table_name}")
            for col_name, col in table.columns.items():
                has_sql = bool(col.dialects)
                tag     = "[SQL explícito]" if has_sql else "[inferido]"
                lines.append(
                    f"    {col_name:<22} {col.type:<12} {tag}"
                )
                if col.information_loss:
                    lines.append(
                        f"    {'':22} ⚠ {col.information_loss}"
                    )

        return "\n".join(lines)


# ── Constructor de tipos canónicos desde ColumnConfig ─────────────

def _build_canonical_type(col: ColumnConfig):
    """
    Instancia el tipo canónico correspondiente a partir de
    la configuración de una columna.

    Si la columna tiene expresiones SQL explícitas por dialecto,
    se inyectan en el tipo resultante para que el CanonicalPipeline
    las use en lugar de las generadas automáticamente.
    """
    common = dict(
        column_name      = col.name,
        nullable         = col.nullable,
        information_loss = col.information_loss,
    )

    if col.type == "numeric":
        instance = NumericCanonical(
            **common,
            precision = col.precision or 3,
            scale     = col.scale     or 10,
        )

    elif col.type == "integer":
        instance = IntegerCanonical(**common)

    elif col.type == "text":
        instance = TextCanonical(
            **common,
            transformations = col.transformations or [
                TextTransformation.TRIM,
                TextTransformation.LOWERCASE,
                TextTransformation.NFC,
            ],
            encoding   = col.encoding   or "utf8mb4",
            max_length = 255,
        )

    elif col.type in ("timestamp", "date"):
        instance = TimestampCanonical(
            **common,
            force_utc  = col.force_utc if col.force_utc is not None
                         else True,
            precision  = col.temporal_precision or (
                "day" if col.type == "date" else "second"
            ),
        )

    elif col.type == "boolean":
        instance = BooleanCanonical(**common)

    else:
        # Fallback defensivo: no debería llegar aquí si
        # validate_config() ha pasado correctamente
        instance = TextCanonical(**common)

    # Inyección de expresiones SQL explícitas por dialecto.
    # Si el YAML declara 'dialects', se almacenan en el tipo
    # para que el pipeline los use con preferencia.
    if col.dialects:
        instance._explicit_dialects = col.dialects

    return instance
