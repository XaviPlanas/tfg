"""
Sistema de logging centralizado del TFG.

Decisión de diseño: se usa el módulo estándar `logging` de Python
en lugar de librerías externas (loguru, structlog) porque:
  - Sin dependencias adicionales.
  - Comportamiento predecible y ampliamente documentado.
  - Compatible con cualquier framework que consuma el logger raíz.
  - Suficiente para los tres niveles de observabilidad que necesita
    este sistema: traza de pipeline, llamadas LLM y errores de DB.

Arquitectura de handlers:
  ┌─────────────────────────────────────────────────────────┐
  │  logger = logging.getLogger("tfg.canonical_engine.X")  │
  └──────────────────┬──────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   ConsoleHandler          RotatingFileHandler
   level: INFO             level: DEBUG
   formato: humano         formato: JSON estructurado
   (demo/defensa)          (análisis posterior)

Jerarquía de loggers del proyecto:
  tfg                          ← logger raíz del proyecto
  tfg.canonical_engine         ← motor de canonización
  tfg.canonical_engine.pipeline
  tfg.canonical_engine.dialect
  tfg.canonical_engine.introspection
  tfg.datadiff_classifier      ← clasificador
  tfg.datadiff_classifier.llm  ← llamadas al LLM (tiempos, tokens)

Uso por módulo:
    import logging
    logger = logging.getLogger(__name__)   # ← una línea, nada más

Inicialización (una sola vez, en main o entrypoint):
    from tfg.logging_config import setup_logging
    setup_logging(level="DEBUG", log_file="logs/tfg.log")
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import time
from datetime import datetime, timezone
from pathlib  import Path
from typing   import Optional


# ─────────────────────────────────────────────────────────────
# Niveles custom
# ─────────────────────────────────────────────────────────────

# TRACE: por debajo de DEBUG, para SQL generado y respuestas raw LLM.
# Se usa solo en development; nunca llega a producción sin --trace.
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def trace(self: logging.Logger, message: str, *args, **kwargs):
    """Método de conveniencia: logger.trace('SQL generado: %s', sql)."""
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


logging.Logger.trace = trace  # type: ignore[attr-defined]


# ─────────────────────────────────────────────────────────────
# Formateadores
# ─────────────────────────────────────────────────────────────

class HumanFormatter(logging.Formatter):
    """
    Formato legible para consola durante la demo y la defensa.
    Incluye colores ANSI si el terminal lo soporta.

    Ejemplo de salida:
      [14:23:01] INFO     canonical_engine.pipeline   Vista canónica creada: titanic_canonical
      [14:23:02] WARNING  canonical_engine.dialect     UnsupportedTransformation: MySQL no soporta NORMALIZE(NFC)
      [14:23:05] DEBUG    datadiff_classifier.llm      LLM claude-sonnet latencia=1243ms tokens_in=412 tokens_out=87
    """

    COLORS = {
        TRACE:            "\033[90m",      # gris (mejor que dim)
        logging.DEBUG:    "\033[36m",      # cyan
        logging.INFO:     "\033[32m",      # green
        logging.WARNING:  "\033[33m",      # yellow
        logging.ERROR:    "\033[31m",      # red
        logging.CRITICAL: "\033[1;31m",    # bold red
    }
    RESET = "\033[0m"

    def __init__(self, use_colors: bool = True):
        super().__init__()
        self.use_colors = use_colors and self._terminal_supports_colors()

    @staticmethod
    def _terminal_supports_colors() -> bool:
        return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        # Nombre del módulo sin el prefijo "tfg." para ahorrar espacio
        short_name = record.name.removeprefix("tfg.")
        if len(short_name) > 36:
            short_name = "…" + short_name[-35:]

        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level     = record.levelname[:7].ljust(7)
        message   = record.getMessage()

        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        line = f"[{timestamp}] {level}  {short_name:<36}  {message}"

        if self.use_colors:
            color = self.COLORS.get(record.levelno, "")
            return f"{color}{line}{self.RESET}"
        return line


class JsonFormatter(logging.Formatter):
    """
    Formato JSON por línea para el fichero de log.
    Permite post-procesado con jq, ELK, o cualquier herramienta de análisis.

    Ejemplo de línea en el fichero:
    {"ts":"2024-01-15T14:23:05Z","level":"WARNING","logger":"tfg.canonical_engine.dialect",
     "msg":"UnsupportedTransformation","dialect":"mysql","transform":"unicode_nfc",
     "fallback":"python","module":"mysql.py","line":87}
    """

    STANDARD_FIELDS = {
        "name", "msg", "args", "levelname", "levelno",
        "pathname", "filename", "module", "exc_info",
        "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread",
        "threadName", "processName", "process", "message",
    }

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        doc: dict = {
            "ts":      datetime.fromtimestamp(record.created, tz=timezone.utc)
                               .isoformat(timespec="milliseconds"),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.message,
            "module":  record.filename,
            "line":    record.lineno,
            "func":    record.funcName,
        }

        # Campos extra añadidos con logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key not in self.STANDARD_FIELDS and not key.startswith("_"):
                doc[key] = value

        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)

        return json.dumps(doc, ensure_ascii=False, default=str)


# ─────────────────────────────────────────────────────────────
# Filtros
# ─────────────────────────────────────────────────────────────

class LLMCallFilter(logging.Filter):
    """
    Filtra solo los registros del logger tfg.datadiff_classifier.llm.
    Útil para separar el log de llamadas LLM del log general.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("tfg.datadiff_classifier.llm")


# ─────────────────────────────────────────────────────────────
# Función de setup — entrypoint único
# ─────────────────────────────────────────────────────────────

def setup_logging(
    level:        str           = "INFO",
    log_file:     Optional[str] = "logs/tfg.log",
    llm_log_file: Optional[str] = "logs/tfg_llm.log",
    use_colors:   bool          = True,
    max_bytes:    int           = 5 * 1024 * 1024,   # 5 MB
    backup_count: int           = 3,
) -> logging.Logger:
    """
    Inicializa el sistema de logging del proyecto.
    Debe llamarse UNA SOLA VEZ, al inicio del entrypoint (main).

    Parámetros:
        level:        Nivel mínimo para consola. "DEBUG" en desarrollo,
                      "INFO" en demos. El fichero siempre guarda DEBUG.
        log_file:     Ruta del fichero de log general (JSON rotativo).
                      None para desactivar el fichero.
        llm_log_file: Ruta del fichero exclusivo de llamadas LLM.
                      Permite analizar tiempos y costes de forma aislada.
        use_colors:   Colores ANSI en consola (autodetectado si es TTY).
        max_bytes:    Tamaño máximo de cada fichero antes de rotar.
        backup_count: Número de ficheros rotados que se conservan.

    Returns:
        Logger raíz del proyecto ("tfg") ya configurado.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # ── Logger raíz del proyecto ──────────────────────────────
    root = logging.getLogger("tfg")
    root.setLevel(TRACE)   # captura todo; los handlers filtran por nivel
    root.handlers.clear()  # evitar duplicados si se llama varias veces

    # ── Handler 1: consola (humano, colorizado) ───────────────
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(numeric_level)
    console.setFormatter(HumanFormatter(use_colors=use_colors))
    root.addHandler(console)

    # ── Handler 2: fichero general (JSON, rotativo) ───────────
    if log_file:
        _ensure_dir(log_file)
        file_handler = logging.handlers.RotatingFileHandler(
            filename    = log_file,
            maxBytes    = max_bytes,
            backupCount = backup_count,
            encoding    = "utf-8",
        )
        file_handler.setLevel(TRACE)         # guarda todo, incluyendo TRACE
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)

    # ── Handler 3: fichero exclusivo LLM ─────────────────────
    if llm_log_file:
        _ensure_dir(llm_log_file)
        llm_handler = logging.handlers.RotatingFileHandler(
            filename    = llm_log_file,
            maxBytes    = max_bytes,
            backupCount = backup_count,
            encoding    = "utf-8",
        )
        llm_handler.setLevel(logging.DEBUG)
        llm_handler.setFormatter(JsonFormatter())
        llm_handler.addFilter(LLMCallFilter())
        root.addHandler(llm_handler)

    # ── Silenciar loggers verbosos de dependencias ────────────
    for noisy in ("sqlalchemy.engine", "sqlalchemy.pool",
                  "urllib3", "httpx", "httpcore", "anthropic"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root.info(
        "Logging inicializado",
        extra={
            "console_level": level,
            "log_file":      log_file,
            "llm_log_file":  llm_log_file,
        },
    )
    return root


# ─────────────────────────────────────────────────────────────
# Context manager para medir tiempos
# ─────────────────────────────────────────────────────────────

class timed:
    """
    Context manager que mide el tiempo de un bloque y lo emite como log.

    Uso:
        with timed(logger, "Aplicar plan MySQL", level="INFO"):
            pipeline_mysql.apply_plan(plan)

        # Emite al salir:
        # [14:23:05] INFO  canonical_engine.pipeline  Aplicar plan MySQL  elapsed_ms=234
    """

    def __init__(
        self,
        logger:    logging.Logger,
        operation: str,
        level:     str  = "DEBUG",
        extra:     dict = None,
    ):
        self.logger    = logger
        self.operation = operation
        self.level     = getattr(logging, level.upper(), logging.DEBUG)
        self.extra     = extra or {}
        self._start:   float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed_ms = int((time.perf_counter() - self._start) * 1000)
        extra      = {**self.extra, "elapsed_ms": elapsed_ms}
        if exc_type:
            self.logger.error(
                "%s — FALLÓ tras %d ms: %s",
                self.operation, elapsed_ms, exc_val,
                extra=extra, exc_info=True,
            )
        else:
            self.logger.log(self.level, "%s — %d ms", self.operation, elapsed_ms, extra=extra)
        return False   # no suprimir excepciones


# ─────────────────────────────────────────────────────────────
# Helper privado
# ─────────────────────────────────────────────────────────────

def _ensure_dir(file_path: str) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)