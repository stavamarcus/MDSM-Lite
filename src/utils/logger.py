"""
MDSM-Lite – Centralized Logger
src/utils/logger.py

Zodpovědnost:
    Poskytnout jednotný logging mechanismus pro všechny vrstvy systému.
    Žádná vrstva nesmí volat print() ani konfigurovat vlastní logger.

Použití:
    from src.utils.logger import get_logger
    logger = get_logger("AccessLayer")
    logger.info("cache hit | conid=756733")

Log soubory:
    logs/system.log  – INFO a výše (rotující)
    logs/errors.log  – ERROR a výše (rotující)

Konzole:
    DEBUG a výše (pouze pokud log level = DEBUG v configu)
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Formát logu
# Odpovídá addendu: timestamp | layer | action | conid | source | result |
#                   latency_ms | request_id | message
# Vrstva a ostatní strukturovaná pole se předávají v message textu.
# ---------------------------------------------------------------------------
_LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ---------------------------------------------------------------------------
# Interní stav – logger se inicializuje jednou
# ---------------------------------------------------------------------------
_initialized: bool = False
_root_logger_name: str = "mdsm"


def setup_logging(
    log_dir: Path,
    level: str = "DEBUG",
    max_bytes: int = 10_485_760,
    backup_count: int = 5,
) -> None:
    """
    Inicializuje logging systém. Musí být zavolán jednou při startu aplikace,
    typicky z main skriptu nebo Access Layer po načtení configu.

    Args:
        log_dir:      adresář pro log soubory (absolutní Path)
        level:        log level string – DEBUG / INFO / WARNING / ERROR
        max_bytes:    maximální velikost jednoho log souboru (default 10 MB)
        backup_count: počet rotovaných záložních souborů
    """
    global _initialized

    if _initialized:
        return

    log_dir.mkdir(parents=True, exist_ok=True)

    numeric_level = getattr(logging, level.upper(), logging.DEBUG)

    root = logging.getLogger(_root_logger_name)
    root.setLevel(logging.DEBUG)  # root zachytí vše, handlery filtrují

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # ------------------------------------------------------------------
    # Handler 1: system.log – INFO a výše
    # ------------------------------------------------------------------
    system_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / "system.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    system_handler.setLevel(logging.INFO)
    system_handler.setFormatter(formatter)
    root.addHandler(system_handler)

    # ------------------------------------------------------------------
    # Handler 2: errors.log – ERROR a výše
    # ------------------------------------------------------------------
    error_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / "errors.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root.addHandler(error_handler)

    # ------------------------------------------------------------------
    # Handler 3: konzole – dle nastaveného levelu (DEBUG během vývoje)
    # ------------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    _initialized = True


def get_logger(layer: str) -> logging.Logger:
    """
    Vrátí pojmenovaný logger pro danou vrstvu systému.

    Název loggeru tvoří hierarchii: mdsm.<layer>
    Například: mdsm.AccessLayer, mdsm.Provider, mdsm.Cache

    Args:
        layer: název vrstvy nebo komponenty (např. "AccessLayer", "Provider")

    Returns:
        logging.Logger
    """
    return logging.getLogger(f"{_root_logger_name}.{layer}")


# ---------------------------------------------------------------------------
# Pomocná funkce pro strukturované logování
# Formát odpovídá addendu v1.0
# ---------------------------------------------------------------------------

def format_log(
    action: str,
    result: str,
    conid: Optional[int] = None,
    source: Optional[str] = None,
    latency_ms: Optional[int] = None,
    request_id: Optional[str] = None,
    rows: Optional[int] = None,
    message: Optional[str] = None,
) -> str:
    """
    Sestaví strukturovaný log řetězec odpovídající formátu addendu.

    Příklad výstupu:
        action=fetch_history | conid=756733 | source=ibkr | result=success |
        latency_ms=1840 | rows=1258

    Args:
        action:     provedená operace (např. fetch_history, cache_read)
        result:     výsledek – success / fail / skip
        conid:      identifikátor instrumentu (volitelný)
        source:     zdroj dat – cache / ibkr (volitelný)
        latency_ms: doba trvání v ms (volitelný)
        request_id: unikátní ID požadavku pro tracing (volitelný)
        rows:       počet řádků dat (volitelný)
        message:    doplňující text (volitelný)

    Returns:
        Formátovaný string pro předání do logger.info() / logger.error()
    """
    parts: list[str] = [f"action={action}"]

    if conid is not None:
        parts.append(f"conid={conid}")
    if source is not None:
        parts.append(f"source={source}")

    parts.append(f"result={result}")

    if latency_ms is not None:
        parts.append(f"latency_ms={latency_ms}")
    if rows is not None:
        parts.append(f"rows={rows}")
    if request_id is not None:
        parts.append(f"request_id={request_id}")
    if message is not None:
        parts.append(f"msg={message}")

    return " | ".join(parts)
