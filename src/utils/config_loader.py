"""
MDSM-Lite – Config Loader
src/utils/config_loader.py

Zodpovědnost:
    Načíst hlavní konfiguraci z config/config.yaml, validovat povinná pole
    a převést relativní projektové cesty na absolutní Path objekty.

Použití:
    from src.utils.config_loader import load_config
    config = load_config()
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    """Výjimka pro chyby konfigurace."""


@dataclass(frozen=True)
class Config:
    # Projekt
    project_name: str
    project_root: Path

    # TWS / IBKR
    tws_host: str
    tws_port: int
    tws_client_id: int
    tws_timeout_seconds: int

    # Market
    exchange_timezone: str
    market_close_time: str
    ingest_buffer_minutes: int

    # Cache / logging
    cache_format_version: int
    log_level: str

    # Cesty
    path_docs: Path
    path_scripts: Path
    path_logs: Path
    path_universe: Path
    path_cache_prices: Path
    path_cache_metadata: Path

    # Volitelně si necháme i původní config cestu
    config_file: Path


def load_config(config_path: Path | None = None) -> Config:
    """
    Načte config/config.yaml a vrátí objekt Config.

    Args:
        config_path:
            Volitelná explicitní cesta ke config.yaml.
            Když není zadána, použije se:
            <project_root>/config/config.yaml

    Returns:
        Config

    Raises:
        ConfigError:
            Když soubor chybí, YAML nejde načíst, nebo chybí povinná pole.
    """
    project_root = _detect_project_root()
    resolved_config_path = (
        config_path.resolve()
        if config_path is not None
        else project_root / "config" / "config.yaml"
    )

    if not resolved_config_path.exists():
        raise ConfigError(
            f"Konfigurační soubor nebyl nalezen: {resolved_config_path}"
        )

    try:
        with resolved_config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"Soubor config.yaml obsahuje neplatný YAML: {exc}"
        ) from exc
    except OSError as exc:
        raise ConfigError(
            f"Soubor config.yaml nelze otevřít: {exc}"
        ) from exc

    if not isinstance(raw, dict):
        raise ConfigError(
            "config.yaml musí obsahovat mapu/sekce na nejvyšší úrovni."
        )

    project_name = _require(raw, "project", "name", expected_type=str)

    tws_host = _require(raw, "tws", "host", expected_type=str)
    tws_port = _require(raw, "tws", "port", expected_type=int)
    tws_client_id = _require(raw, "tws", "client_id", expected_type=int)
    tws_timeout_seconds = _require(
        raw, "tws", "timeout_seconds", expected_type=int
    )

    exchange_timezone = _require(
        raw, "market", "exchange_timezone", expected_type=str
    )
    market_close_time = _require(
        raw, "market", "market_close_time", expected_type=str
    )
    ingest_buffer_minutes = _require(
        raw, "market", "ingest_buffer_minutes", expected_type=int
    )

    cache_format_version = _require(
        raw, "cache", "format_version", expected_type=int
    )

    log_level = _require(raw, "logging", "level", expected_type=str)

    path_docs = _resolve_project_path(
        project_root,
        _require(raw, "paths", "docs", expected_type=str),
        "paths.docs",
    )
    path_scripts = _resolve_project_path(
        project_root,
        _require(raw, "paths", "scripts", expected_type=str),
        "paths.scripts",
    )
    path_logs = _resolve_project_path(
        project_root,
        _require(raw, "paths", "logs", expected_type=str),
        "paths.logs",
    )
    path_universe = _resolve_project_path(
        project_root,
        _require(raw, "paths", "universe", expected_type=str),
        "paths.universe",
    )
    path_cache_prices = _resolve_project_path(
        project_root,
        _require(raw, "paths", "cache_prices", expected_type=str),
        "paths.cache_prices",
    )
    path_cache_metadata = _resolve_project_path(
        project_root,
        _require(raw, "paths", "cache_metadata", expected_type=str),
        "paths.cache_metadata",
    )

    return Config(
        project_name=project_name,
        project_root=project_root,
        tws_host=tws_host,
        tws_port=tws_port,
        tws_client_id=tws_client_id,
        tws_timeout_seconds=tws_timeout_seconds,
        exchange_timezone=exchange_timezone,
        market_close_time=market_close_time,
        ingest_buffer_minutes=ingest_buffer_minutes,
        cache_format_version=cache_format_version,
        log_level=log_level,
        path_docs=path_docs,
        path_scripts=path_scripts,
        path_logs=path_logs,
        path_universe=path_universe,
        path_cache_prices=path_cache_prices,
        path_cache_metadata=path_cache_metadata,
        config_file=resolved_config_path,
    )


def _detect_project_root() -> Path:
    """
    Určí kořen projektu z umístění tohoto souboru.

    Struktura:
        src/utils/config_loader.py
    => project_root = parents[2]
    """
    return Path(__file__).resolve().parents[2]


def _require(
    data: dict[str, Any],
    section: str,
    key: str,
    *,
    expected_type: type,
) -> Any:
    """
    Vytáhne povinnou hodnotu z data[section][key] a ověří její typ.
    """
    section_data = data.get(section)
    if not isinstance(section_data, dict):
        raise ConfigError(
            f"V config.yaml chybí sekce '{section}' nebo nemá správný formát."
        )

    if key not in section_data:
        raise ConfigError(
            f"V config.yaml chybí povinné pole '{section}.{key}'."
        )

    value = section_data[key]

    # bool je podtyp int, proto ho u int kontrolujeme zvlášť
    if expected_type is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(
                f"Pole '{section}.{key}' musí být typu int, "
                f"ale má hodnotu '{value}'."
            )
        return value

    if not isinstance(value, expected_type):
        raise ConfigError(
            f"Pole '{section}.{key}' musí být typu "
            f"{expected_type.__name__}, ale má hodnotu '{value}'."
        )

    return value


def _resolve_project_path(project_root: Path, raw_path: str, field_name: str) -> Path:
    """
    Převede relativní projektovou cestu na absolutní Path.

    Absolutní cesty záměrně nepovolujeme, aby konfigurace zůstala přenositelná.
    """
    path = Path(raw_path)

    if path.is_absolute():
        raise ConfigError(
            f"Pole '{field_name}' nesmí být absolutní cesta: '{raw_path}'. "
            "Použij relativní cestu vůči kořeni projektu."
        )

    return (project_root / path).resolve()