"""
MDSM-Lite – Metadata Manager
src/cache/metadata_manager.py

Zodpovědnost:
    Čtení a zápis JSON metadat per instrument a timeframe.
    Rozhoduje, zda je cache pro daný instrument platná.
    Cache Layer POUZE hlásí stav – rozhodnutí o dalším postupu
    je odpovědností Access Layer.

Soubory:
    metadata/{conid}_{timeframe}.json
    Příklad: metadata/756733_D1.json
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Cache.Metadata")

# ---------------------------------------------------------------------------
# Povinná pole metadat – používají se při čtení i zápisu
# ---------------------------------------------------------------------------
REQUIRED_METADATA_FIELDS: list[str] = [
    "conid",
    "timeframe",
    "cache_format_version",
    "last_updated",
    "start_date",
    "end_date",
    "rows",
    "permissions_status",
    "is_valid",
]

# Povolené hodnoty permissions_status
# Neznámá hodnota = podezřelý stav = cache neplatná
VALID_PERMISSIONS_STATUSES: set[str] = {"ok", "no_permissions", "error"}

# Blokující hodnoty permissions_status
BLOCKING_PERMISSIONS_STATUSES: set[str] = {"no_permissions", "error"}


class MetadataError(Exception):
    """Výjimka pro chyby při práci s metadaty."""


class MetadataManager:
    """
    Spravuje JSON metadata souborů cache.
    Každý instrument + timeframe má vlastní metadata soubor.
    """

    def __init__(self, config: Config) -> None:
        self._metadata_dir: Path = config.path_cache_metadata
        self._config_format_version: int = config.cache_format_version
        self._metadata_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Názvy souborů
    # ------------------------------------------------------------------

    def _metadata_path(self, conid: int, timeframe: str) -> Path:
        return self._metadata_dir / f"{conid}_{timeframe}.json"

    # ------------------------------------------------------------------
    # Čtení
    # ------------------------------------------------------------------

    def read(self, conid: int, timeframe: str = "D1") -> Optional[dict]:
        """
        Načte metadata pro daný instrument a timeframe.
        Validuje přítomnost všech povinných polí.

        Returns:
            dict s metadaty nebo None pokud soubor neexistuje nebo je neplatný
        """
        path = self._metadata_path(conid, timeframe)

        if not path.exists():
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error(format_log(
                action="metadata_read",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=invalid_json error={exc}",
            ))
            return None

        # Validace povinných polí
        missing = [f for f in REQUIRED_METADATA_FIELDS if f not in data]
        if missing:
            logger.error(format_log(
                action="metadata_read",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=missing_fields fields={missing}",
            ))
            return None

        return data

    # ------------------------------------------------------------------
    # Zápis
    # ------------------------------------------------------------------

    def write(self, conid: int, metadata: dict, timeframe: str = "D1") -> None:
        """
        Zapíše metadata pro daný instrument a timeframe.
        Před zápisem validuje strukturu a konzistenci dat.
        Atomic write: .tmp → rename → .json

        Args:
            conid:     identifikátor instrumentu
            metadata:  dict s metadaty
            timeframe: timeframe (výchozí "D1")

        Raises:
            MetadataError: validace selhala nebo zápis selhal
        """
        self._validate_for_write(conid, timeframe, metadata)

        path = self._metadata_path(conid, timeframe)
        tmp_path = path.with_suffix(".tmp")

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, default=str)
            tmp_path.replace(path)
        except Exception as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            raise MetadataError(
                f"Zápis metadat selhal pro conid={conid} timeframe={timeframe}: {exc}"
            ) from exc

        logger.info(format_log(
            action="metadata_write",
            conid=conid,
            result="success",
            message=f"timeframe={timeframe} path={path}",
        ))

    def _validate_for_write(
        self, conid: int, timeframe: str, metadata: dict
    ) -> None:
        """
        Validuje metadata před zápisem.

        Kontroluje:
            1. přítomnost všech povinných polí
            2. metadata["conid"] odpovídá argumentu conid
            3. metadata["timeframe"] odpovídá argumentu timeframe

        Raises:
            MetadataError: při jakékoli neshodě
        """
        # 1. Povinná pole
        missing = [f for f in REQUIRED_METADATA_FIELDS if f not in metadata]
        if missing:
            raise MetadataError(
                f"Metadata pro conid={conid} timeframe={timeframe} "
                f"chybí povinná pole: {missing}"
            )

        # 2. Konzistence conid
        if metadata["conid"] != conid:
            raise MetadataError(
                f"Neshoda conid: argument={conid}, "
                f"metadata['conid']={metadata['conid']}"
            )

        # 3. Konzistence timeframe
        if metadata["timeframe"] != timeframe:
            raise MetadataError(
                f"Neshoda timeframe: argument={timeframe}, "
                f"metadata['timeframe']={metadata['timeframe']}"
            )

    # ------------------------------------------------------------------
    # Validace stavu cache
    # ------------------------------------------------------------------

    def is_valid(self, conid: int, timeframe: str = "D1") -> bool:
        """
        Rozhodne, zda je cache pro daný instrument platná.

        Kontroluje (v pořadí):
            1. metadata existují a mají všechna povinná pole
            2. cache_format_version odpovídá konfiguraci
            3. is_valid == True
            4. permissions_status je jedna z VALID_PERMISSIONS_STATUSES
            5. permissions_status není blokující (BLOCKING_PERMISSIONS_STATUSES)

        Returns:
            True = cache je platná a použitelná
            False = cache je neplatná, Access Layer musí rozhodnout co dál

        Poznámka:
            Cache Layer POUZE hlásí stav.
            Rozhodnutí o komunikaci s IBKR je odpovědností Access Layer.
        """
        metadata = self.read(conid, timeframe)

        if metadata is None:
            logger.info(format_log(
                action="metadata_validate",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=no_metadata_or_missing_fields",
            ))
            return False

        # Kontrola cache_format_version
        cached_version = metadata.get("cache_format_version")
        if cached_version != self._config_format_version:
            logger.warning(format_log(
                action="metadata_validate",
                conid=conid,
                result="fail",
                message=(
                    f"timeframe={timeframe} reason=version_mismatch "
                    f"cached={cached_version} expected={self._config_format_version}"
                ),
            ))
            return False

        # Kontrola is_valid flagu
        if not metadata.get("is_valid", False):
            logger.info(format_log(
                action="metadata_validate",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=is_valid=False",
            ))
            return False

        # Kontrola permissions_status – musí být jedna z povolených hodnot
        permissions = metadata.get("permissions_status", "")
        if permissions not in VALID_PERMISSIONS_STATUSES:
            logger.warning(format_log(
                action="metadata_validate",
                conid=conid,
                result="fail",
                message=(
                    f"timeframe={timeframe} reason=unknown_permissions_status "
                    f"value='{permissions}' allowed={sorted(VALID_PERMISSIONS_STATUSES)}"
                ),
            ))
            return False

        # Kontrola blokujícího permissions_status
        if permissions in BLOCKING_PERMISSIONS_STATUSES:
            logger.warning(format_log(
                action="metadata_validate",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=permissions_blocked status={permissions}",
            ))
            return False

        return True

    def exists(self, conid: int, timeframe: str = "D1") -> bool:
        """Vrátí True pokud metadata soubor existuje."""
        return self._metadata_path(conid, timeframe).exists()
