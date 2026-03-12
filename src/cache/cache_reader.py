"""
MDSM-Lite – Cache Reader
src/cache/cache_reader.py

Zodpovědnost:
    Čtení cenových dat z Parquet souborů.
    Validuje metadata, existenci souboru, strukturu sloupců a neprázdnost dat.
    cache_format_version validuje přes MetadataManager.

Soubory:
    prices/{conid}_{timeframe}.parquet
    Příklad: prices/756733_D1.parquet
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import pandas as pd

from src.cache.metadata_manager import MetadataManager
from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Cache.Reader")

# ---------------------------------------------------------------------------
# Očekávané sloupce v Parquet souboru
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS: list[str] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
]


class CacheReadError(Exception):
    """Výjimka pro chyby při čtení cache."""


class CacheReader:
    """
    Čte cenová data z Parquet souborů.
    Před čtením validuje metadata (včetně cache_format_version).
    """

    def __init__(self, config: Config, metadata_manager: MetadataManager) -> None:
        self._prices_dir: Path = config.path_cache_prices
        self._metadata = metadata_manager

    # ------------------------------------------------------------------
    # Názvy souborů
    # ------------------------------------------------------------------

    def _parquet_path(self, conid: int, timeframe: str) -> Path:
        return self._prices_dir / f"{conid}_{timeframe}.parquet"

    # ------------------------------------------------------------------
    # Čtení
    # ------------------------------------------------------------------

    def read(
        self,
        conid: int,
        timeframe: str = "D1",
    ) -> Optional[pd.DataFrame]:
        """
        Načte cenová data z cache.

        Validace před a po čtení:
            1. metadata existují a cache je platná (is_valid + cache_format_version)
            2. Parquet soubor existuje
            3. Soubor jde načíst
            4. DataFrame obsahuje očekávané sloupce
            5. DataFrame není prázdný

        Returns:
            pd.DataFrame nebo None pokud cache není platná nebo neexistuje.
            None = Access Layer musí rozhodnout co dál.
        """
        start = time.time()

        # 1. Validace metadat (včetně cache_format_version)
        if not self._metadata.is_valid(conid, timeframe):
            logger.info(format_log(
                action="cache_read",
                conid=conid,
                source="cache",
                result="miss",
                message=f"timeframe={timeframe} reason=metadata_invalid",
            ))
            return None

        # 2. Existence Parquet souboru
        path = self._parquet_path(conid, timeframe)
        if not path.exists():
            logger.info(format_log(
                action="cache_read",
                conid=conid,
                source="cache",
                result="miss",
                message=f"timeframe={timeframe} reason=file_not_found",
            ))
            return None

        # 3. Čtení souboru
        try:
            df = pd.read_parquet(path, engine="pyarrow")
        except Exception as exc:
            logger.error(format_log(
                action="cache_read",
                conid=conid,
                source="cache",
                result="fail",
                message=f"timeframe={timeframe} reason=read_error error={exc}",
            ))
            return None

        # 4. Validace sloupců
        missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        if missing_cols:
            logger.error(format_log(
                action="cache_read",
                conid=conid,
                source="cache",
                result="fail",
                message=f"timeframe={timeframe} reason=missing_columns columns={missing_cols}",
            ))
            return None

        # 5. Validace neprázdnosti – technicky čitelný ale prázdný Parquet není platný cache hit
        if df.empty:
            logger.warning(format_log(
                action="cache_read",
                conid=conid,
                source="cache",
                result="miss",
                message=f"timeframe={timeframe} reason=empty_dataframe",
            ))
            return None

        latency_ms = int((time.time() - start) * 1000)
        logger.info(format_log(
            action="cache_read",
            conid=conid,
            source="cache",
            result="success",
            latency_ms=latency_ms,
            rows=len(df),
            message=f"timeframe={timeframe}",
        ))

        return df

    def exists(self, conid: int, timeframe: str = "D1") -> bool:
        """Vrátí True pokud Parquet soubor existuje."""
        return self._parquet_path(conid, timeframe).exists()
