"""
MDSM-Lite – Cache Writer
src/cache/cache_writer.py

Zodpovědnost:
    Atomic zápis cenových dat ve formátu Parquet.
    Neúplný zápis nikdy nepřepíše platnou cache.

Atomic write flow:
    data → prices/{conid}_{timeframe}.tmp
         → replace()
         → prices/{conid}_{timeframe}.parquet

Soubory:
    prices/{conid}_{timeframe}.parquet
    Příklad: prices/756733_D1.parquet
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Cache.Writer")

# ---------------------------------------------------------------------------
# Očekávané sloupce v cenovém DataFrame
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS: list[str] = ["open", "high", "low", "close", "volume"]


class CacheWriteError(Exception):
    """Výjimka pro chyby při zápisu cache."""


class CacheWriter:
    """
    Zapisuje cenová data do Parquet souborů.
    Používá atomic write (tmp → replace) pro ochranu integrity cache.
    """

    def __init__(self, config: Config) -> None:
        self._prices_dir: Path = config.path_cache_prices
        self._prices_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Názvy souborů
    # ------------------------------------------------------------------

    def _parquet_path(self, conid: int, timeframe: str) -> Path:
        return self._prices_dir / f"{conid}_{timeframe}.parquet"

    def _tmp_path(self, conid: int, timeframe: str) -> Path:
        return self._prices_dir / f"{conid}_{timeframe}.tmp"

    # ------------------------------------------------------------------
    # Validace DataFrame před zápisem
    # ------------------------------------------------------------------

    def _validate_dataframe(self, conid: int, timeframe: str, df: pd.DataFrame) -> None:
        """
        Základní validace struktury DataFrame před zápisem.

        Kontroluje:
            1. df není None ani prázdný
            2. df má index
            3. index není prázdný
            4. všechny očekávané sloupce jsou přítomny

        Raises:
            CacheWriteError: při jakékoli neshodě
        """
        if df is None:
            raise CacheWriteError(
                f"DataFrame je None pro conid={conid} timeframe={timeframe}"
            )

        if df.empty:
            raise CacheWriteError(
                f"DataFrame je prázdný pro conid={conid} timeframe={timeframe}"
            )

        if df.index is None or len(df.index) == 0:
            raise CacheWriteError(
                f"DataFrame nemá index pro conid={conid} timeframe={timeframe}"
            )

        missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        if missing_cols:
            raise CacheWriteError(
                f"DataFrame chybí sloupce {missing_cols} "
                f"pro conid={conid} timeframe={timeframe}"
            )

    # ------------------------------------------------------------------
    # Zápis
    # ------------------------------------------------------------------

    def write(
        self,
        conid: int,
        df: pd.DataFrame,
        timeframe: str = "D1",
    ) -> None:
        """
        Zapíše DataFrame do Parquet souboru atomicky.

        Postup:
            1. Validuj strukturu DataFrame
            2. Zapiš do .tmp souboru
            3. Ověř, že .tmp není prázdný
            4. Nahraď finální soubor přes replace()

        Cleanup: .tmp se vždy odstraní při jakémkoli selhání.

        Args:
            conid:     identifikátor instrumentu
            df:        DataFrame s cenovými daty
            timeframe: timeframe (výchozí "D1")

        Raises:
            CacheWriteError: validace nebo zápis selhal
        """
        parquet_path = self._parquet_path(conid, timeframe)
        tmp_path = self._tmp_path(conid, timeframe)

        # Krok 1: Validace DataFrame před zápisem
        try:
            self._validate_dataframe(conid, timeframe, df)
        except CacheWriteError as exc:
            logger.error(format_log(
                action="cache_write",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=validation_failed error={exc}",
            ))
            raise

        # Krok 2–4: Atomic write s důsledným cleanup .tmp
        try:
            # Krok 2: Zápis do .tmp
            df.to_parquet(tmp_path, index=True, engine="pyarrow")

            # Krok 3: Ověř, že .tmp není prázdný
            if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                raise CacheWriteError(
                    f"Zápis do .tmp selhal nebo je soubor prázdný: {tmp_path}"
                )

            # Krok 4: Atomic replace .tmp → .parquet
            # replace() je bezpečnější než rename() – přepíše existující soubor jedním krokem
            tmp_path.replace(parquet_path)

        except CacheWriteError:
            # Vlastní chyba – ukliď .tmp a zaloguj
            self._cleanup_tmp(tmp_path, conid, timeframe)
            logger.error(format_log(
                action="cache_write",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} reason=write_failed path={parquet_path}",
            ))
            raise

        except Exception as exc:
            # Neočekávaná chyba – ukliď .tmp, zaloguj a přebal na CacheWriteError
            self._cleanup_tmp(tmp_path, conid, timeframe)
            logger.error(format_log(
                action="cache_write",
                conid=conid,
                result="fail",
                message=f"timeframe={timeframe} error={exc}",
            ))
            raise CacheWriteError(
                f"Zápis cache selhal pro conid={conid} timeframe={timeframe}: {exc}"
            ) from exc

        logger.info(format_log(
            action="cache_write",
            conid=conid,
            result="success",
            rows=len(df),
            message=f"timeframe={timeframe} path={parquet_path}",
        ))

    # ------------------------------------------------------------------
    # Interní pomocné metody
    # ------------------------------------------------------------------

    def _cleanup_tmp(self, tmp_path: Path, conid: int, timeframe: str) -> None:
        """Odstraní .tmp soubor pokud existuje. Nikdy nevyhodí výjimku."""
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception as exc:
            logger.warning(format_log(
                action="cache_write",
                conid=conid,
                result="skip",
                message=f"timeframe={timeframe} reason=tmp_cleanup_failed error={exc}",
            ))

    def exists(self, conid: int, timeframe: str = "D1") -> bool:
        """Vrátí True pokud Parquet soubor existuje."""
        return self._parquet_path(conid, timeframe).exists()
