"""
MDSM-Lite – Access Layer
src/access/access_layer.py

Zodpovědnost:
    Hlavní vstupní bod systému MDSM-Lite pro analytické moduly.
    Koordinuje rozhodování o zdroji dat: cache hit / cache miss / force refresh.
    Nikdy nekomunikuje přímo s IBKR – vždy přes RequestManager.

    Rozhodovací flow:
        1. validace vstupního rozsahu (start <= end, effective_end >= start)
        2. conid existuje v universe?              → ne  → AccessError
        3. timeframe podporovaný?                  → ne  → AccessError
        4. permissions_status = no_permissions?    → ano → AccessError
           (platí vždy, i při force_refresh)
        5. force_refresh=True?                     → ano → fetch
        6. cache pokrývá požadovaný rozsah?        → ne  → fetch
        7. cache_reader.read()                     → None → fetch
        8.                                         → DataFrame → ořez na [start, end]

    Podporované timeframes (milestone 0.1):
        "D1" – denní data

    AccessLayer NEŘEŠÍ:
        - komunikaci s IBKR (to je RequestManager / TWSProvider)
        - čtení/zápis souborů (to je Cache Layer)
        - výpočet svátků (to je MarketCalendar)
        - validaci dat (to je DataValidator)

Použití:
    layer = AccessLayer(config)
    layer.connect()
    df = layer.get_historical(conid=756733, start=date(2025,1,1), end=date(2026,3,10))
"""

from __future__ import annotations

import math
import time
from datetime import date

import pandas as pd

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log
from src.utils.universe_loader import UniverseLoader
from src.utils.market_calendar import MarketCalendar
from src.utils.data_validator import DataValidator
from src.cache.cache_reader import CacheReader
from src.cache.cache_writer import CacheWriter
from src.cache.metadata_manager import MetadataManager
from src.provider.request_manager import RequestManager, RequestError

logger = get_logger("AccessLayer")

# ---------------------------------------------------------------------------
# Podporované timeframes – milestone 0.1
# Mapování timeframe → IBKR bar_size
# ---------------------------------------------------------------------------
_SUPPORTED_TIMEFRAMES: dict[str, str] = {
    "D1": "1 day",
}


class AccessError(Exception):
    """Výjimka pro chyby Access Layer."""


class AccessLayer:
    """
    Hlavní vstupní bod MDSM-Lite.

    Koordinuje:
        - ověření instrumentu přes universe
        - rozhodování cache hit / cache miss / force refresh
        - fetch z IBKR při cache miss
        - validaci dat před zápisem do cache
        - logování cache hit ratio a latence
    """

    def __init__(self, config: Config) -> None:
        self._config    = config
        self._universe  = UniverseLoader(config)
        self._calendar  = MarketCalendar(config)
        self._validator = DataValidator(self._calendar)
        self._metadata  = MetadataManager(config)
        self._reader    = CacheReader(config, self._metadata)
        self._writer    = CacheWriter(config)
        self._manager   = RequestManager.get_instance(config)

    # ------------------------------------------------------------------
    # Připojení
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Připojí RequestManager k TWS."""
        self._manager.connect()

    def disconnect(self) -> None:
        """Odpojí RequestManager od TWS."""
        self._manager.disconnect()

    # ------------------------------------------------------------------
    # Hlavní veřejné API
    # ------------------------------------------------------------------

    def get_historical(
        self,
        conid: int,
        start: date,
        end: date,
        timeframe: str = "D1",
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Vrátí historická data pro daný instrument a rozsah.

        Vrácený DataFrame je vždy ořezán na požadovaný rozsah [start, end].
        Cache nebo fetch může fyzicky pokrývat širší rozsah, ale analytický
        modul dostane pouze data v požadovaném okně.

        Args:
            conid:         IBKR contract ID
            start:         počáteční datum (inclusive)
            end:           koncové datum (inclusive)
            timeframe:     časový rámec – milestone 0.1 podporuje pouze "D1"
            force_refresh: přeskočit cache a stáhnout znovu z IBKR
                           (nepřeskočí ochranu no_permissions)

        Returns:
            pd.DataFrame s OHLCV daty, index = DatetimeIndex,
            ořezaný na [start, end]

        Raises:
            AccessError: neplatný rozsah, neznámý conid, nepodporovaný
                         timeframe, blokované permissions, fetch selhal
        """
        t_start = time.time()

        # ----------------------------------------------------------
        # 1. Validace vstupního rozsahu
        # ----------------------------------------------------------
        if start > end:
            raise AccessError(
                f"Neplatný rozsah: start={start} je po end={end}."
            )

        # Omezte end na poslední platný obchodní den
        last_valid    = self._calendar.last_valid_trading_day()
        effective_end = min(end, last_valid)

        if effective_end < start:
            raise AccessError(
                f"Požadovaný rozsah leží celý v budoucnosti nebo před prvním "
                f"dostupným obchodním dnem: start={start} "
                f"last_valid_trading_day={last_valid}."
            )

        # ----------------------------------------------------------
        # 2. Ověření conid v universe
        # ----------------------------------------------------------
        instrument = self._universe.get_by_conid(conid)
        if instrument is None:
            raise AccessError(
                f"conid={conid} není v universe. "
                f"Ověř universe.csv nebo zadaný conid."
            )

        ticker = instrument.get("ticker", str(conid))

        # ----------------------------------------------------------
        # 3. Validace timeframe
        # ----------------------------------------------------------
        if timeframe not in _SUPPORTED_TIMEFRAMES:
            supported = list(_SUPPORTED_TIMEFRAMES.keys())
            raise AccessError(
                f"Nepodporovaný timeframe: '{timeframe}'. "
                f"Milestone 0.1 podporuje pouze: {supported}."
            )

        logger.info(format_log(
            action="get_historical",
            conid=conid,
            result="start",
            message=f"ticker={ticker} start={start} end={effective_end} "
                    f"timeframe={timeframe} force_refresh={force_refresh}",
        ))

        # ----------------------------------------------------------
        # 4. Permissions check – blokuje fetch vždy, i při force_refresh
        #    Pouze no_permissions blokuje; "error" neblokuje.
        # ----------------------------------------------------------
        if self._metadata.exists(conid, timeframe):
            meta = self._metadata.read(conid, timeframe)
            if meta and meta.get("permissions_status") == "no_permissions":
                logger.warning(format_log(
                    action="get_historical",
                    conid=conid,
                    result="skip",
                    message="permissions_status=no_permissions – "
                            "fetch přeskočen (platí i pro force_refresh). "
                            "Reset přes reset_permissions_status().",
                ))
                raise AccessError(
                    f"conid={conid} má permissions_status='no_permissions'. "
                    f"Fetch přeskočen. Pro reset zavolej reset_permissions_status()."
                )

        # ----------------------------------------------------------
        # 5. Force refresh – přeskočit cache (ale ne permissions ochranu)
        # ----------------------------------------------------------
        if force_refresh:
            logger.info(format_log(
                action="get_historical",
                conid=conid,
                result="skip",
                message="force_refresh=True – přeskakuji cache",
            ))
            df = self._fetch_and_cache(
                conid, start, effective_end, timeframe, t_start
            )
            return self._slice(df, start, effective_end)

        # ----------------------------------------------------------
        # 6. Cache coverage check + autoritativní čtení přes reader
        # ----------------------------------------------------------
        if self._metadata.exists(conid, timeframe):
            meta = self._metadata.read(conid, timeframe)
            if meta and meta.get("is_valid"):
                cache_start = date.fromisoformat(meta["start_date"])
                cache_end   = date.fromisoformat(meta["end_date"])

                if self._calendar.covers_range(
                    cache_start, cache_end, start, effective_end
                ):
                    df = self._reader.read(conid, timeframe)
                    if df is not None:
                        latency_ms = int((time.time() - t_start) * 1000)
                        logger.info(format_log(
                            action="get_historical",
                            conid=conid,
                            source="cache",
                            result="success",
                            latency_ms=latency_ms,
                            rows=len(df),
                            message=f"ticker={ticker} cache_hit=True",
                        ))
                        return self._slice(df, start, effective_end)

                    logger.warning(format_log(
                        action="get_historical",
                        conid=conid,
                        result="skip",
                        message="cache_reader vrátil None – přecházím na fetch",
                    ))

        # ----------------------------------------------------------
        # 7. Cache miss → fetch
        # ----------------------------------------------------------
        df = self._fetch_and_cache(
            conid, start, effective_end, timeframe, t_start
        )
        return self._slice(df, start, effective_end)

    def reset_permissions_status(
        self, conid: int, timeframe: str = "D1"
    ) -> None:
        """
        Resetuje permissions_status na 'ok' v metadatech.
        Umožní Access Layeru znovu pokusit fetch pro daný instrument.

        Raises:
            AccessError: metadata neexistují nebo je nelze načíst
        """
        if not self._metadata.exists(conid, timeframe):
            raise AccessError(
                f"Metadata pro conid={conid} timeframe={timeframe} neexistují."
            )

        meta = self._metadata.read(conid, timeframe)
        if meta is None:
            raise AccessError(
                f"Nepodařilo se načíst metadata pro conid={conid}."
            )

        meta["permissions_status"] = "ok"
        self._metadata.write(conid, meta, timeframe)

        logger.info(format_log(
            action="reset_permissions_status",
            conid=conid,
            result="success",
            message=f"permissions_status reset na 'ok' pro timeframe={timeframe}",
        ))

    # ------------------------------------------------------------------
    # Interní fetch + cache write
    # ------------------------------------------------------------------

    def _fetch_and_cache(
        self,
        conid: int,
        start: date,
        end: date,
        timeframe: str,
        t_start: float,
    ) -> pd.DataFrame:
        """
        Stáhne data z IBKR, validuje je a zapíše do cache.

        Args:
            conid:     IBKR contract ID
            start:     požadovaný počáteční datum
            end:       effective_end (již ořezaný na last_valid_trading_day)
            timeframe: časový rámec
            t_start:   čas začátku celého požadavku (pro latenci)

        Returns:
            pd.DataFrame – kompletní stažený dataset (před ořezem na rozsah)

        Raises:
            AccessError: fetch selhal nebo data neprošla validací
        """
        bar_size     = _SUPPORTED_TIMEFRAMES[timeframe]
        duration     = self._date_range_to_duration(start, end)
        end_date_str = end.strftime("%Y%m%d")

        logger.info(format_log(
            action="fetch_start",
            conid=conid,
            source="ibkr",
            result="start",
            message=f"duration={duration} end={end_date_str} bar_size={bar_size}",
        ))

        try:
            df = self._manager.fetch_historical(
                conid=conid,
                duration=duration,
                end_date=end_date_str,
                bar_size=bar_size,
            )
        except RequestError as exc:
            error_str = str(exc).lower()
            if "no data permissions" in error_str or \
               "no market data permissions" in error_str:
                self._write_permissions_blocked(conid, timeframe)
            raise AccessError(
                f"Fetch selhal pro conid={conid}: {exc}"
            ) from exc
        except ConnectionError as exc:
            raise AccessError(
                f"Připojení k TWS selhalo: {exc}"
            ) from exc

        # Validace dat před zápisem do cache
        validation = self._validator.validate(df, conid, start, end)
        if not validation.is_valid:
            self._write_invalid_metadata(conid, timeframe)
            raise AccessError(
                f"Data pro conid={conid} neprošla validací: {validation.reason}"
            )

        # Zápis dat a metadat do cache
        self._writer.write(conid, df, timeframe)

        metadata = {
            "conid": conid,
            "timeframe": timeframe,
            "cache_format_version": self._config.cache_format_version,
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "start_date": df.index[0].date().isoformat(),
            "end_date": df.index[-1].date().isoformat(),
            "rows": len(df),
            "permissions_status": "ok",
            "is_valid": True,
        }
        self._metadata.write(conid, metadata, timeframe)

        latency_ms = int((time.time() - t_start) * 1000)
        logger.info(format_log(
            action="get_historical",
            conid=conid,
            source="ibkr",
            result="success",
            latency_ms=latency_ms,
            rows=len(df),
            message="cache_hit=False written=True",
        ))

        return df

    # ------------------------------------------------------------------
    # Zápis chybových metadat
    # ------------------------------------------------------------------

    def _write_permissions_blocked(self, conid: int, timeframe: str) -> None:
        """Zapíše permissions_status=no_permissions do metadat."""
        self._write_error_metadata(conid, timeframe, permissions="no_permissions")

    def _write_invalid_metadata(self, conid: int, timeframe: str) -> None:
        """
        Zapíše is_valid=False do metadat při selhání validace dat.
        permissions_status zůstává 'ok' – nejde o problém s oprávněními.
        """
        self._write_error_metadata(conid, timeframe, permissions="ok")

    def _write_error_metadata(
        self, conid: int, timeframe: str, permissions: str
    ) -> None:
        """Sdílená logika pro zápis chybových metadat."""
        metadata = {
            "conid": conid,
            "timeframe": timeframe,
            "cache_format_version": self._config.cache_format_version,
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "start_date": "",
            "end_date": "",
            "rows": 0,
            "permissions_status": permissions,
            "is_valid": False,
        }
        try:
            self._metadata.write(conid, metadata, timeframe)
        except Exception as exc:
            logger.error(format_log(
                action="write_error_metadata",
                conid=conid,
                result="fail",
                message=f"error={exc}",
            ))

    # ------------------------------------------------------------------
    # Pomocné metody
    # ------------------------------------------------------------------

    @staticmethod
    def _slice(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
        """
        Ořeže DataFrame na požadovaný rozsah [start, end] včetně.

        Analytický modul vždy dostane pouze data v požadovaném okně,
        i když cache nebo fetch pokrývá širší rozsah.
        """
        return df.loc[
            pd.Timestamp(start): pd.Timestamp(end)
        ]

    @staticmethod
    def _date_range_to_duration(start: date, end: date) -> str:
        """
        Převede rozsah dat na IBKR duration string.
        Zaokrouhluje konzervativně nahoru (math.ceil), aby fetch vždy
        pokryl celý požadovaný rozsah.

        Logika:
            < 30 dní   → "N D"
            < 365 dní  → "N M"  (ceil na měsíce)
            >= 365 dní → "N Y"  (ceil na roky)
        """
        days = (end - start).days + 1

        if days < 30:
            return f"{days} D"

        months = math.ceil(days / 30)
        if months < 12:
            return f"{months} M"

        years = math.ceil(days / 365)
        return f"{years} Y"
