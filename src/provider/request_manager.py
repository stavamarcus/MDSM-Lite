"""
MDSM-Lite – Request Manager
src/provider/request_manager.py

Zodpovědnost:
    Process-wide Singleton. Řídí celý lifecycle každého IBKR požadavku.
    Serializuje requesty, zajišťuje pacing protection, čeká na dokončení
    a vrací kompletní DataFrame vyšší vrstvě.

    RequestManager NEŘEŠÍ:
        - nízkoúrovňovou komunikaci s IBKR (to je TWSProvider)
        - čtení/zápis cache (to je Cache Layer)
        - rozhodování o cache hit/miss (to je Access Layer)

Použití:
    manager = RequestManager.get_instance(config)
    manager.connect()
    df = manager.fetch_historical(conid=756733, duration="1 Y", end_date="20260310")
"""

from __future__ import annotations

import re
import threading
import time
from typing import Optional

import pandas as pd

from src.provider.tws_provider import TWSProvider
from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Provider.RequestManager")

# ---------------------------------------------------------------------------
# Pacing protection – konzervativní throttling mezi historical requesty
# Milestone 0.1: bezpečná hodnota, v budoucnu konfigurovatelná
# ---------------------------------------------------------------------------
_PACING_INTERVAL_SECONDS = 10

# Validní formáty end_date
_END_DATE_PATTERN_SHORT = re.compile(r"^\d{8}$")                    # YYYYMMDD
_END_DATE_PATTERN_LONG  = re.compile(r"^\d{8} \d{2}:\d{2}:\d{2}$") # YYYYMMDD HH:MM:SS


class RequestError(Exception):
    """Výjimka pro chyby při zpracování IBKR requestu."""


class RequestManager:
    """
    Process-wide Singleton.

    Zajišťuje:
        - jedno připojení k TWS pro celý proces
        - serializaci všech requestů (threading.Lock)
        - pacing protection mezi historical requesty
        - timeout handling
        - sestavení DataFrame z per-request bufferu TWSProvider
    """

    _instance: Optional[RequestManager] = None
    _instance_lock = threading.Lock()

    def __init__(self, config: Config) -> None:
        self._config = config
        self._provider = TWSProvider(config)
        self._lock = threading.Lock()
        self._req_id_counter: int = 1
        self._last_request_time: float = 0.0
        self._timeout: int = config.tws_timeout_seconds

    # ------------------------------------------------------------------
    # Singleton přístup
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls, config: Config) -> RequestManager:
        """Vrátí process-wide instanci RequestManager. Vytvoří při prvním volání."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(config)
                    logger.info(format_log(
                        action="request_manager_init",
                        result="success",
                        message="Singleton vytvořen",
                    ))
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Resetuje Singleton – pouze pro testovací účely."""
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance._provider.safe_disconnect()
            cls._instance = None

    # ------------------------------------------------------------------
    # Připojení
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Připojí TWSProvider k TWS pokud ještě není připraven.

        Raises:
            ConnectionError: připojení selhalo
        """
        if not self._provider.is_ready():
            self._provider.connect_and_run()

    def disconnect(self) -> None:
        """Odpojí TWSProvider od TWS."""
        self._provider.safe_disconnect()

    def is_connected(self) -> bool:
        """Vrátí True pokud je provider připraven přijímat requesty."""
        return self._provider.is_ready()

    # ------------------------------------------------------------------
    # Hlavní fetch metoda
    # ------------------------------------------------------------------

    def fetch_historical(
        self,
        conid: int,
        duration: str,
        end_date: str,
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: int = 1,
    ) -> pd.DataFrame:
        """
        Stáhne historická data pro daný instrument.

        Flow:
            1. Ověř readiness přes is_ready()
            2. Validuj end_date formát
            3. Zamkni Lock – žádné paralelní requesty
            4. Pacing protection
            5. Přiděl reqId, inicializuj buffer a event
            6. Odešli request – aktualizuj pacing timer hned po odeslání
            7. Čekej na completion event s timeoutem
            8. Zkontroluj chybu z TWS
            9. Sestav DataFrame
            10. Cleanup (vždy ve finally)

        Args:
            conid:        IBKR contract ID
            duration:     délka historie, např. "1 Y", "6 M", "3 M"
            end_date:     "YYYYMMDD" nebo "YYYYMMDD HH:MM:SS"
            bar_size:     velikost baru (výchozí "1 day")
            what_to_show: typ dat (výchozí "TRADES")
            use_rth:      1 = pouze regular trading hours

        Returns:
            pd.DataFrame s OHLCV daty, index = datetime

        Raises:
            ConnectionError: provider není připraven
            RequestError:    neplatný formát, timeout, IBKR chyba, prázdná data
        """
        # 1. Readiness check přes is_ready()
        if not self._provider.is_ready():
            raise ConnectionError(
                "RequestManager není připojen k TWS. Zavolej nejdřív connect()."
            )

        # 2. Validace end_date formátu
        end_date_time = self._format_end_date(end_date)

        with self._lock:
            # 3. Pacing protection
            self._wait_for_pacing()

            # 4. Přiděl reqId
            req_id = self._next_req_id()

            fetch_start = time.time()
            logger.info(format_log(
                action="fetch_historical",
                conid=conid,
                result="start",
                request_id=str(req_id),
                message=f"duration={duration} end={end_date_time}",
            ))

            # 5. Inicializuj per-request buffer a event
            completion_event = self._provider.init_request(req_id)

            try:
                # 6. Odešli request
                self._provider.request_historical_data(
                    req_id=req_id,
                    conid=conid,
                    end_date_time=end_date_time,
                    duration_str=duration,
                    bar_size=bar_size,
                    what_to_show=what_to_show,
                    use_rth=use_rth,
                )

                # Pacing timer se aktualizuje hned po odeslání requestu,
                # ne až po dokončení – pacing chrání frekvenci odesílání
                self._last_request_time = time.time()

                # 7. Čekej na completion event s timeoutem
                completed = completion_event.wait(timeout=self._timeout)

                if not completed:
                    logger.error(format_log(
                        action="fetch_historical",
                        conid=conid,
                        result="fail",
                        request_id=str(req_id),
                        message=f"reason=timeout timeout={self._timeout}s",
                    ))
                    raise RequestError(
                        f"Timeout ({self._timeout}s) při čekání na historická data "
                        f"conid={conid} req_id={req_id}"
                    )

                # 8. Zkontroluj chybu z TWS
                error = self._provider.get_request_error(req_id)
                if error:
                    logger.error(format_log(
                        action="fetch_historical",
                        conid=conid,
                        result="fail",
                        request_id=str(req_id),
                        message=f"reason=ibkr_error error={error}",
                    ))
                    raise RequestError(
                        f"IBKR vrátil chybu pro conid={conid} req_id={req_id}: {error}"
                    )

                # 9. Sestav DataFrame
                bars = self._provider.get_request_data(req_id)
                try:
                    df = self._bars_to_dataframe(bars, conid, req_id)
                except RequestError as exc:
                    logger.error(format_log(
                        action="fetch_historical",
                        conid=conid,
                        result="fail",
                        request_id=str(req_id),
                        message=f"reason=empty_data error={exc}",
                    ))
                    raise

                latency_ms = int((time.time() - fetch_start) * 1000)
                logger.info(format_log(
                    action="fetch_historical",
                    conid=conid,
                    source="ibkr",
                    result="success",
                    latency_ms=latency_ms,
                    rows=len(df),
                    request_id=str(req_id),
                ))

                return df

            finally:
                # Vždy ukliď per-request stav v TWSProvider
                self._provider.cleanup_request(req_id)

    # ------------------------------------------------------------------
    # Interní pomocné metody
    # ------------------------------------------------------------------

    def _next_req_id(self) -> int:
        """Vrátí další unikátní reqId."""
        req_id = self._req_id_counter
        self._req_id_counter += 1
        return req_id

    def _wait_for_pacing(self) -> None:
        """
        Konzervativní pacing protection.
        Čeká pokud od posledního odeslání requestu neuplynul bezpečný interval.
        """
        if self._last_request_time == 0.0:
            return

        elapsed = time.time() - self._last_request_time
        wait_time = _PACING_INTERVAL_SECONDS - elapsed

        if wait_time > 0:
            logger.info(format_log(
                action="pacing_wait",
                result="skip",
                message=f"čekám {wait_time:.1f}s (pacing protection)",
            ))
            time.sleep(wait_time)

    def _format_end_date(self, end_date: str) -> str:
        """
        Validuje a normalizuje end_date na formát
        "YYYYMMDD HH:MM:SS US/Eastern".

        Přijímá pouze:
            - "YYYYMMDD"                        → doplní " 23:59:59 US/Eastern"
            - "YYYYMMDD HH:MM:SS"              → doplní " US/Eastern"
            - "YYYYMMDD HH:MM:SS US/Eastern"   → vrátí beze změny

        Raises:
            RequestError: neplatný formát
        """
        end_date = end_date.strip()

        if _END_DATE_PATTERN_SHORT.match(end_date):
            return f"{end_date} 23:59:59 US/Eastern"

        if _END_DATE_PATTERN_LONG.match(end_date):
            return f"{end_date} US/Eastern"

        if end_date.endswith(" US/Eastern"):
            base = end_date[:-11].strip()
            if _END_DATE_PATTERN_LONG.match(base):
                return end_date

        raise RequestError(
            f"Neplatný formát end_date: '{end_date}'. "
            f"Povoleno: 'YYYYMMDD', 'YYYYMMDD HH:MM:SS' "
            f"nebo 'YYYYMMDD HH:MM:SS US/Eastern'."
        )

    def _bars_to_dataframe(
        self, bars: list, conid: int, req_id: int
    ) -> pd.DataFrame:
        """
        Převede seznam BarData objektů na pd.DataFrame s OHLCV daty.

        Raises:
            RequestError: žádné bary nebyly přijaty
        """
        if not bars:
            raise RequestError(
                f"Žádná data nebyla přijata pro conid={conid} req_id={req_id}. "
                f"Ověř IBKR permissions nebo zadaný rozsah dat."
            )

        records = []
        for bar in bars:
            records.append({
                "date":   bar.date,
                "open":   float(bar.open),
                "high":   float(bar.high),
                "low":    float(bar.low),
                "close":  float(bar.close),
                "volume": int(bar.volume),
            })

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        return df
