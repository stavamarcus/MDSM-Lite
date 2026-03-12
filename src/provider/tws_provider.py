"""
MDSM-Lite – TWS Provider
src/provider/tws_provider.py

Zodpovědnost:
    Nízkoúrovňová komunikace s IBKR TWS API.
    Připojení, odpojení, odeslání historical data requestů,
    zpracování callbacků a udržování per-request bufferů.

    TWSProvider NEŘEŠÍ:
        - pacing
        - timeout orchestration
        - serializaci requestů
    To je odpovědností RequestManager.

Použití:
    Pouze přes RequestManager. Nikdy přímo z jiných vrstev.

Poznámka k Contract modelu:
    V milestone 0.1 je contract sestavován s dočasnými výchozími hodnotami
    (secType="STK", exchange="SMART", currency="USD").
    *** TEMPORARY SIMPLIFICATION – milestone 0.1 only ***
    V budoucí verzi musí request_historical_data() přijímat contract parametry
    z universe metadat (exchange, currency, instrument_type).
"""

from __future__ import annotations

import threading
from typing import Optional

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Provider.TWS")


class TWSProvider(EWrapper, EClient):
    """
    Nízkoúrovňový IBKR klient.

    Dědí z EWrapper (příjem callbacků) a EClient (odesílání requestů).
    Udržuje per-request buffery a completion eventy pro každý reqId.
    """

    def __init__(self, config: Config) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._host: str = config.tws_host
        self._port: int = config.tws_port
        self._client_id: int = config.tws_client_id

        # Connection timeout – pouze pro fázi připojení
        # Historical request timeout řídí RequestManager
        self._connection_timeout: int = config.tws_timeout_seconds

        # Per-request izolace
        self._request_buffers: dict[int, list] = {}
        self._request_events: dict[int, threading.Event] = {}
        self._request_errors: dict[int, str] = {}

        # Thread pro EClient message loop
        self._thread: Optional[threading.Thread] = None

        # Connection state
        self._connected_event = threading.Event()
        self._is_connected: bool = False

    # ------------------------------------------------------------------
    # Připojení a odpojení
    # ------------------------------------------------------------------

    def connect_and_run(self) -> None:
        """
        Připojí se k TWS a spustí message loop v samostatném threadu.

        Ochrana proti dvojímu volání:
            Pokud je provider již připojen nebo thread běží, vyhodí RuntimeError.

        State reset:
            Před každým novým pokusem o připojení se resetuje connection state.

        Raises:
            RuntimeError:    provider je již připojen nebo thread běží
            ConnectionError: připojení selhalo nebo timeout
        """
        # Ochrana proti dvojímu spuštění
        if self._is_connected or (self._thread is not None and self._thread.is_alive()):
            raise RuntimeError(
                "TWSProvider je již připojen nebo message loop thread běží. "
                "Zavolej safe_disconnect() před novým připojením."
            )

        # Reset connection state před novým pokusem
        self._reset_connection_state()

        logger.info(format_log(
            action="tws_connect",
            result="start",
            message=f"host={self._host} port={self._port} client_id={self._client_id}",
        ))

        self.connect(self._host, self._port, self._client_id)

        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

        # Čekej na potvrzení připojení přes nextValidId callback
        connected = self._connected_event.wait(timeout=self._connection_timeout)
        if not connected:
            self.disconnect()
            raise ConnectionError(
                f"Připojení k TWS selhalo (timeout {self._connection_timeout}s). "
                f"Ověř, že TWS běží na {self._host}:{self._port}."
            )

        logger.info(format_log(
            action="tws_connect",
            result="success",
            message=f"host={self._host} port={self._port}",
        ))

    def safe_disconnect(self) -> None:
        """Odpojí se od TWS. Nevyhodí výjimku."""
        try:
            if self.isConnected():
                self.disconnect()
                logger.info(format_log(
                    action="tws_disconnect",
                    result="success",
                ))
        except Exception as exc:
            logger.warning(format_log(
                action="tws_disconnect",
                result="fail",
                message=f"error={exc}",
            ))
        finally:
            self._is_connected = False

    def is_ready(self) -> bool:
        """
        Vrátí True pokud je provider připojen a připraven přijímat requesty.

        Kombinuje kontrolu EClient.isConnected() a interního _is_connected flagu.
        RequestManager volá tuto metodu před odesláním každého requestu.
        """
        return self._is_connected and self.isConnected()

    # ------------------------------------------------------------------
    # Interní reset stavu
    # ------------------------------------------------------------------

    def _reset_connection_state(self) -> None:
        """
        Resetuje connection state před novým pokusem o připojení.
        Zabrání přenosu starého stavu z předchozího selhání.
        """
        self._connected_event.clear()
        self._is_connected = False
        self._thread = None
        # Uklid zbytků po předchozích requestech
        self._request_buffers.clear()
        self._request_events.clear()
        self._request_errors.clear()

    # ------------------------------------------------------------------
    # Per-request správa bufferů
    # ------------------------------------------------------------------

    def init_request(self, req_id: int) -> threading.Event:
        """
        Inicializuje buffer a completion event pro nový request.

        Args:
            req_id: unikátní ID requestu přidělené RequestManagerem

        Returns:
            threading.Event – RequestManager čeká na jeho set()
        """
        self._request_buffers[req_id] = []
        self._request_events[req_id] = threading.Event()
        self._request_errors.pop(req_id, None)
        return self._request_events[req_id]

    def get_request_data(self, req_id: int) -> list:
        """Vrátí nasbírané bary pro daný reqId."""
        return self._request_buffers.get(req_id, [])

    def get_request_error(self, req_id: int) -> Optional[str]:
        """Vrátí chybovou zprávu pro daný reqId nebo None."""
        return self._request_errors.get(req_id)

    def cleanup_request(self, req_id: int) -> None:
        """Uklízí buffer a event pro daný reqId po dokončení."""
        self._request_buffers.pop(req_id, None)
        self._request_events.pop(req_id, None)
        self._request_errors.pop(req_id, None)

    # ------------------------------------------------------------------
    # Odeslání historical data requestu
    # ------------------------------------------------------------------

    def request_historical_data(
        self,
        req_id: int,
        conid: int,
        end_date_time: str,
        duration_str: str,
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: int = 1,
        # *** TEMPORARY SIMPLIFICATION – milestone 0.1 only ***
        # V budoucí verzi musí tyto parametry pocházet z universe metadat.
        # Architektura je připravena – parametry jsou explicitní, ne hardcoded uvnitř.
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> None:
        """
        Odešle reqHistoricalData na IBKR.

        Args:
            req_id:        unikátní ID requestu
            conid:         IBKR contract ID
            end_date_time: koncové datum "YYYYMMDD HH:MM:SS"
            duration_str:  délka historie, např. "1 Y", "6 M"
            bar_size:      velikost baru, výchozí "1 day"
            what_to_show:  typ dat, výchozí "TRADES"
            use_rth:       1 = pouze regular trading hours
            sec_type:      typ cenného papíru (dočasně "STK")
            exchange:      burza (dočasně "SMART")
            currency:      měna (dočasně "USD")

        Poznámka:
            sec_type, exchange, currency jsou dočasně výchozí pro milestone 0.1.
            V budoucnu budou předávány z universe metadat přes RequestManager.
        """
        contract = Contract()
        contract.conId = conid   # pozor: velke I – IBKR Python wrapper rozlisuje velikost
        contract.secType = sec_type
        contract.exchange = "SMART"

        contract.currency = currency

        self.reqHistoricalData(
            req_id,
            contract,
            end_date_time,
            duration_str,
            bar_size,
            what_to_show,
            use_rth,
            1,      # formatDate: 1 = string
            False,  # keepUpToDate
            [],     # chartOptions
        )

        logger.info(format_log(
            action="historical_request",
            conid=conid,
            result="sent",
            request_id=str(req_id),
            message=f"end={end_date_time} duration={duration_str} bar={bar_size}",
        ))

    # ------------------------------------------------------------------
    # EWrapper callbacky
    # ------------------------------------------------------------------

    def nextValidId(self, orderId: int) -> None:
        """Callback po úspěšném připojení – signalizuje handshake."""
        self._is_connected = True
        self._connected_event.set()
        logger.info(format_log(
            action="tws_handshake",
            result="success",
            message=f"next_valid_id={orderId}",
        ))

    def historicalData(self, reqId: int, bar: BarData) -> None:
        """Callback pro každý bar – přidá do per-request bufferu."""
        if reqId in self._request_buffers:
            self._request_buffers[reqId].append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """Callback signalizující konec streamu – nastaví completion event."""
        bar_count = len(self._request_buffers.get(reqId, []))
        logger.info(format_log(
            action="historical_data_end",
            result="success",
            request_id=str(reqId),
            rows=bar_count,
            message=f"start={start} end={end}",
        ))
        if reqId in self._request_events:
            self._request_events[reqId].set()

    def error(self, reqId: int, errorCode: int, errorString: str,
              advancedOrderRejectJson: str = "") -> None:
        """
        Callback pro chyby z TWS.

        Informační kódy (< 2000 nebo ve whitelist) se pouze logují.
        Skutečné chyby se ukládají do _request_errors a nastavují event.
        """
        INFO_CODES = {2104, 2106, 2107, 2108, 2158}

        if errorCode in INFO_CODES:
            logger.info(format_log(
                action="tws_info",
                result="skip",
                message=f"code={errorCode} msg={errorString}",
            ))
            return

        logger.error(format_log(
            action="tws_error",
            result="fail",
            request_id=str(reqId) if reqId != -1 else "n/a",
            message=f"code={errorCode} msg={errorString}",
        ))

        if reqId != -1 and reqId in self._request_events:
            self._request_errors[reqId] = f"code={errorCode} msg={errorString}"
            self._request_events[reqId].set()

    def connectionClosed(self) -> None:
        """Callback při uzavření spojení."""
        self._is_connected = False
        logger.warning(format_log(
            action="tws_connection_closed",
            result="fail",
            message="Spojení s TWS bylo uzavřeno.",
        ))
