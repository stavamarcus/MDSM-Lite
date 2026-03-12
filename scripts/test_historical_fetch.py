"""
MDSM-Lite – Historical Fetch Test
scripts/test_historical_fetch.py

Ověřuje:
    1.  get_instance() vrátí objekt RequestManager
    2.  get_instance() vrátí skutečný Singleton (dvě volání = stejná instance)
    3.  connect() proběhne bez výjimky
    4.  is_connected() = True po připojení
    5.  fetch_historical() vrátí neprázdný DataFrame
    6.  DataFrame má správné OHLCV sloupce
    7.  DataFrame má DatetimeIndex
    8.  DataFrame je seřazený vzestupně
    9.  open > 0 pro každý řádek
    10. high > 0 pro každý řádek
    11. low > 0 pro každý řádek
    12. close > 0 pro každý řádek
    13. high >= low pro každý řádek
    14. Neplatný end_date vyvolá RequestError
    15. is_connected() = False po disconnect()

Stavy výsledku:
    PASS – test prošel
    FAIL – test selhal (architektonická nebo logická chyba)
    SKIP – test přeskočen kvůli externímu problému (např. chybí IBKR subscription)

Program ukončí s exit(1) pouze při alespoň jednom FAIL.
SKIP neblokuje úspěšné ukončení testu.

Spuštění:
    python scripts/test_historical_fetch.py

Požadavky:
    TWS nebo IB Gateway musí být spuštěný.
    Instrument s daným conid musí mít aktivní IBKR market data subscription.

Poznámka k testovacím parametrům:
    TEST_CONID je dočasný lokální parametr pro milestone 0.1.
    Musí odpovídat instrumentu dostupnému v universe.csv a aktivní subscription.
    Dlouhodobě by měl být testovací instrument načítán přímo z universe.
"""

import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.provider.request_manager import RequestManager, RequestError


# ------------------------------------------------------------------
# Testovací parametry
# TEMPORARY – milestone 0.1 only
# Sladit s dostupným instrumentem v universe.csv a aktivní subscription.
# ------------------------------------------------------------------
TEST_CONID    = 756733       # Apple Inc. – dočasný testovací instrument
TEST_DURATION = "3 M"        # 3 měsíce historických dat
TEST_END_DATE = "20260310"   # koncové datum


def check(label: str, condition: bool) -> str:
    """Vrátí 'PASS' nebo 'FAIL' podle podmínky."""
    result = "PASS" if condition else "FAIL"
    print(f"[{result}] {label}")
    return result


def skip(label: str) -> str:
    """Vrátí 'SKIP' – test přeskočen kvůli externímu problému."""
    print(f"[SKIP] {label}")
    return "SKIP"


def main() -> None:
    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    config = load_config()
    setup_logging(log_dir=config.path_logs, level=config.log_level)
    print(f"Config načten: {config}")
    print(f"Test instrument: conid={TEST_CONID} duration={TEST_DURATION} "
          f"end={TEST_END_DATE}\n")

    results: list[str] = []
    start = time.time()

    # ------------------------------------------------------------------
    # Singleton test
    # ------------------------------------------------------------------
    print("--- Singleton ---")
    manager = RequestManager.get_instance(config)
    results.append(check(
        "get_instance() vrátí objekt RequestManager",
        isinstance(manager, RequestManager),
    ))

    manager2 = RequestManager.get_instance(config)
    results.append(check(
        "Dvě volání get_instance() vrátí stejnou instanci (Singleton)",
        manager is manager2,
    ))

    # ------------------------------------------------------------------
    # Připojení
    # ------------------------------------------------------------------
    print("\n--- Připojení ---")
    try:
        manager.connect()
        results.append(check("connect() proběhl bez výjimky", True))
    except ConnectionError as exc:
        results.append(check(f"connect() selhal: {exc}", False))
        print("\n[INFO] TWS není dostupné. Přeskakuji fetch testy.")
        _print_summary(results, start)
        RequestManager.reset_instance()
        sys.exit(1)

    results.append(check(
        "is_connected() = True po připojení",
        manager.is_connected(),
    ))

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------
    print("\n--- Fetch: historická data ---")
    df = None
    fetch_ok = False

    try:
        df = manager.fetch_historical(
            conid=TEST_CONID,
            duration=TEST_DURATION,
            end_date=TEST_END_DATE,
        )
        fetch_ok = True
        results.append(check("fetch_historical() proběhl bez výjimky", True))
    except RequestError as exc:
        results.append(check(f"fetch_historical() selhal: {exc}", False))
        print("[INFO] Fetch selhal – dataframe validace přeskočeny "
              "(příčina: externí dostupnost dat, ne architektura).")

    # ------------------------------------------------------------------
    # Validace DataFramu
    # ------------------------------------------------------------------
    print("\n--- Validace DataFramu ---")

    if fetch_ok and df is not None:
        expected_columns = {"open", "high", "low", "close", "volume"}

        results.append(check(
            "DataFrame není prázdný",
            len(df) > 0,
        ))
        results.append(check(
            f"DataFrame má OHLCV sloupce {expected_columns}",
            expected_columns.issubset(set(df.columns)),
        ))
        results.append(check(
            "DataFrame index je DatetimeIndex",
            isinstance(df.index, pd.DatetimeIndex),
        ))
        results.append(check(
            "DataFrame je seřazený vzestupně",
            df.index.is_monotonic_increasing,
        ))
        results.append(check("open > 0 pro každý řádek",  (df["open"]  > 0).all()))
        results.append(check("high > 0 pro každý řádek",  (df["high"]  > 0).all()))
        results.append(check("low > 0 pro každý řádek",   (df["low"]   > 0).all()))
        results.append(check("close > 0 pro každý řádek", (df["close"] > 0).all()))
        results.append(check(
            "high >= low pro každý řádek",
            (df["high"] >= df["low"]).all(),
        ))

        print(f"\n[INFO] Přijato {len(df)} barů "
              f"({df.index[0].date()} – {df.index[-1].date()})")
    else:
        # Fetch selhal z externího důvodu – validační testy jsou SKIP, ne FAIL
        for label in [
            "DataFrame není prázdný",
            "DataFrame má OHLCV sloupce",
            "DataFrame index je DatetimeIndex",
            "DataFrame je seřazený vzestupně",
            "open > 0 pro každý řádek",
            "high > 0 pro každý řádek",
            "low > 0 pro každý řádek",
            "close > 0 pro každý řádek",
            "high >= low pro každý řádek",
        ]:
            results.append(skip(label))

    # ------------------------------------------------------------------
    # Validace neplatného end_date formátu
    # ------------------------------------------------------------------
    print("\n--- Validace end_date ---")
    got_request_error = False
    try:
        manager.fetch_historical(
            conid=TEST_CONID,
            duration="1 M",
            end_date="2026-03-10",   # neplatný formát – pomlčky
        )
    except RequestError:
        got_request_error = True
    except Exception:
        got_request_error = False

    results.append(check(
        "Neplatný end_date vyvolá RequestError",
        got_request_error,
    ))

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------
    print("\n--- Teardown ---")
    manager.disconnect()
    time.sleep(0.5)

    results.append(check(
        "is_connected() = False po disconnect()",
        not manager.is_connected(),
    ))

    RequestManager.reset_instance()

    # ------------------------------------------------------------------
    # Souhrn
    # ------------------------------------------------------------------
    _print_summary(results, start)

    # Exit 1 pouze při alespoň jednom FAIL – SKIP neblokuje
    if "FAIL" in results:
        sys.exit(1)


def _print_summary(results: list[str], start: float) -> None:
    elapsed = int((time.time() - start) * 1000)
    passed  = results.count("PASS")
    failed  = results.count("FAIL")
    skipped = results.count("SKIP")
    total   = len(results)
    print(f"\n{'='*40}")
    print(f"Výsledek:  {passed}/{total} PASS  |  {failed} FAIL  |  {skipped} SKIP")
    print(f"Čas:       {elapsed} ms")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
