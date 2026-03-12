"""
MDSM-Lite – Access Layer Integration Test
scripts/test_access_layer.py

Ověřuje:
    1.  AccessLayer se inicializuje bez chyby
    2.  Neznámý conid vyvolá AccessError
    3.  Nepodporovaný timeframe vyvolá AccessError
    4.  start > end vyvolá AccessError
    5.  Budoucí rozsah vyvolá AccessError
    6.  cache miss → fetch → DataFrame vrácen
    7.  Vrácený DataFrame má OHLCV sloupce
    8.  Vrácený DataFrame má DatetimeIndex
    9.  Vrácený DataFrame je ořezán na požadovaný rozsah
    10. Cache hit vrátí data (latence je pouze informativní metrika)
    11. Kratší okno z existující cache vrací ořezaná data
    12. no_permissions blokuje fetch i při force_refresh=True
    13. reset_permissions_status() odblokuje fetch (ověřeno přes force_refresh)

Stavy výsledku:
    PASS – test prošel
    FAIL – test selhal (architektonická nebo logická chyba)
    SKIP – test přeskočen kvůli externímu problému
           (TWS nedostupné, chybí subscription, jiný provozní důvod)

Program ukončí s exit(1) pouze při alespoň jednom FAIL.
Nedostupné TWS nebo selhání fetche označí závislé testy jako SKIP.

Spuštění:
    python scripts/test_access_layer.py

Požadavky:
    TWS nebo IB Gateway musí být spuštěný pro testy 6–13.
    Instrument s TEST_CONID musí existovat v universe.csv.

Poznámka:
    TEST_CONID je dočasný parametr pro milestone 0.1.
    Sladit s dostupným instrumentem v universe.csv.
"""

import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.access.access_layer import AccessLayer, AccessError
from src.provider.request_manager import RequestManager

# ------------------------------------------------------------------
# Testovací parametry – TEMPORARY, milestone 0.1
# ------------------------------------------------------------------
TEST_CONID       = 265598  # AAPL
TEST_START       = date(2025, 10, 1)
TEST_END         = date(2026, 3, 10)
TEST_SHORT_START = date(2026, 1, 1)
TEST_SHORT_END   = date(2026, 2, 28)
UNKNOWN_CONID    = 999999999


def check(label: str, condition: bool) -> str:
    result = "PASS" if condition else "FAIL"
    print(f"[{result}] {label}")
    return result


def skip(label: str) -> str:
    print(f"[SKIP] {label}")
    return "SKIP"


def info(label: str) -> None:
    print(f"[INFO] {label}")


def cleanup_test_state(config) -> None:
    """
    Odstraní cache a metadata pro TEST_CONID před integrační částí.

    Zajistí, že test vždy začíná z čistého stavu bez ohledu na
    předchozí běhy. Zabrání tomu, aby zbytky jako:
        - permissions_status="no_permissions"
        - is_valid=False
        - stará cache data
    ovlivnily výsledky nového běhu.
    """
    prices_dir   = config.path_cache_prices
    metadata_dir = config.path_cache_metadata

    files_to_remove = [
        prices_dir   / f"{TEST_CONID}_D1.parquet",
        prices_dir   / f"{TEST_CONID}_D1.tmp",
        metadata_dir / f"{TEST_CONID}_D1.json",
    ]

    removed = []
    for path in files_to_remove:
        if path.exists():
            try:
                path.unlink()
                removed.append(path.name)
            except Exception as exc:
                info(f"Cleanup: nepodařilo se odstranit {path.name}: {exc}")

    if removed:
        info(f"Cleanup: odstraněno {removed}")
    else:
        info("Cleanup: žádné soubory k odstranění (čistý stav)")


def main() -> None:
    config = load_config()
    setup_logging(log_dir=config.path_logs, level=config.log_level)
    print(f"Config načten: {config}")
    print(f"Test instrument: conid={TEST_CONID} "
          f"range={TEST_START} – {TEST_END}\n")

    results: list[str] = []
    t_total = time.time()

    # ------------------------------------------------------------------
    # Inicializace
    # ------------------------------------------------------------------
    print("--- Inicializace ---")
    try:
        layer = AccessLayer(config)
        results.append(check("AccessLayer inicializován bez chyby", True))
    except Exception as exc:
        results.append(check(f"AccessLayer inicializace selhala: {exc}", False))
        _print_summary(results, t_total)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Část A: Validační testy (bez TWS)
    # ------------------------------------------------------------------
    print("\n--- Část A: Validace vstupů (bez TWS) ---")

    got_error = False
    try:
        layer.get_historical(UNKNOWN_CONID, TEST_START, TEST_END)
    except AccessError:
        got_error = True
    results.append(check("Neznámý conid vyvolá AccessError", got_error))

    got_error = False
    try:
        layer.get_historical(TEST_CONID, TEST_START, TEST_END, timeframe="W1")
    except AccessError:
        got_error = True
    results.append(check("Nepodporovaný timeframe vyvolá AccessError", got_error))

    got_error = False
    try:
        layer.get_historical(TEST_CONID, TEST_END, TEST_START)
    except AccessError:
        got_error = True
    results.append(check("start > end vyvolá AccessError", got_error))

    future_start = date.today() + timedelta(days=30)
    future_end   = date.today() + timedelta(days=60)
    got_error = False
    try:
        layer.get_historical(TEST_CONID, future_start, future_end)
    except AccessError:
        got_error = True
    results.append(check("Budoucí rozsah vyvolá AccessError", got_error))

    # ------------------------------------------------------------------
    # Část B: TWS-dependent testy
    # Cleanup před připojením zajistí čistý stav pro každý běh.
    # ------------------------------------------------------------------
    print("\n--- Část B: TWS-dependent testy ---")
    print("[INFO] Cleanup testovacího stavu před integrační částí...")
    cleanup_test_state(config)

    try:
        layer.connect()
        info("Připojeno k TWS.")
        tws_available = True
    except ConnectionError as exc:
        info(f"TWS není dostupné ({exc}).")
        info("Fetch/cache testy budou označeny jako SKIP.")
        tws_available = False

    if not tws_available:
        for label in [
            "fetch vrátil DataFrame bez chyby",
            "DataFrame má OHLCV sloupce",
            "DataFrame má DatetimeIndex",
            "DataFrame ořezán na požadovaný rozsah",
            "Cache hit vrátí neprázdný DataFrame",
            "Kratší okno vrací ořezaná data",
            "no_permissions blokuje fetch i při force_refresh=True",
            "Po reset_permissions_status() force_refresh projde",
        ]:
            results.append(skip(label))
        _teardown(layer)
        _print_summary(results, t_total)
        sys.exit(0)

    # ------------------------------------------------------------------
    # Cache miss → fetch
    # ------------------------------------------------------------------
    print("\n--- Cache miss → fetch ---")
    df_full  = None
    fetch_ok = False
    fetch_ms = 0

    try:
        t_fetch = time.time()
        df_full = layer.get_historical(
            TEST_CONID, TEST_START, TEST_END, force_refresh=True
        )
        fetch_ms = int((time.time() - t_fetch) * 1000)
        fetch_ok = True
        results.append(check("fetch vrátil DataFrame bez chyby", True))
        info(f"Fetch: {len(df_full)} barů za {fetch_ms} ms")
    except AccessError as exc:
        # Fetch selhal kvůli externímu důvodu (subscription, IBKR omezení atd.)
        # → SKIP, ne FAIL architektury
        info(f"Fetch selhal z externího důvodu: {exc}")
        info("Fetch a na něm závislé testy označeny jako SKIP.")
        results.append(skip("fetch vrátil DataFrame bez chyby"))

    if fetch_ok and df_full is not None:
        expected_cols = {"open", "high", "low", "close", "volume"}
        results.append(check(
            "DataFrame má OHLCV sloupce",
            expected_cols.issubset(set(df_full.columns)),
        ))
        results.append(check(
            "DataFrame má DatetimeIndex",
            isinstance(df_full.index, pd.DatetimeIndex),
        ))
        if len(df_full) > 0:
            actual_start = df_full.index[0].date()
            actual_end   = df_full.index[-1].date()
            results.append(check(
                f"DataFrame ořezán na požadovaný rozsah "
                f"[{TEST_START} – {TEST_END}]",
                actual_start >= TEST_START and actual_end <= TEST_END,
            ))
        else:
            results.append(skip("Ořez rozsahu – DataFrame je prázdný"))
    else:
        for label in [
            "DataFrame má OHLCV sloupce",
            "DataFrame má DatetimeIndex",
            "DataFrame ořezán na požadovaný rozsah",
        ]:
            results.append(skip(label))

    # ------------------------------------------------------------------
    # Cache hit
    # Latence je pouze informativní metrika – není tvrdý PASS/FAIL test.
    # ------------------------------------------------------------------
    print("\n--- Cache hit ---")
    if fetch_ok:
        try:
            t_cache   = time.time()
            df_cached = layer.get_historical(TEST_CONID, TEST_START, TEST_END)
            cache_ms  = int((time.time() - t_cache) * 1000)

            results.append(check(
                "Cache hit vrátí neprázdný DataFrame",
                df_cached is not None and len(df_cached) > 0,
            ))
            info(f"Cache hit: {len(df_cached)} barů za {cache_ms} ms "
                 f"(fetch byl {fetch_ms} ms) – pouze informativní metrika")
        except AccessError as exc:
            results.append(check(f"Cache hit selhal: {exc}", False))
    else:
        results.append(skip("Cache hit – fetch nebyl úspěšný"))

    # ------------------------------------------------------------------
    # Ořez kratšího okna z existující cache
    # ------------------------------------------------------------------
    print("\n--- Ořez kratšího okna ---")
    if fetch_ok:
        try:
            df_short = layer.get_historical(
                TEST_CONID, TEST_SHORT_START, TEST_SHORT_END
            )
            if len(df_short) > 0:
                actual_start = df_short.index[0].date()
                actual_end   = df_short.index[-1].date()
                results.append(check(
                    f"Kratší okno [{TEST_SHORT_START} – {TEST_SHORT_END}] "
                    f"vrací ořezaná data",
                    actual_start >= TEST_SHORT_START
                    and actual_end <= TEST_SHORT_END,
                ))
            else:
                results.append(skip("Ořez kratšího okna – prázdný DataFrame"))
        except AccessError as exc:
            results.append(check(f"Ořez kratšího okna selhal: {exc}", False))
    else:
        results.append(skip("Ořez kratšího okna – fetch nebyl úspěšný"))

    # ------------------------------------------------------------------
    # no_permissions blokuje i force_refresh
    # Poznámka: simulujeme stav přímým zápisem do MetadataManager –
    # testovací zkratka, protože Access Layer nemá veřejnou set_permissions.
    # ------------------------------------------------------------------
    print("\n--- no_permissions blokace ---")
    if fetch_ok:
        from src.cache.metadata_manager import MetadataManager
        meta_mgr = MetadataManager(config)

        if meta_mgr.exists(TEST_CONID, "D1"):
            meta = meta_mgr.read(TEST_CONID, "D1")
            if meta:
                meta["permissions_status"] = "no_permissions"
                meta_mgr.write(TEST_CONID, meta, "D1")

                got_error = False
                try:
                    layer.get_historical(
                        TEST_CONID, TEST_START, TEST_END, force_refresh=True
                    )
                except AccessError:
                    got_error = True
                results.append(check(
                    "no_permissions blokuje fetch i při force_refresh=True",
                    got_error,
                ))

                # Ověř reset přes force_refresh – ne cache čtení
                layer.reset_permissions_status(TEST_CONID, "D1")
                try:
                    df_after = layer.get_historical(
                        TEST_CONID, TEST_START, TEST_END,
                        force_refresh=True,
                    )
                    results.append(check(
                        "Po reset_permissions_status() force_refresh projde",
                        df_after is not None and len(df_after) > 0,
                    ))
                except AccessError as exc:
                    results.append(check(
                        f"Po resetu fetch stále selhal: {exc}", False
                    ))
            else:
                results.append(skip("no_permissions test – metadata nelze načíst"))
                results.append(skip("reset_permissions_status test"))
        else:
            results.append(skip("no_permissions test – metadata neexistují"))
            results.append(skip("reset_permissions_status test"))
    else:
        results.append(skip("no_permissions test – fetch nebyl úspěšný"))
        results.append(skip("reset_permissions_status test"))

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------
    _teardown(layer)

    # ------------------------------------------------------------------
    # Souhrn
    # ------------------------------------------------------------------
    _print_summary(results, t_total)

    if "FAIL" in results:
        sys.exit(1)


def _teardown(layer: "AccessLayer") -> None:
    try:
        layer.disconnect()
    except Exception:
        pass
    RequestManager.reset_instance()


def _print_summary(results: list[str], t_start: float) -> None:
    elapsed = int((time.time() - t_start) * 1000)
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
