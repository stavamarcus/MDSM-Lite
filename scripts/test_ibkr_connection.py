"""
MDSM-Lite – IBKR Connection Test
scripts/test_ibkr_connection.py

Ověřuje:
    1. connect_and_run() proběhne bez výjimky
    2. is_ready() = True po připojení
    3. safe_disconnect() proběhne bez výjimky
    4. is_ready() = False po odpojení
    5. Nedostupné TWS vyvolá ConnectionError (ne crash)

Spuštění:
    python scripts/test_ibkr_connection.py

Požadavky:
    TWS nebo IB Gateway musí být spuštěný na portu z config.yaml.
    Test 5 (timeout) se spustí vždy bez ohledu na stav TWS.
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config, Config
from src.utils.logger import setup_logging
from src.provider.tws_provider import TWSProvider


def check(label: str, condition: bool) -> bool:
    status = "[OK]  " if condition else "[FAIL]"
    print(f"{status} {label}")
    return condition


def main() -> None:
    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    config = load_config()
    setup_logging(log_dir=config.path_logs, level=config.log_level)
    print(f"Config načten: {config}")
    print(f"TWS target: {config.tws_host}:{config.tws_port} "
          f"client_id={config.tws_client_id}\n")

    results: list[bool] = []
    start = time.time()

    # ------------------------------------------------------------------
    # Část A: Normální připojení (vyžaduje běžící TWS)
    # ------------------------------------------------------------------
    print("--- Část A: Připojení k TWS ---")
    provider = TWSProvider(config)

    try:
        provider.connect_and_run()
        results.append(check("connect_and_run() proběhl bez výjimky", True))
    except ConnectionError as exc:
        results.append(check(f"connect_and_run() selhal: {exc}", False))
        print("\n[INFO] TWS není dostupné. Přeskakuji testy A2–A4.")
        results.extend([False, False, False])
        _run_timeout_test(results)
        _print_summary(results, start)
        sys.exit(1)

    # Testujeme veřejné API is_ready() – ne interní atributy
    results.append(check(
        "is_ready() = True po připojení",
        provider.is_ready(),
    ))

    # Odpojení
    try:
        provider.safe_disconnect()
        results.append(check("safe_disconnect() proběhl bez výjimky", True))
    except Exception as exc:
        results.append(check(f"safe_disconnect() selhal: {exc}", False))

    time.sleep(0.5)  # krátká pauza pro čisté uzavření spojení

    results.append(check(
        "is_ready() = False po odpojení",
        not provider.is_ready(),
    ))

    # ------------------------------------------------------------------
    # Část B: Nedostupné TWS – timeout test
    # ------------------------------------------------------------------
    print("\n--- Část B: Timeout test (nedostupné TWS) ---")
    _run_timeout_test(results)

    # ------------------------------------------------------------------
    # Souhrn
    # ------------------------------------------------------------------
    _print_summary(results, start)

    if not all(results):
        sys.exit(1)


def _run_timeout_test(results: list) -> None:
    """
    Ověří, že při nedostupném TWS systém vyhodí ConnectionError.
    Vytváří normální TWSProvider přes standardní konstruktor s fake configem.
    """

    class _FakeConfig:
        """Dočasný config s neexistujícím portem a krátkým timeoutem."""
        tws_host = "127.0.0.1"
        tws_port = 19999          # neexistující port
        tws_client_id = 99
        tws_timeout_seconds = 3   # krátký timeout pro test

    # Normální vytvoření přes konstruktor – žádný __new__ hack
    provider_bad = TWSProvider(_FakeConfig())

    got_connection_error = False
    try:
        provider_bad.connect_and_run()
    except ConnectionError:
        got_connection_error = True
    except Exception:
        got_connection_error = False
    finally:
        provider_bad.safe_disconnect()

    results.append(check(
        "Nedostupné TWS vyvolá ConnectionError (ne crash)",
        got_connection_error,
    ))


def _print_summary(results: list, start: float) -> None:
    elapsed = int((time.time() - start) * 1000)
    passed = sum(results)
    total  = len(results)
    print(f"\n{'='*40}")
    print(f"Výsledek:  {passed}/{total} testů prošlo")
    print(f"Čas:       {elapsed} ms")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
