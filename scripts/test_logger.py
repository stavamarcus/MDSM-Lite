"""
MDSM-Lite – Logger Test Script
scripts/test_logger.py

Ověřuje:
    1. inicializaci loggeru
    2. zápis do system.log a errors.log
    3. správný formát záznamu (strukturovaný pipe formát)
    4. přítomnost request_id uvnitř logu
    5. přítomnost error záznamu v errors.log
    6. že INFO záznamy neprosáknou do errors.log
    7. přítomnost warning záznamu v system.log

Spuštění:
    python scripts/test_logger.py
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging, get_logger, format_log


def check(label: str, condition: bool) -> bool:
    status = "[OK]  " if condition else "[FAIL]"
    print(f"{status} {label}")
    return condition


def main() -> None:
    # ------------------------------------------------------------------
    # 1. Načti config
    # ------------------------------------------------------------------
    config = load_config()
    print(f"Config načten: {config}\n")

    # ------------------------------------------------------------------
    # 2. Inicializuj logging
    # ------------------------------------------------------------------
    setup_logging(
        log_dir=config.path_logs,
        level=config.log_level,
    )

    # ------------------------------------------------------------------
    # 3. Získej loggery pro různé vrstvy
    # ------------------------------------------------------------------
    access_logger   = get_logger("AccessLayer")
    provider_logger = get_logger("Provider")
    cache_logger    = get_logger("Cache")

    # ------------------------------------------------------------------
    # 4. Zapiš testovací záznamy
    # ------------------------------------------------------------------
    start = time.time()

    access_logger.info(format_log(
        action="fetch_history",
        conid=756733,
        source="ibkr",
        result="success",
        latency_ms=1840,
        rows=1258,
        request_id="req-0001",
    ))

    access_logger.info(format_log(
        action="cache_read",
        conid=756733,
        source="cache",
        result="success",
        latency_ms=12,
        rows=1258,
        request_id="req-0002",
    ))

    provider_logger.warning(format_log(
        action="tws_connect",
        result="fail",
        message="TWS neodpovídá na portu 7497",
        request_id="req-0003",
    ))

    cache_logger.error(format_log(
        action="cache_write",
        conid=756733,
        result="fail",
        message="Atomický zápis selhal – disk plný",
        request_id="req-0004",
    ))

    cache_logger.debug(format_log(
        action="cache_write",
        conid=756733,
        result="skip",
        message="DEBUG záznam – viditelný pouze v konzoli při level=DEBUG",
    ))

    # ------------------------------------------------------------------
    # 5. Počkej na flush handlerů na disk
    # ------------------------------------------------------------------
    time.sleep(0.2)

    elapsed = int((time.time() - start) * 1000)

    # ------------------------------------------------------------------
    # 6. Ověření výsledků
    # ------------------------------------------------------------------
    system_log = config.path_logs / "system.log"
    errors_log = config.path_logs / "errors.log"

    print("\n--- Výsledky testu ---")

    results: list[bool] = []

    # Existence souborů
    results.append(check("system.log existuje", system_log.exists()))
    results.append(check("errors.log existuje", errors_log.exists()))

    # Obsah system.log
    if system_log.exists():
        system_content = system_log.read_text(encoding="utf-8")
        results.append(check(
            "system.log obsahuje 'fetch_history'",
            "fetch_history" in system_content,
        ))
        results.append(check(
            "system.log obsahuje 'cache_read'",
            "cache_read" in system_content,
        ))
        results.append(check(
            "system.log obsahuje 'tws_connect' (warning)",
            "tws_connect" in system_content,
        ))
        results.append(check(
            "system.log obsahuje request_id=req-0001",
            "request_id=req-0001" in system_content,
        ))
        results.append(check(
            "system.log obsahuje pipe formát ' | '",
            " | " in system_content,
        ))
        results.append(check(
            "system.log obsahuje 'conid=756733'",
            "conid=756733" in system_content,
        ))
    else:
        results.extend([False] * 6)

    # Obsah errors.log
    if errors_log.exists():
        errors_content = errors_log.read_text(encoding="utf-8")
        results.append(check(
            "errors.log obsahuje 'cache_write'",
            "cache_write" in errors_content,
        ))
        results.append(check(
            "errors.log obsahuje request_id=req-0004",
            "request_id=req-0004" in errors_content,
        ))
        results.append(check(
            "errors.log neobsahuje 'fetch_history' (INFO do errors.log nepatří)",
            "fetch_history" not in errors_content,
        ))
        results.append(check(
            "errors.log neobsahuje 'tws_connect' (WARNING do errors.log nepatří)",
            "tws_connect" not in errors_content,
        ))
    else:
        results.extend([False] * 4)

    # ------------------------------------------------------------------
    # 7. Souhrn
    # ------------------------------------------------------------------
    passed = sum(results)
    total  = len(results)

    print(f"\n{'='*40}")
    print(f"Výsledek: {passed}/{total} testů prošlo")
    print(f"Čas:      {elapsed} ms")
    print(f"Log dir:  {config.path_logs}")
    print(f"{'='*40}")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
