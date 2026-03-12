"""
MDSM-Lite – Universe Loader Test Script
scripts/test_universe_loader.py

Ověřuje:
    1. soubor universe.csv existuje
    2. povinné sloupce jsou přítomny (conid jako index)
    3. index se jmenuje 'conid'
    4. conid index není null, nulový ani duplicitní
    5. active_flag je normalizován na bool
    6. get_all() vrátí celé universe
    7. get_active() vrátí pouze aktivní instrumenty
    8. get_by_conid() vrátí správný záznam včetně conid
    9. get_by_conid() vrátí None pro neexistující conid
    10. singleton chování – druhé load() nezmění data

Spuštění:
    python scripts/test_universe_loader.py
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.utils.universe_loader import UniverseLoader, REQUIRED_COLUMNS


def check(label: str, condition: bool) -> bool:
    status = "[OK]  " if condition else "[FAIL]"
    print(f"{status} {label}")
    return condition


def main() -> None:
    # ------------------------------------------------------------------
    # 1. Setup
    # ------------------------------------------------------------------
    config = load_config()
    setup_logging(log_dir=config.path_logs, level=config.log_level)
    print(f"Config načten: {config}\n")

    loader = UniverseLoader(config)
    results: list[bool] = []
    start = time.time()

    # ------------------------------------------------------------------
    # 2. Existence souboru
    # ------------------------------------------------------------------
    print("--- Soubor ---")
    universe_path = config.path_universe
    results.append(check(
        f"universe.csv existuje ({universe_path})",
        universe_path.exists(),
    ))

    if not universe_path.exists():
        print("\n[STOP] universe.csv nenalezen. Nelze pokračovat v testech.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3. Načtení
    # ------------------------------------------------------------------
    print("\n--- Načtení ---")
    try:
        loader.load()
        results.append(check("load() proběhl bez výjimky", True))
    except Exception as exc:
        results.append(check(f"load() selhal: {exc}", False))
        print("\n[STOP] Načtení selhalo. Nelze pokračovat.")
        sys.exit(1)

    df_all = loader.get_all()

    # ------------------------------------------------------------------
    # 4. Index – conid
    # ------------------------------------------------------------------
    print("\n--- Index (conid) ---")
    results.append(check(
        "index se jmenuje 'conid'",
        df_all.index.name == "conid",
    ))
    results.append(check(
        "index neobsahuje null",
        df_all.index.isnull().sum() == 0,
    ))
    results.append(check(
        "index neobsahuje nulu",
        (df_all.index == 0).sum() == 0,
    ))
    results.append(check(
        "index neobsahuje duplicity",
        df_all.index.duplicated().sum() == 0,
    ))

    # ------------------------------------------------------------------
    # 5. Povinné sloupce (conid je index – kontroluje se zvlášť výše)
    # ------------------------------------------------------------------
    print("\n--- Sloupce ---")
    data_columns = [c for c in REQUIRED_COLUMNS if c != "conid"]
    for col in data_columns:
        results.append(check(
            f"sloupec '{col}' existuje",
            col in df_all.columns,
        ))

    results.append(check(
        "universe není prázdné",
        len(df_all) > 0,
    ))

    # ------------------------------------------------------------------
    # 6. active_flag normalizace
    # ------------------------------------------------------------------
    print("\n--- active_flag ---")
    results.append(check(
        "active_flag je datový typ bool",
        all(isinstance(v, bool) for v in df_all["active_flag"]),
    ))

    # ------------------------------------------------------------------
    # 7. API metody
    # ------------------------------------------------------------------
    print("\n--- API ---")
    df_active = loader.get_active()
    results.append(check(
        "get_active() vrátí DataFrame",
        hasattr(df_active, "columns"),
    ))
    results.append(check(
        "get_active() obsahuje pouze active_flag=True",
        all(df_active["active_flag"]) if len(df_active) > 0 else True,
    ))
    results.append(check(
        "get_active() je podmnožina get_all()",
        len(df_active) <= len(df_all),
    ))

    # get_by_conid – použij první conid z indexu
    first_conid = int(df_all.index[0])
    instrument = loader.get_by_conid(first_conid)
    results.append(check(
        f"get_by_conid({first_conid}) vrátí dict",
        isinstance(instrument, dict),
    ))
    results.append(check(
        f"get_by_conid({first_conid}) obsahuje klíč 'conid'",
        instrument is not None and "conid" in instrument,
    ))
    results.append(check(
        f"get_by_conid({first_conid}) má správný conid",
        instrument is not None and instrument["conid"] == first_conid,
    ))
    results.append(check(
        "get_by_conid(999999999) vrátí None",
        loader.get_by_conid(999999999) is None,
    ))

    # ------------------------------------------------------------------
    # 8. Singleton chování – druhé load() nesmí změnit data
    # ------------------------------------------------------------------
    print("\n--- Singleton ---")
    df_before = loader.get_all()
    loader.load()  # druhé volání nesmí znovu číst soubor
    df_after = loader.get_all()

    results.append(check(
        "opakované load() nezmění počet řádků",
        len(df_before) == len(df_after),
    ))
    results.append(check(
        "opakované load() nezmění index",
        list(df_before.index) == list(df_after.index),
    ))
    results.append(check(
        "opakované load() nezmění sloupce",
        list(df_before.columns) == list(df_after.columns),
    ))

    # ------------------------------------------------------------------
    # 9. Souhrn
    # ------------------------------------------------------------------
    elapsed = int((time.time() - start) * 1000)
    passed = sum(results)
    total  = len(results)

    print(f"\n{'='*40}")
    print(f"Universe:  {len(df_all)} instrumentů, {len(df_active)} aktivních")
    print(f"Výsledek:  {passed}/{total} testů prošlo")
    print(f"Čas:       {elapsed} ms")
    print(f"{'='*40}")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
