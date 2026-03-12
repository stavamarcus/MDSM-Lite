"""
MDSM-Lite – Cache Layer Test Script
scripts/test_cache_layer.py

Ověřuje:
    CacheWriter:
        1. zápis Parquet (atomic write)
        2. .tmp soubor nevznikne jako finální
    MetadataManager:
        3. zápis a čtení JSON
        4. exists() funguje
        5. is_valid() = True pro platná metadata
        6. is_valid() = False při neshodě cache_format_version
        7. is_valid() = False když is_valid=False v metadatech
        8. is_valid() = False při permissions_status=no_permissions
        9. is_valid() = False při neznámém permissions_status
    CacheReader:
        10. read() vrátí DataFrame
        11. read() vrátí správný počet řádků
        12. read() vrátí správné sloupce
        13. read() vrátí None pro neexistující conid
        14. read() vrátí None když metadata is_valid=False
        15. read() vrátí None pro Parquet s chybějícími sloupci

Spuštění:
    python scripts/test_cache_layer.py
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.cache.metadata_manager import MetadataManager
from src.cache.cache_writer import CacheWriter
from src.cache.cache_reader import CacheReader

# Testovací konstanty
TEST_CONID = 999999
TEST_TIMEFRAME = "D1"


def check(label: str, condition: bool) -> bool:
    status = "[OK]  " if condition else "[FAIL]"
    print(f"{status} {label}")
    return condition


def make_test_df() -> pd.DataFrame:
    """Vytvoří minimální testovací DataFrame s OHLCV daty."""
    dates = pd.date_range("2025-01-02", periods=5, freq="B")
    return pd.DataFrame({
        "open":   [100.0, 101.0, 102.0, 103.0, 104.0],
        "high":   [105.0, 106.0, 107.0, 108.0, 109.0],
        "low":    [99.0,  100.0, 101.0, 102.0, 103.0],
        "close":  [102.0, 103.0, 104.0, 105.0, 106.0],
        "volume": [1000,  1100,  1200,  1300,  1400],
    }, index=dates)


def make_broken_df() -> pd.DataFrame:
    """Vytvoří DataFrame s chybějícím sloupcem 'volume' – simuluje poškozený Parquet."""
    dates = pd.date_range("2025-01-02", periods=5, freq="B")
    return pd.DataFrame({
        "open":  [100.0, 101.0, 102.0, 103.0, 104.0],
        "high":  [105.0, 106.0, 107.0, 108.0, 109.0],
        "low":   [99.0,  100.0, 101.0, 102.0, 103.0],
        "close": [102.0, 103.0, 104.0, 105.0, 106.0],
        # 'volume' záměrně chybí
    }, index=dates)


def make_test_metadata(config, is_valid: bool = True, permissions: str = "ok",
                        version: int = None) -> dict:
    """Vytvoří testovací metadata."""
    return {
        "conid": TEST_CONID,
        "timeframe": TEST_TIMEFRAME,
        "cache_format_version": version if version is not None else config.cache_format_version,
        "last_updated": datetime.now().isoformat(),
        "start_date": "2025-01-02",
        "end_date": "2025-01-08",
        "rows": 5,
        "permissions_status": permissions,
        "is_valid": is_valid,
    }


def cleanup(writer: CacheWriter, meta: MetadataManager,
            conid: int = TEST_CONID) -> None:
    """Odstraní testovací soubory po testu."""
    parquet = writer._parquet_path(conid, TEST_TIMEFRAME)
    metadata = meta._metadata_path(conid, TEST_TIMEFRAME)
    if parquet.exists():
        parquet.unlink()
    if metadata.exists():
        metadata.unlink()


def main() -> None:
    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    config = load_config()
    setup_logging(log_dir=config.path_logs, level=config.log_level)
    print(f"Config načten: {config}\n")

    meta   = MetadataManager(config)
    writer = CacheWriter(config)
    reader = CacheReader(config, meta)

    results: list[bool] = []
    start = time.time()

    # Cleanup před testem
    cleanup(writer, meta)

    # ------------------------------------------------------------------
    # 1–2. CacheWriter
    # ------------------------------------------------------------------
    print("--- CacheWriter ---")
    df_test = make_test_df()
    try:
        writer.write(TEST_CONID, df_test, TEST_TIMEFRAME)
        results.append(check("write() proběhl bez výjimky", True))
    except Exception as exc:
        results.append(check(f"write() selhal: {exc}", False))

    results.append(check(
        "Parquet soubor existuje po write()",
        writer.exists(TEST_CONID, TEST_TIMEFRAME),
    ))

    tmp_path = writer._tmp_path(TEST_CONID, TEST_TIMEFRAME)
    results.append(check(
        ".tmp soubor neexistuje po úspěšném write()",
        not tmp_path.exists(),
    ))

    # ------------------------------------------------------------------
    # 3–9. MetadataManager
    # ------------------------------------------------------------------
    print("\n--- MetadataManager ---")
    test_meta = make_test_metadata(config)
    try:
        meta.write(TEST_CONID, test_meta, TEST_TIMEFRAME)
        results.append(check("metadata write() proběhl bez výjimky", True))
    except Exception as exc:
        results.append(check(f"metadata write() selhal: {exc}", False))

    loaded_meta = meta.read(TEST_CONID, TEST_TIMEFRAME)
    results.append(check(
        "metadata read() vrátí dict",
        isinstance(loaded_meta, dict),
    ))
    results.append(check(
        "metadata obsahuje správný conid",
        loaded_meta is not None and loaded_meta.get("conid") == TEST_CONID,
    ))

    # exists()
    results.append(check(
        "metadata exists() = True po zápisu",
        meta.exists(TEST_CONID, TEST_TIMEFRAME),
    ))
    results.append(check(
        "metadata exists() = False pro neexistující conid",
        not meta.exists(888888, TEST_TIMEFRAME),
    ))

    # is_valid() – platná metadata
    meta.write(TEST_CONID, make_test_metadata(config), TEST_TIMEFRAME)
    results.append(check(
        "is_valid() = True pro platná metadata",
        meta.is_valid(TEST_CONID, TEST_TIMEFRAME),
    ))

    # is_valid() – neshoda cache_format_version
    meta.write(TEST_CONID, make_test_metadata(config, version=999), TEST_TIMEFRAME)
    results.append(check(
        "is_valid() = False při neshodě cache_format_version",
        not meta.is_valid(TEST_CONID, TEST_TIMEFRAME),
    ))

    # is_valid() – is_valid=False v metadatech
    meta.write(TEST_CONID, make_test_metadata(config, is_valid=False), TEST_TIMEFRAME)
    results.append(check(
        "is_valid() = False když is_valid=False v metadatech",
        not meta.is_valid(TEST_CONID, TEST_TIMEFRAME),
    ))

    # is_valid() – blokující permissions_status
    meta.write(TEST_CONID, make_test_metadata(config, permissions="no_permissions"), TEST_TIMEFRAME)
    results.append(check(
        "is_valid() = False při permissions_status=no_permissions",
        not meta.is_valid(TEST_CONID, TEST_TIMEFRAME),
    ))

    # is_valid() – neznámý permissions_status
    meta.write(TEST_CONID, make_test_metadata(config, permissions="unknown"), TEST_TIMEFRAME)
    results.append(check(
        "is_valid() = False při neznámém permissions_status='unknown'",
        not meta.is_valid(TEST_CONID, TEST_TIMEFRAME),
    ))

    # Obnov platná metadata pro čtecí testy
    meta.write(TEST_CONID, make_test_metadata(config), TEST_TIMEFRAME)

    # ------------------------------------------------------------------
    # 10–15. CacheReader
    # ------------------------------------------------------------------
    print("\n--- CacheReader ---")
    df_read = reader.read(TEST_CONID, TEST_TIMEFRAME)
    results.append(check(
        "read() vrátí DataFrame",
        isinstance(df_read, pd.DataFrame),
    ))
    results.append(check(
        "read() vrátí správný počet řádků",
        df_read is not None and len(df_read) == 5,
    ))
    results.append(check(
        "read() vrátí správné sloupce",
        df_read is not None and all(
            c in df_read.columns for c in ["open", "high", "low", "close", "volume"]
        ),
    ))

    # read() – neexistující conid → None
    results.append(check(
        "read() vrátí None pro neexistující conid",
        reader.read(888888, TEST_TIMEFRAME) is None,
    ))

    # read() – invalidní metadata → None
    meta.write(TEST_CONID, make_test_metadata(config, is_valid=False), TEST_TIMEFRAME)
    results.append(check(
        "read() vrátí None když metadata is_valid=False",
        reader.read(TEST_CONID, TEST_TIMEFRAME) is None,
    ))

    # read() – Parquet s chybějícími sloupci → None
    # Zapišeme broken DataFrame přímo přes pyarrow (obejdeme validaci writeru)
    meta.write(TEST_CONID, make_test_metadata(config), TEST_TIMEFRAME)
    broken_df = make_broken_df()
    broken_path = writer._parquet_path(TEST_CONID, TEST_TIMEFRAME)
    broken_df.to_parquet(broken_path, index=True, engine="pyarrow")
    results.append(check(
        "read() vrátí None pro Parquet s chybějícím sloupcem 'volume'",
        reader.read(TEST_CONID, TEST_TIMEFRAME) is None,
    ))

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    cleanup(writer, meta)

    # ------------------------------------------------------------------
    # Souhrn
    # ------------------------------------------------------------------
    elapsed = int((time.time() - start) * 1000)
    passed = sum(results)
    total  = len(results)

    print(f"\n{'='*40}")
    print(f"Výsledek:  {passed}/{total} testů prošlo")
    print(f"Čas:       {elapsed} ms")
    print(f"{'='*40}")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
