"""
MDSM-Lite – Universe Loader
src/utils/universe_loader.py

Zodpovědnost:
    Načíst universe.csv, validovat strukturu a data, poskytnout
    ostatním vrstvám seznam instrumentů ve stabilním formátu.
    Universe se načte pouze jednou a drží se v paměti.
    conid je nastaven jako index pro rychlý lookup.

API:
    loader = UniverseLoader(config)
    loader.get_all()              -> pd.DataFrame (celé universe)
    loader.get_active()           -> pd.DataFrame (pouze active_flag=True)
    loader.get_by_conid(756733)   -> dict | None
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("UniverseLoader")

# ---------------------------------------------------------------------------
# Povinné sloupce
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: list[str] = [
    "conid",
    "ticker",
    "exchange",
    "currency",
    "instrument_type",
    "sector",
    "industry",
    "active_flag",
]

# Textové sloupce, které se stripují
_STRING_COLUMNS: list[str] = [
    "ticker",
    "exchange",
    "currency",
    "instrument_type",
    "sector",
    "industry",
]

# Textové sloupce, které nesmí být prázdné po normalizaci
_REQUIRED_TEXT_COLUMNS: list[str] = [
    "ticker",
    "exchange",
    "currency",
    "instrument_type",
]


class UniverseError(Exception):
    """Výjimka pro chyby při načítání nebo validaci universe."""


class UniverseLoader:
    """
    Načte a validuje universe.csv.
    Universe se načte pouze jednou – při prvním volání load().
    Všechny metody pracují s daty v paměti.
    conid je index DataFramu pro rychlý lookup přes .loc[].
    """

    def __init__(self, config: Config) -> None:
        self._path: Path = config.path_universe
        self._df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Načtení a validace
    # ------------------------------------------------------------------

    def load(self) -> None:
        """
        Načte universe.csv, validuje a uloží do paměti.
        Pokud bylo již načteno, nic nedělá.

        Raises:
            UniverseError: soubor neexistuje / chybí sloupce / neplatná data
        """
        if self._df is not None:
            return

        logger.info(format_log(
            action="universe_load",
            result="start",
            message=f"path={self._path}",
        ))

        self._df = self._read_and_validate()

        logger.info(format_log(
            action="universe_load",
            result="success",
            rows=len(self._df),
            message=f"active={self._df['active_flag'].sum()}",
        ))

    def _read_and_validate(self) -> pd.DataFrame:
        """Načte CSV a provede kompletní validaci. Vrátí čistý DataFrame."""

        # 1. Existence souboru
        if not self._path.exists():
            raise UniverseError(f"Universe soubor nenalezen: {self._path}")

        # 2. Načtení CSV – bez dtype, conid převedeme ručně
        try:
            df = pd.read_csv(self._path)
        except Exception as exc:
            raise UniverseError(f"Chyba při čtení universe CSV: {exc}") from exc

        # 3. Prázdný soubor
        if df.empty:
            raise UniverseError("Universe soubor je prázdný.")

        # 4. Povinné sloupce
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            raise UniverseError(
                f"Universe CSV chybí povinné sloupce: {missing_cols}"
            )

        # 5. Ověření, že conid jsou skutečně celá čísla (ne floaty jako 756733.5)
        numeric_conid = pd.to_numeric(df["conid"], errors="raise")
        non_integer_mask = numeric_conid != numeric_conid.apply(
            lambda x: int(x) if pd.notna(x) else x
        )
        if non_integer_mask.any():
            bad_values = numeric_conid[non_integer_mask].tolist()
            raise UniverseError(
                f"Sloupec conid obsahuje neceločíselné hodnoty: {bad_values}. "
                f"Hodnoty jako 756733.5 nejsou povoleny."
            )
        df["conid"] = numeric_conid.astype(int)

        # 6. conid nesmí být null nebo nulový
        if df["conid"].isnull().any():
            raise UniverseError("Universe obsahuje záznamy s prázdným conid.")
        if (df["conid"] == 0).any():
            raise UniverseError("Universe obsahuje záznamy s conid=0.")

        # 7. Duplicitní conid
        duplicates = df[df["conid"].duplicated()]["conid"].tolist()
        if duplicates:
            raise UniverseError(
                f"Universe obsahuje duplicitní conid: {duplicates}"
            )

        # 8. Strip whitespace ze všech textových sloupců
        # Zabraňuje chybám "AAPL " != "AAPL" při dotazech na IBKR API
        for col in _STRING_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # 9. Povinné textové sloupce nesmí být prázdné po normalizaci
        for col in _REQUIRED_TEXT_COLUMNS:
            empty_mask = df[col].isin(["", "nan", "None", "NaN"])
            null_mask = df[col].isnull()
            invalid_mask = empty_mask | null_mask
            if invalid_mask.any():
                bad_conids = df.loc[invalid_mask, "conid"].tolist()
                raise UniverseError(
                    f"Sloupec '{col}' obsahuje prázdné hodnoty "
                    f"u těchto conid: {bad_conids}"
                )

        # 10. Normalizace active_flag → bool
        df["active_flag"] = df["active_flag"].apply(_normalize_active_flag)

        # 11. Nastav conid jako index pro rychlý lookup přes .loc[]
        df.set_index("conid", inplace=True)

        return df

    # ------------------------------------------------------------------
    # Veřejné API
    # ------------------------------------------------------------------

    def get_all(self) -> pd.DataFrame:
        """
        Vrátí celé universe jako DataFrame.
        conid je index.
        """
        self._ensure_loaded()
        return self._df.copy()

    def get_active(self) -> pd.DataFrame:
        """
        Vrátí pouze aktivní instrumenty (active_flag=True).
        conid je index.
        """
        self._ensure_loaded()
        return self._df[self._df["active_flag"]].copy()

    def get_by_conid(self, conid: int) -> Optional[dict]:
        """
        Vrátí jeden instrument jako slovník, nebo None pokud neexistuje.
        Lookup přes index .loc[] – rychlý O(1).

        Args:
            conid: identifikátor instrumentu

        Returns:
            dict se všemi sloupci (conid je součástí dict) nebo None
        """
        self._ensure_loaded()
        if conid not in self._df.index:
            return None
        row = self._df.loc[conid]
        result = row.to_dict()
        result["conid"] = conid  # přidej conid zpět (je index)
        return result

    # ------------------------------------------------------------------
    # Interní pomocné metody
    # ------------------------------------------------------------------

    def _ensure_loaded(self) -> None:
        """Zajistí, že universe je načteno. Pokud ne, načte ho."""
        if self._df is None:
            self.load()

    def __repr__(self) -> str:
        if self._df is None:
            return "UniverseLoader(not loaded)"
        return (
            f"UniverseLoader(instruments={len(self._df)}, "
            f"active={self._df['active_flag'].sum()})"
        )


# ---------------------------------------------------------------------------
# Pomocná funkce – normalizace active_flag
# ---------------------------------------------------------------------------

def _normalize_active_flag(value: object) -> bool:
    """
    Převede různé formáty active_flag na bool.

    Podporované hodnoty:
        True, "true", "True", "TRUE", 1, "1"  → True
        False, "false", "False", "FALSE", 0, "0" → False

    Raises:
        UniverseError: neznámá hodnota
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 1:
            return True
        if value == 0:
            return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1"):
            return True
        if normalized in ("false", "0"):
            return False

    raise UniverseError(
        f"Neplatná hodnota active_flag: '{value}'. "
        f"Povoleno: True/False, 1/0, 'true'/'false'."
    )
