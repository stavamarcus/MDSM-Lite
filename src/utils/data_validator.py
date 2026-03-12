"""
MDSM-Lite – Data Validator
src/utils/data_validator.py

Zodpovědnost:
    Validace historických dat po každém fetchi z IBKR.
    Pokud validace selže, data nesmí být zapsána do cache.

    DataValidator NEŘEŠÍ:
        - fetch dat (to je RequestManager)
        - zápis do cache (to je CacheWriter)
        - hraniční pokrytí rozsahu (to je MarketCalendar.covers_range())
    To je odpovědností Access Layer.

Použití:
    validator = DataValidator(market_calendar)
    result = validator.validate(df, conid, required_start, required_end)
    if not result.is_valid:
        # nezapisovat do cache
        logger.error(result.reason)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from src.utils.market_calendar import MarketCalendar
from src.utils.logger import get_logger, format_log

logger = get_logger("Utils.DataValidator")


@dataclass
class ValidationResult:
    """Výsledek validace datasetu."""
    is_valid: bool
    reason: str = ""

    def __bool__(self) -> bool:
        return self.is_valid


class DataValidator:
    """
    Validátor historických dat po fetchi z IBKR.

    Kontroly (v tomto pořadí):
        1. dataset není prázdný
        2. index je DatetimeIndex
        3. index neobsahuje duplicitní data
        4. očekávané sloupce jsou přítomny
        5. počáteční datum odpovídá očekávanému prvnímu obchodnímu dni
        6. koncové datum odpovídá očekávanému poslednímu obchodnímu dni
        7. dataset obsahuje všechny očekávané obchodní dny (bez mezer)
    """

    EXPECTED_COLUMNS = {"open", "high", "low", "close", "volume"}

    def __init__(self, calendar: MarketCalendar) -> None:
        self._calendar = calendar

    def validate(
        self,
        df: pd.DataFrame,
        conid: int,
        required_start: date,
        required_end: date,
    ) -> ValidationResult:
        """
        Spustí všechny validační kontroly na datasetu.

        Args:
            df:             DataFrame s historickými daty (index = DatetimeIndex)
            conid:          identifikátor instrumentu (pro logování)
            required_start: očekávaný počáteční datum
            required_end:   očekávaný koncový datum

        Returns:
            ValidationResult – is_valid=True pokud všechny kontroly prošly
        """
        checks = [
            self._check_not_empty,
            self._check_datetime_index,
            self._check_no_duplicates,
            self._check_columns,
            self._check_start_date,
            self._check_end_date,
            self._check_no_gaps,
        ]

        for check in checks:
            result = check(df, required_start, required_end)
            if not result.is_valid:
                logger.error(format_log(
                    action="data_validation",
                    conid=conid,
                    result="fail",
                    message=result.reason,
                ))
                return result

        logger.info(format_log(
            action="data_validation",
            conid=conid,
            result="success",
            rows=len(df),
            message=f"range={required_start} – {required_end}",
        ))
        return ValidationResult(is_valid=True)

    # ------------------------------------------------------------------
    # Jednotlivé kontroly
    # ------------------------------------------------------------------

    def _check_not_empty(
        self, df: pd.DataFrame, _start: date, _end: date
    ) -> ValidationResult:
        """Dataset nesmí být prázdný."""
        if df is None or len(df) == 0:
            return ValidationResult(
                is_valid=False,
                reason="Dataset je prázdný – žádná data nebyla přijata.",
            )
        return ValidationResult(is_valid=True)

    def _check_datetime_index(
        self, df: pd.DataFrame, _start: date, _end: date
    ) -> ValidationResult:
        """Index musí být DatetimeIndex."""
        if not isinstance(df.index, pd.DatetimeIndex):
            return ValidationResult(
                is_valid=False,
                reason=f"Index není DatetimeIndex: {type(df.index).__name__}.",
            )
        return ValidationResult(is_valid=True)

    def _check_no_duplicates(
        self, df: pd.DataFrame, _start: date, _end: date
    ) -> ValidationResult:
        """Index nesmí obsahovat duplicitní timestampy."""
        duplicates = df.index[df.index.duplicated()].date
        if len(duplicates) > 0:
            return ValidationResult(
                is_valid=False,
                reason=(
                    f"Dataset obsahuje duplicitní timestampy: "
                    f"{sorted(set(duplicates))}. "
                    f"Duplicitní řádky mohou rozbít cache a analytické moduly."
                ),
            )
        return ValidationResult(is_valid=True)

    def _check_columns(
        self, df: pd.DataFrame, _start: date, _end: date
    ) -> ValidationResult:
        """Musí být přítomny všechny očekávané sloupce."""
        missing = self.EXPECTED_COLUMNS - set(df.columns)
        if missing:
            return ValidationResult(
                is_valid=False,
                reason=f"Chybějící sloupce: {sorted(missing)}.",
            )
        return ValidationResult(is_valid=True)

    def _check_start_date(
        self, df: pd.DataFrame, required_start: date, _end: date
    ) -> ValidationResult:
        """
        První datum v datasetu nesmí být pozdější než očekávaný první
        obchodní den na nebo po required_start.
        """
        actual_start = df.index[0].date()
        expected_start = self._next_trading_day_or_same(required_start)

        if actual_start > expected_start:
            return ValidationResult(
                is_valid=False,
                reason=(
                    f"Počáteční datum dat ({actual_start}) je pozdější "
                    f"než očekávaný první obchodní den ({expected_start})."
                ),
            )
        return ValidationResult(is_valid=True)

    def _check_end_date(
        self, df: pd.DataFrame, _start: date, required_end: date
    ) -> ValidationResult:
        """
        Poslední datum v datasetu nesmí být dřívější než očekávaný poslední
        obchodní den na nebo před required_end.
        """
        actual_end = df.index[-1].date()
        expected_end = self._prev_trading_day_or_same(required_end)

        if actual_end < expected_end:
            return ValidationResult(
                is_valid=False,
                reason=(
                    f"Koncové datum dat ({actual_end}) je dřívější "
                    f"než očekávaný poslední obchodní den ({expected_end})."
                ),
            )
        return ValidationResult(is_valid=True)

    def _check_no_gaps(
        self, df: pd.DataFrame, required_start: date, required_end: date
    ) -> ValidationResult:
        """
        Kontroluje vnitřní kontinuitu datasetu porovnáním skutečných dat
        proti očekávaným obchodním dnům z MarketCalendar.

        Logika:
            1. vezme první a poslední datum z datasetu
            2. přes trading_days_in_range() sestaví seznam všech očekávaných
               obchodních dní v tomto rozsahu
            3. porovná je se skutečnými daty v indexu
            4. pokud nějaký obchodní den chybí, validace selže

        Tato kontrola odhalí chybějící obchodní dny uvnitř rozsahu,
        které by hrubá kontrola kalendářní mezery přehlédla.
        """
        actual_dates = set(df.index.date)

        # Rozsah pro kontrolu kontinuity je omezen na skutečná data v datasetu
        data_start = df.index[0].date()
        data_end   = df.index[-1].date()

        expected_days = self._calendar.trading_days_in_range(data_start, data_end)

        missing = [d for d in expected_days if d not in actual_dates]

        if missing:
            # Zobrazíme max 5 chybějících dní v chybové zprávě
            preview = missing[:5]
            more = len(missing) - len(preview)
            more_str = f" ... a dalších {more}" if more > 0 else ""
            return ValidationResult(
                is_valid=False,
                reason=(
                    f"Chybějící obchodní dny uvnitř rozsahu "
                    f"({len(missing)} celkem): "
                    f"{preview}{more_str}."
                ),
            )

        return ValidationResult(is_valid=True)

    # ------------------------------------------------------------------
    # Pomocné metody
    # ------------------------------------------------------------------

    def _next_trading_day_or_same(self, d: date) -> date:
        """Vrátí d pokud je obchodní den, jinak nejbližší následující."""
        while not self._calendar.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def _prev_trading_day_or_same(self, d: date) -> date:
        """Vrátí d pokud je obchodní den, jinak nejbližší předchozí."""
        while not self._calendar.is_trading_day(d):
            d -= timedelta(days=1)
        return d
