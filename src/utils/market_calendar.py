"""
MDSM-Lite – Market Calendar
src/utils/market_calendar.py

Zodpovědnost:
    Výpočet obchodních dní, svátků NYSE a validace časového rozsahu cache.
    Access Layer používá tento modul pro rozhodování o pokrytí cache.

    MarketCalendar NEŘEŠÍ:
        - čtení ani zápis cache
        - rozhodování o fetch
        - validaci vnitřní kontinuity dat (to je data_validator.py)
    To je odpovědností Access Layer.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.utils.config_loader import Config
from src.utils.logger import get_logger, format_log

logger = get_logger("Utils.MarketCalendar")

# ---------------------------------------------------------------------------
# NYSE svátky – pevné datum (month, day)
# Veterans Day záměrně NENÍ v seznamu – NYSE na Veterans Day standardně nezavírá.
# ---------------------------------------------------------------------------
_FIXED_HOLIDAYS: list[tuple[int, int]] = [
    (1, 1),   # New Year's Day
    (6, 19),  # Juneteenth National Independence Day
    (7, 4),   # Independence Day
    (12, 25), # Christmas Day
]


# ---------------------------------------------------------------------------
# Good Friday – závisí na datu Velikonoc (Computus algoritmus)
# ---------------------------------------------------------------------------

def _easter(year: int) -> date:
    """
    Vrátí datum Velikonoční neděle pro daný rok.
    Algoritmus: Anonymous Gregorian (Meeus/Jones/Butcher).
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day   = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _good_friday(year: int) -> date:
    """Vrátí datum Velkého pátku (2 dny před Velikonoční nedělí)."""
    return _easter(year) - timedelta(days=2)


# ---------------------------------------------------------------------------
# NYSE svátky – plovoucí (počítají se každý rok)
# ---------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Vrátí n-tý výskyt daného weekday (0=Po ... 6=Ne) v měsíci."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Vrátí poslední výskyt daného weekday v měsíci."""
    last = date(year, month + 1, 1) - timedelta(days=1) if month < 12 \
        else date(year, 12, 31)
    delta = (last.weekday() - weekday) % 7
    return last - timedelta(days=delta)


def _floating_holidays(year: int) -> list[date]:
    """Vrátí seznam plovoucích svátků NYSE pro daný rok."""
    return [
        _good_friday(year),                 # Good Friday
        _nth_weekday(year, 1, 0, 3),        # MLK Day – 3. pondělí v lednu
        _nth_weekday(year, 2, 0, 3),        # Presidents Day – 3. pondělí v únoru
        _last_weekday(year, 5, 0),          # Memorial Day – poslední pondělí v květnu
        _nth_weekday(year, 9, 0, 1),        # Labor Day – 1. pondělí v září
        _nth_weekday(year, 11, 3, 4),        # Thanksgiving – 4. ctvrtek v listopadu
    ]


def _observed(holiday: date) -> date:
    """
    Pokud svátek připadne na sobotu, slaví se v pátek.
    Pokud na neděli, slaví se v pondělí.
    """
    if holiday.weekday() == 5:  # sobota
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:  # neděle
        return holiday + timedelta(days=1)
    return holiday


def _nyse_holidays(year: int) -> set[date]:
    """Vrátí set všech NYSE svátků pro daný rok (s observed pravidlem)."""
    holidays: set[date] = set()

    for month, day in _FIXED_HOLIDAYS:
        try:
            holidays.add(_observed(date(year, month, day)))
        except ValueError:
            pass

    for h in _floating_holidays(year):
        holidays.add(h)

    return holidays


class MarketCalendar:
    """
    Kalendář NYSE obchodních dní.

    Zodpovědnosti:
        - výpočet posledního platného obchodního dne
        - ověření, zda cache pokrývá požadovaný rozsah (hraniční pokrytí)
        - generování seznamu obchodních dní v rozsahu

    Poznámka k validaci:
        Tento modul kontroluje hraniční pokrytí cache (start a end datum).
        Vnitřní kontinuitu dat (mezery uvnitř rozsahu) ověřuje data_validator.py.
    """

    def __init__(self, config: Config) -> None:
        self._tz = ZoneInfo(config.exchange_timezone)
        self._market_close_time = self._parse_time(config.market_close_time)
        self._ingest_buffer_minutes = config.ingest_buffer_minutes
        self._holiday_cache: dict[int, set[date]] = {}

    # ------------------------------------------------------------------
    # Veřejné API
    # ------------------------------------------------------------------

    def last_valid_trading_day(self, reference: datetime | None = None) -> date:
        """
        Vrátí poslední platný obchodní den.

        Logika:
            - pokud je aktuální čas v EST po (market_close + ingest_buffer),
              dnešek je kandidát
            - jinak se bere předchozí obchodní den
            - výsledek musí být obchodní den (ne víkend, ne svátek)

        Args:
            reference: čas pro výpočet (výchozí: now() v exchange timezone)

        Returns:
            date – poslední platný obchodní den
        """
        if reference is None:
            reference = datetime.now(tz=self._tz)
        else:
            if reference.tzinfo is None:
                reference = reference.replace(tzinfo=self._tz)

        cutoff = datetime.combine(
            reference.date(),
            self._market_close_time,
            tzinfo=self._tz,
        ) + timedelta(minutes=self._ingest_buffer_minutes)

        candidate = reference.date() if reference >= cutoff \
            else reference.date() - timedelta(days=1)

        return self._prev_trading_day_or_same(candidate)

    def is_trading_day(self, d: date) -> bool:
        """Vrátí True pokud je datum obchodní den NYSE."""
        if d.weekday() >= 5:
            return False
        return d not in self._get_holidays(d.year)

    def trading_days_in_range(self, start: date, end: date) -> list[date]:
        """
        Vrátí seznam všech obchodních dní v rozsahu <start, end> včetně.

        Args:
            start: počáteční datum (inclusive)
            end:   koncové datum (inclusive)

        Returns:
            list[date] seřazený vzestupně
        """
        if start > end:
            return []

        result: list[date] = []
        current = start
        while current <= end:
            if self.is_trading_day(current):
                result.append(current)
            current += timedelta(days=1)
        return result

    def covers_range(
        self,
        cache_start: date,
        cache_end: date,
        required_start: date,
        required_end: date,
    ) -> bool:
        """
        Vrátí True pokud cache pokrývá celý požadovaný rozsah.

        Tato metoda kontroluje pouze hraniční pokrytí:
            - cache_start <= required_start
            - cache_end >= required_end

        Vnitřní kontinuitu dat (mezery uvnitř rozsahu) tato metoda
        nekontroluje. Tu ověřuje data_validator.py po každém fetchi.

        Args:
            cache_start:    první datum v cache
            cache_end:      poslední datum v cache
            required_start: požadovaný počátek
            required_end:   požadovaný konec

        Returns:
            bool
        """
        if cache_start > required_start:
            logger.info(format_log(
                action="cache_coverage_check",
                result="miss",
                message=f"cache_start={cache_start} > required_start={required_start}",
            ))
            return False

        if cache_end < required_end:
            logger.info(format_log(
                action="cache_coverage_check",
                result="miss",
                message=f"cache_end={cache_end} < required_end={required_end}",
            ))
            return False

        logger.info(format_log(
            action="cache_coverage_check",
            result="success",
            message=f"cache [{cache_start} – {cache_end}] "
                    f"pokrývá [{required_start} – {required_end}]",
        ))
        return True

    def count_trading_days(self, start: date, end: date) -> int:
        """Vrátí počet obchodních dní v rozsahu <start, end> včetně."""
        return len(self.trading_days_in_range(start, end))

    # ------------------------------------------------------------------
    # Interní pomocné metody
    # ------------------------------------------------------------------

    def _get_holidays(self, year: int) -> set[date]:
        """Vrátí svátky pro daný rok (s cache)."""
        if year not in self._holiday_cache:
            self._holiday_cache[year] = _nyse_holidays(year)
        return self._holiday_cache[year]

    def _prev_trading_day_or_same(self, d: date) -> date:
        """Vrátí d pokud je obchodní den, jinak předchozí obchodní den."""
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    @staticmethod
    def _parse_time(time_str: str) -> time:
        """
        Parsuje čas ve formátu "HH:MM".

        Raises:
            ValueError: neplatný formát
        """
        try:
            parts = time_str.strip().split(":")
            return time(int(parts[0]), int(parts[1]))
        except (IndexError, ValueError) as exc:
            raise ValueError(
                f"Neplatný formát market_close_time: '{time_str}'. "
                f"Očekáváno 'HH:MM'."
            ) from exc
