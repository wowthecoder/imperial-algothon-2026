"""Fair value computation for CMI Exchange products.

Fetches real-world data from:
- Open-Meteo: weather (free, no key)
- EA Flood Monitoring: Thames tidal levels (free, no key)
- AeroDataBox via RapidAPI: Heathrow flights (requires key, ~150 req/month)

Settlement formulas:
  TIDE_SPOT  = abs(tide_at_noon_mm)
  TIDE_SWING = Σ strangle(20cm, 25cm) over 15-min tidal diffs across 24h
  WX_SPOT    = temp_F × humidity_% at noon
  WX_SUM     = Σ(temp_F × humidity_%) / 100 over 24h (96 observations)
  LHR_COUNT  = total arrivals + departures during 24h session
  LHR_INDEX  = abs(Σ(arr-dep)/(arr+dep) per 30-min window) × 100
  LON_ETF    = TIDE_SPOT + WX_SPOT + LHR_COUNT
  LON_FLY    = 2×Put(6200) + Call(6200) − 2×Call(6600) + 3×Call(7000) on LON_ETF
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LONDON_LAT, LONDON_LON = 51.5074, -0.1278
LONDON_TZ = ZoneInfo("Europe/London")
THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"
AERODATABOX_HOST = "aerodatabox.p.rapidapi.com"
AIRPORT = "LHR"
SESSION_HOURS = 24

# Settlement time: 12:00 London time (Europe/London)
SETTLEMENT_HOUR = 12
SETTLEMENT_MINUTE = 0

# LON_FLY option strikes
FLY_STRIKES = {"put1": 6200, "call1": 6200, "call2": 6600, "call3": 7000}


def current_time(tz=timezone.utc) -> datetime:
    """Centralized wall-clock access for consistent time handling."""
    return datetime.now(tz=tz)


def next_settlement_noon(now_dt: datetime) -> datetime:
    next_noon = now_dt.replace(
        hour=SETTLEMENT_HOUR,
        minute=SETTLEMENT_MINUTE,
        second=0,
        microsecond=0,
    )
    if next_noon <= now_dt:
        next_noon += timedelta(days=1)
    return next_noon


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FairValueEstimate:
    """Current fair value estimates for all products."""
    fv: dict[str, float] = field(default_factory=dict)
    confidence: dict[str, float] = field(default_factory=dict)  # 0.0 = low, 1.0 = high
    last_data_update: float = 0.0  # monotonic time of last full data refresh

    def spread_for(self, product: str, base_narrow: float = 3.0, base_wide: float = 10.0) -> float:
        """Return quoting spread (half-width) based on confidence."""
        conf = self.confidence.get(product, 0.0)
        # Linear interpolation: low confidence → wide, high confidence → narrow
        return base_wide - conf * (base_wide - base_narrow)

    def is_stale(self, max_age_seconds: float = 120.0) -> bool:
        return (time.monotonic() - self.last_data_update) > max_age_seconds


# ---------------------------------------------------------------------------
# Weather (Open-Meteo)
# ---------------------------------------------------------------------------

def get_weather(past_steps: int = 96, forecast_steps: int = 96) -> pd.DataFrame:
    """Fetch 15-min resolution weather for London.

    Returns DataFrame with columns:
        time, temperature (°C), humidity (%), wind_speed, precipitation,
        cloud_cover, visibility, apparent_temperature
    """
    variables = (
        "temperature_2m,apparent_temperature,relative_humidity_2m,"
        "precipitation,wind_speed_10m,cloud_cover,visibility"
    )
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LONDON_LAT,
            "longitude": LONDON_LON,
            "minutely_15": variables,
            "past_minutely_15": past_steps,
            "forecast_minutely_15": forecast_steps,
            "timezone": "Europe/London",
        },
        timeout=(5, 30),
    )
    resp.raise_for_status()
    m = resp.json()["minutely_15"]
    df = pd.DataFrame({
        "time": pd.to_datetime(m["time"]).tz_localize("Europe/London"),
        "temperature": m["temperature_2m"],          # °C
        "apparent_temperature": m["apparent_temperature"],
        "humidity": m["relative_humidity_2m"],        # %
        "precipitation": m["precipitation"],
        "wind_speed": m["wind_speed_10m"],
        "cloud_cover": m["cloud_cover"],
        "visibility": m["visibility"],
    })
    return df.sort_values("time").reset_index(drop=True)


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


# ---------------------------------------------------------------------------
# Thames Tidal (EA Flood Monitoring)
# ---------------------------------------------------------------------------

def get_thames(limit: int = 200) -> pd.DataFrame:
    """Fetch Thames tidal readings at Westminster gauge.

    Returns DataFrame with columns: time (Europe/London tz), level (mAOD).
    limit=200 covers ~50 hours; use limit=400 for ~4 days.
    """
    resp = requests.get(
        f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings",
        params={"_sorted": "", "_limit": limit},
        timeout=15,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    df = pd.DataFrame(items)[["dateTime", "value"]].rename(
        columns={"dateTime": "time", "value": "level"}
    )
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Europe/London")
    return df.sort_values("time").reset_index(drop=True)


def project_tide_at_noon(tidal_df: pd.DataFrame) -> tuple[float, float]:
    """Project Thames tidal level at next 12:00 London time.

    Uses sinusoidal least-squares fit on recent readings.
    Returns (projected_level_mAOD, confidence 0-1).
    """
    if tidal_df.empty:
        return 0.0, 0.0

    df = tidal_df.copy()
    df["t_hours"] = (df["time"] - df["time"].iloc[0]).dt.total_seconds() / 3600.0
    levels = df["level"].values
    t = df["t_hours"].values

    # Compute next settlement time
    now_london = current_time(df["time"].iloc[-1].tzinfo)
    next_noon = next_settlement_noon(now_london)

    last_obs_time = df["time"].iloc[-1]
    hours_to_noon = (next_noon - last_obs_time).total_seconds() / 3600.0

    # Simple sinusoidal fit: Thames has ~12.4h tidal cycle
    TIDAL_PERIOD = 12.42  # hours (M2 lunar semi-diurnal tide)
    omega = 2 * np.pi / TIDAL_PERIOD

    # Build design matrix: [1, sin(ωt), cos(ωt)]
    try:
        A = np.column_stack([np.ones_like(t), np.sin(omega * t), np.cos(omega * t)])
        coeffs, residuals, _, _ = np.linalg.lstsq(A, levels, rcond=None)
        mean_level, sin_coeff, cos_coeff = coeffs

        t_noon = t[-1] + hours_to_noon
        projected = mean_level + sin_coeff * np.sin(omega * t_noon) + cos_coeff * np.cos(omega * t_noon)

        # Confidence: based on fit quality and recency of data
        if len(residuals) > 0:
            rmse = np.sqrt(residuals[0] / len(t))
            conf = max(0.0, min(1.0, 1.0 - rmse / 0.5))
        else:
            # Perfect fit (overdetermined or exact)
            amplitude = np.sqrt(sin_coeff**2 + cos_coeff**2)
            rmse_est = amplitude * 0.05  # assume 5% error
            conf = max(0.0, min(1.0, 1.0 - rmse_est / 0.5))

        # Reduce confidence if prediction is far in future
        if hours_to_noon > 6:
            conf *= 0.6
        elif hours_to_noon > 3:
            conf *= 0.8

        return projected, conf

    except Exception:
        # Fallback: use last observed value
        return float(levels[-1]), 0.2


def compute_tide_swing(
    tidal_df: pd.DataFrame,
    session_start: datetime,
    session_end: datetime,
) -> tuple[float, float]:
    """Compute TIDE_SWING settlement estimate.

    Sums strangle payoffs on 15-min tidal diffs over the 24h session.
    For past intervals: uses actual data. For future: uses sinusoidal projection.

    Returns (estimated_settlement, confidence 0-1).
    """
    if tidal_df.empty:
        return 0.0, 0.0

    df = tidal_df.copy()
    df = df[(df["time"] >= session_start) & (df["time"] <= session_end)].reset_index(drop=True)

    def strangle_payoff(diff_cm: float) -> float:
        put = max(0.0, 20.0 - diff_cm)
        call = max(0.0, diff_cm - 25.0)
        return put + call

    total_payoff = 0.0
    observed_count = 0

    # Sum over observed intervals
    for i in range(1, len(df)):
        diff_m = abs(df["level"].iloc[i] - df["level"].iloc[i - 1])
        diff_cm = diff_m * 100
        total_payoff += strangle_payoff(diff_cm)
        observed_count += 1

    # Estimate remaining intervals using sinusoidal projection
    now_london = df["time"].iloc[-1] if not df.empty else session_start
    remaining_start = now_london
    remaining_end = session_end
    remaining_hours = (remaining_end - remaining_start).total_seconds() / 3600
    remaining_intervals = int(remaining_hours * 4)  # 4 per hour = 15-min intervals

    if remaining_intervals > 0 and observed_count > 0:
        # Use average of past payoffs as estimate for future
        avg_past_payoff = total_payoff / observed_count
        total_payoff += avg_past_payoff * remaining_intervals
        confidence = observed_count / (observed_count + remaining_intervals)
    else:
        confidence = 1.0 if remaining_intervals == 0 else 0.1

    return total_payoff, confidence


# ---------------------------------------------------------------------------
# Flights (AeroDataBox via RapidAPI)
# ---------------------------------------------------------------------------

def fetch_flights(
    api_key: str,
    airport: str = AIRPORT,
    offset_minutes: int = -360,
    duration_minutes: int = 720,
) -> dict:
    """Fetch flights by relative time window (offset from now).

    offset_minutes: Start of window relative to now (negative = past).
    duration_minutes: Window length in minutes (max 720 = 12h).
    """
    if not isinstance(api_key, str):
        raise TypeError(f"api_key must be str, got {type(api_key).__name__}")
    if not isinstance(airport, str):
        raise TypeError(f"airport must be str, got {type(airport).__name__}")
    params = f"?offsetMinutes={offset_minutes}&durationMinutes={duration_minutes}&direction=Both"
    url = f"https://{AERODATABOX_HOST}/flights/airports/iata/{airport}{params}"
    resp = requests.get(
        url,
        headers={"x-rapidapi-host": AERODATABOX_HOST, "x-rapidapi-key": api_key},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_flights_range(
    api_key: str,
    airport: str = AIRPORT,
    from_local: str | None = None,
    to_local: str | None = None,
) -> dict:
    """Fetch flights by explicit local time range (max 12h span).

    If omitted, defaults to [now-6h, now+6h] in Europe/London.
    """
    if not isinstance(api_key, str):
        raise TypeError(f"api_key must be str, got {type(api_key).__name__}")
    if not isinstance(airport, str):
        raise TypeError(f"airport must be str, got {type(airport).__name__}")
    if from_local is None or to_local is None:
        now_local = current_time(LONDON_TZ)
        default_from = now_local - timedelta(hours=6)
        default_to = now_local + timedelta(hours=6)
        from_local = from_local or default_from.strftime("%Y-%m-%dT%H:%M")
        to_local = to_local or default_to.strftime("%Y-%m-%dT%H:%M")
    if not isinstance(from_local, str):
        raise TypeError(f"from_local must be str, got {type(from_local).__name__}")
    if not isinstance(to_local, str):
        raise TypeError(f"to_local must be str, got {type(to_local).__name__}")

    url = f"https://{AERODATABOX_HOST}/flights/airports/iata/{airport}/{from_local}/{to_local}?direction=Both"
    resp = requests.get(
        url,
        headers={"x-rapidapi-host": AERODATABOX_HOST, "x-rapidapi-key": api_key},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def compute_flight_estimates(
    flights_data: dict,
    session_start: datetime,
    session_end: datetime,
) -> tuple[float, float, float, float]:
    """Compute LHR_COUNT and LHR_INDEX estimates from flight data.

    Returns: (lhr_count_estimate, lhr_count_confidence,
               lhr_index_estimate, lhr_index_confidence)
    """
    arrivals = flights_data.get("arrivals", [])
    departures = flights_data.get("departures", [])

    total_session_hours = max(1.0, (session_end - session_start).total_seconds() / 3600)

    # Parse flight timestamp from preferred fields, robust to missing keys.
    def parse_time(flight: dict) -> Optional[datetime]:
        for key in ["movement", "departure", "arrival"]:
            section = flight.get(key)
            if not isinstance(section, dict):
                continue
            for t_key in ["scheduledTimeLocal", "actualTimeLocal", "revisedTimeLocal"]:
                val = section.get(t_key)
                if not val:
                    continue
                try:
                    ts = pd.to_datetime(val).to_pydatetime()
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=session_start.tzinfo)
                    return ts
                except Exception:
                    continue
        return None

    def in_session(ts: datetime) -> bool:
        return session_start <= ts <= session_end

    arr_times = [t for f in arrivals if (t := parse_time(f)) is not None and in_session(t)]
    dep_times = [t for f in departures if (t := parse_time(f)) is not None and in_session(t)]
    observed_total = len(arr_times) + len(dep_times)

    # Coverage-based extrapolation (instead of elapsed-time), avoids early-session blowups.
    all_times = sorted(arr_times + dep_times)
    if all_times:
        coverage_start = max(session_start, all_times[0])
        coverage_end = min(session_end, all_times[-1])
        coverage_hours = max(0.5, (coverage_end - coverage_start).total_seconds() / 3600)
    else:
        coverage_hours = 0.5

    if observed_total > 0:
        flights_per_hour = observed_total / coverage_hours
        flights_per_hour = min(150.0, max(20.0, flights_per_hour))
        remaining_hours = max(0.0, total_session_hours - coverage_hours)
        lhr_count_estimate = observed_total + flights_per_hour * remaining_hours
    else:
        lhr_count_estimate = 400.0

    coverage_frac = max(0.0, min(1.0, coverage_hours / total_session_hours))
    lhr_count_confidence = max(0.1, coverage_frac)

    # LHR_INDEX from observed 30-min bins.
    bins: dict[str, dict[str, int]] = {}

    def add_to_bin(ts: datetime, kind: str):
        minute_bucket = (ts.minute // 30) * 30
        bin_key = ts.strftime("%Y-%m-%d %H:") + f"{minute_bucket:02d}"
        if bin_key not in bins:
            bins[bin_key] = {"arr": 0, "dep": 0}
        bins[bin_key][kind] += 1

    for ts in arr_times:
        add_to_bin(ts, "arr")
    for ts in dep_times:
        add_to_bin(ts, "dep")

    index_sum = 0.0
    for b in bins.values():
        arr, dep = b["arr"], b["dep"]
        total = arr + dep
        if total > 0:
            index_sum += (arr - dep) / total

    lhr_index_estimate = abs(index_sum) * 100
    lhr_index_confidence = max(0.1, lhr_count_confidence * 0.8)

    return float(lhr_count_estimate), lhr_count_confidence, lhr_index_estimate, lhr_index_confidence


# ---------------------------------------------------------------------------
# LON_FLY payoff computation
# ---------------------------------------------------------------------------

def lon_fly_payoff(etf_value: float) -> float:
    """Compute LON_FLY settlement given LON_ETF settlement value.

    Formula: 2×Put(6200) + Call(6200) − 2×Call(6600) + 3×Call(7000)
    """
    put_6200 = max(0.0, FLY_STRIKES["put1"] - etf_value)
    call_6200 = max(0.0, etf_value - FLY_STRIKES["call1"])
    call_6600 = max(0.0, etf_value - FLY_STRIKES["call2"])
    call_7000 = max(0.0, etf_value - FLY_STRIKES["call3"])
    return 2 * put_6200 + call_6200 - 2 * call_6600 + 3 * call_7000


# ---------------------------------------------------------------------------
# Master fair value computation
# ---------------------------------------------------------------------------

def compute_all_fair_values(
    weather_df: pd.DataFrame,
    tidal_df: pd.DataFrame,
    flights_data: Optional[dict],
    session_start: datetime,
    api_key: str = "",
) -> FairValueEstimate:
    """Compute fair value estimates for all 8 products.

    Args:
        weather_df: Output of get_weather()
        tidal_df: Output of get_thames()
        flights_data: Output of fetch_flights() or fetch_flights_range(), or None
        session_start: Session start datetime (timezone-aware, Europe/London)
        api_key: Not used here; pass flights_data=None if unavailable

    Returns FairValueEstimate with fv and confidence for each product.
    """
    session_end = session_start + timedelta(hours=SESSION_HOURS)
    est = FairValueEstimate()

    # -- TIDE_SPOT --
    projected_level_maod, tide_conf = project_tide_at_noon(tidal_df)
    tide_spot = abs(projected_level_maod * 1000)  # metres → millimetres
    est.fv["TIDE_SPOT"] = tide_spot
    est.confidence["TIDE_SPOT"] = tide_conf

    # -- TIDE_SWING --
    tide_swing, swing_conf = compute_tide_swing(tidal_df, session_start, session_end)
    est.fv["TIDE_SWING"] = tide_swing
    est.confidence["TIDE_SWING"] = swing_conf

    # -- WX_SPOT (temperature_F × humidity at noon) --
    wx_spot_fv = _compute_wx_spot(weather_df)
    wx_spot_conf = _weather_confidence(weather_df, horizon="spot", session_start=session_start)
    est.fv["WX_SPOT"] = wx_spot_fv
    est.confidence["WX_SPOT"] = wx_spot_conf

    # -- WX_SUM --
    wx_sum_fv = _compute_wx_sum(weather_df, session_start, session_end)
    wx_sum_conf = _weather_confidence(weather_df, horizon="sum", session_start=session_start)
    est.fv["WX_SUM"] = wx_sum_fv
    est.confidence["WX_SUM"] = wx_sum_conf

    # -- Flight products --
    if flights_data is not None:
        lhr_count, lhr_count_conf, lhr_index, lhr_index_conf = compute_flight_estimates(
            flights_data, session_start, session_end
        )
    else:
        # No flight data: use broad defaults
        lhr_count, lhr_count_conf = 400.0, 0.1
        lhr_index, lhr_index_conf = 5.0, 0.1

    est.fv["LHR_COUNT"] = lhr_count
    est.confidence["LHR_COUNT"] = lhr_count_conf
    est.fv["LHR_INDEX"] = lhr_index
    est.confidence["LHR_INDEX"] = lhr_index_conf

    # -- LON_ETF (= TIDE_SPOT + WX_SPOT + LHR_COUNT) --
    etf_fv = tide_spot + wx_spot_fv + lhr_count
    etf_conf = min(tide_conf, wx_spot_conf, lhr_count_conf)  # weakest link
    est.fv["LON_ETF"] = etf_fv
    est.confidence["LON_ETF"] = etf_conf

    # -- LON_FLY --
    fly_fv = lon_fly_payoff(etf_fv)
    est.fv["LON_FLY"] = fly_fv
    est.confidence["LON_FLY"] = etf_conf * 0.8  # payoff is nonlinear, less certain

    est.last_data_update = time.monotonic()
    return est


# ---------------------------------------------------------------------------
# Internal weather helpers
# ---------------------------------------------------------------------------

def _compute_wx_spot(weather_df: pd.DataFrame) -> float:
    """Find the 15-min row closest to 12:00 London and return temp_F × humidity."""
    if weather_df.empty:
        return 4000.0  # rough default

    now_tz = weather_df["time"].iloc[0].tzinfo
    today = current_time(now_tz)
    next_noon = next_settlement_noon(today)

    next_noon_ts = pd.Timestamp(next_noon)
    idx = (weather_df["time"] - next_noon_ts).abs().idxmin()
    row = weather_df.iloc[idx]
    temp_f = celsius_to_fahrenheit(row["temperature"])
    return temp_f * row["humidity"]


def _compute_wx_sum(
    weather_df: pd.DataFrame, session_start: datetime, session_end: datetime
) -> float:
    """Sum temp_F × humidity over all 15-min intervals in [session_start, session_end], divide by 100."""
    if weather_df.empty:
        return 3840.0  # rough default

    mask = (weather_df["time"] >= pd.Timestamp(session_start)) & (
        weather_df["time"] <= pd.Timestamp(session_end)
    )
    window = weather_df[mask]
    if window.empty:
        return 3840.0

    products = window.apply(
        lambda r: celsius_to_fahrenheit(r["temperature"]) * r["humidity"], axis=1
    )
    return float(products.sum() / 100.0)


def _weather_confidence(
    weather_df: pd.DataFrame,
    horizon: str = "spot",
    session_start: datetime | None = None,
) -> float:
    """Estimate confidence in weather forecast.

    Higher confidence when forecast horizon is short (near-term data is reliable).
    """
    if weather_df.empty:
        return 0.1

    now_tz = weather_df["time"].iloc[0].tzinfo
    now = current_time(now_tz)

    # Next settlement noon
    next_noon = next_settlement_noon(now)

    hours_to_noon = (next_noon - now).total_seconds() / 3600

    if horizon == "spot":
        # Spot accuracy is purely forecast quality vs distance to noon
        if hours_to_noon < 1:
            return 0.95
        elif hours_to_noon < 6:
            return 0.85
        elif hours_to_noon < 12:
            return 0.70
        else:
            return 0.50
    else:
        # Sum: confidence = fraction of 24h session already observed.
        if session_start is None:
            return 0.3
        elapsed = (now - session_start).total_seconds() / 3600
        session_hours = float(SESSION_HOURS)
        observed_frac = max(0.0, min(1.0, elapsed / session_hours))
        return max(0.3, observed_frac)
