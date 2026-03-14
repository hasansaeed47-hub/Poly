"""
Weather data feeds: METAR, NOAA, Open-Meteo, GFS ensemble, Weather Underground.
All free except WU (optional, requires WU_API_KEY).
"""

import os
import time
from typing import Optional, Dict, List

from weatherbot.config import (
    S, log,
    MIN_MEMBERS_CHANGED,
)
from weatherbot.models import Forecast


def fetch_metar(city: str, info: dict) -> Optional[dict]:
    """
    Airport observation from aviationweather.gov. FREE, no key.
    Returns: {"temp_f", "temp_c", "obs_time", "station"} or None.
    """
    icao = info.get("metar_id", "")
    if not icao:
        return None
    try:
        r = S.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "format": "json"},
            timeout=10,
        )
        if r.status_code != 200:
            log.debug(f"[METAR] {city}: HTTP {r.status_code}")
            return None

        data = r.json()
        if not data or not isinstance(data, list):
            return None

        obs = data[0]
        temp_c = obs.get("temp")
        if temp_c is None:
            return None

        temp_f = temp_c * 9.0 / 5.0 + 32.0
        obs_time = obs.get("obsTime", 0)
        log.debug(f"[METAR] {city}/{icao}: {temp_f:.0f}F ({temp_c:.1f}C) at {obs_time}")
        return {"temp_f": temp_f, "temp_c": temp_c, "obs_time": obs_time, "station": icao}
    except Exception as e:
        log.debug(f"[METAR] {city}: {e}")
        return None


def fetch_noaa(city: str, info: dict) -> Optional[Forecast]:
    """NOAA forecast. FREE, US only. Returns None for international cities."""
    office = info.get("noaa_office")
    grid = info.get("noaa_grid")
    if not office or not grid:
        return None
    try:
        url = f"https://api.weather.gov/gridpoints/{office}/{grid}/forecast"
        r = S.get(url, headers={"User-Agent": "WeatherBot/1.0"}, timeout=10)
        if r.status_code != 200:
            return None

        periods = r.json().get("properties", {}).get("periods", [])
        if not periods:
            return None

        today = None
        for p in periods:
            if p.get("isDaytime", False):
                today = p
                break
        if not today:
            today = periods[0]

        high_f = float(today.get("temperature", 0))
        unit = today.get("temperatureUnit", "F")
        if unit == "C":
            high_f = high_f * 9.0 / 5.0 + 32.0

        return Forecast(
            city=city, high_f=high_f,
            high_c=(high_f - 32.0) * 5.0 / 9.0,
            low_f=high_f - 15.0, low_c=(high_f - 15.0 - 32.0) * 5.0 / 9.0,
            confidence=0.85, source="noaa", fetched_at=time.time(),
        )
    except Exception as e:
        log.debug(f"[NOAA] {city}: {e}")
        return None


def fetch_openmeteo(city: str, info: dict) -> Optional[Forecast]:
    """Open-Meteo deterministic forecast. FREE, worldwide."""
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "hourly": "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone": "auto", "forecast_days": 1,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None

        data = r.json()
        daily = data.get("daily", {})
        high_f = daily.get("temperature_2m_max", [0])[0]
        low_f = daily.get("temperature_2m_min", [0])[0]
        hourly = data.get("hourly", {}).get("temperature_2m", [])

        return Forecast(
            city=city, high_f=high_f,
            high_c=(high_f - 32.0) * 5.0 / 9.0,
            low_f=low_f, low_c=(low_f - 32.0) * 5.0 / 9.0,
            confidence=0.80, source="openmeteo", fetched_at=time.time(),
            hourly_temps=hourly[:24],
        )
    except Exception as e:
        log.debug(f"[OPENMETEO] {city}: {e}")
        return None


def fetch_gfs_ensemble(city: str, info: dict) -> Optional[List[float]]:
    """
    GFS 31-member ensemble daily max temps from Open-Meteo. FREE.
    Returns list of 31 high temperatures in F.
    Keys: temperature_2m_max_member00 through _member30.
    """
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "auto", "forecast_days": 1,
                "models": "gfs_seamless",
            },
            timeout=15,
        )
        if r.status_code != 200:
            log.debug(f"[ENSEMBLE] {city}: HTTP {r.status_code}")
            return None

        daily = r.json().get("daily", {})
        members = []
        for i in range(31):
            key = f"temperature_2m_max_member{i:02d}"
            vals = daily.get(key, [])
            if vals and vals[0] is not None:
                members.append(float(vals[0]))

        if len(members) < 5:
            log.debug(f"[ENSEMBLE] {city}: only {len(members)} members")
            return None

        log.info(
            f"[ENSEMBLE] {city}: {len(members)} members, "
            f"range={min(members):.0f}-{max(members):.0f}F, "
            f"mean={sum(members) / len(members):.1f}F"
        )
        return members
    except Exception as e:
        log.debug(f"[ENSEMBLE] {city}: {e}")
        return None


def fetch_wunderground(city: str, info: dict) -> Optional[dict]:
    """
    Weather Underground current observation. Requires WU_API_KEY env var.
    Returns: {"temp_f", "temp_c", "source"} or None.
    """
    api_key = os.environ.get("WU_API_KEY", "")
    if not api_key:
        return None
    station = info.get("wu_station", "")
    if not station:
        return None
    try:
        r = S.get(
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station, "format": "json",
                "units": "e", "apiKey": api_key,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None

        obs = r.json().get("observations", [{}])[0]
        imperial = obs.get("imperial", {})
        temp_f = imperial.get("temp")
        if temp_f is None:
            return None

        return {
            "temp_f": float(temp_f),
            "temp_c": (float(temp_f) - 32.0) * 5.0 / 9.0,
            "source": "wunderground",
        }
    except Exception as e:
        log.debug(f"[WU] {city}: {e}")
        return None


def detect_ensemble_shift(prev: List[float], curr: List[float]) -> bool:
    """True if a new GFS run dropped (5+ members shifted by 1F+)."""
    if len(prev) != len(curr):
        return True
    changed = sum(1 for a, b in zip(prev, curr) if abs(a - b) >= 1.0)
    return changed >= MIN_MEMBERS_CHANGED


def get_forecast(
    city: str,
    info: dict,
    prev_forecast: Optional[Forecast] = None,
    observed_highs: Optional[Dict[str, float]] = None,
) -> Optional[Forecast]:
    """
    Build best available forecast + observations for a city.

    Priority: NOAA > Open-Meteo for point estimate.
    Always fetches: GFS ensemble (probability engine) + METAR/WU observations.
    """
    # Point forecast
    fc = fetch_noaa(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: NOAA high={fc.high_f:.0f}F")
    else:
        fc = fetch_openmeteo(city, info)
        if fc:
            log.info(f"[FORECAST] {city}: OpenMeteo high={fc.high_f:.0f}F")

    if not fc:
        log.warning(f"[FORECAST] {city}: ALL SOURCES FAILED")
        return None

    # GFS ensemble — override point estimate with ensemble mean
    ensemble = fetch_gfs_ensemble(city, info)
    if ensemble:
        fc.ensemble_highs = ensemble
        fc.high_f = sum(ensemble) / len(ensemble)
        fc.high_c = (fc.high_f - 32.0) * 5.0 / 9.0

    # Observations: WU primary, METAR fallback
    metar = fetch_metar(city, info)
    wu_obs = fetch_wunderground(city, info)

    current_obs_high = (observed_highs or {}).get(city)

    if wu_obs:
        temp_f = wu_obs["temp_f"]
        if current_obs_high is None or temp_f > current_obs_high:
            current_obs_high = temp_f
        fc.observed_high_f = current_obs_high
        fc.observed_source = "wunderground"
        log.info(f"[OBS] {city}: WU current={temp_f:.0f}F running_high={current_obs_high:.0f}F")
    elif metar:
        temp_f = metar["temp_f"]
        if current_obs_high is None or temp_f > current_obs_high:
            current_obs_high = temp_f
        fc.observed_high_f = current_obs_high
        fc.observed_source = "metar"
        log.info(f"[OBS] {city}: METAR current={temp_f:.0f}F running_high={current_obs_high:.0f}F")

    if observed_highs is not None and current_obs_high is not None:
        observed_highs[city] = current_obs_high

    # Preserve previous ensemble for shift detection
    if prev_forecast and prev_forecast.has_ensemble:
        fc.prev_ensemble_highs = prev_forecast.ensemble_highs
        if fc.has_ensemble:
            is_new_run = detect_ensemble_shift(
                prev_forecast.ensemble_highs, fc.ensemble_highs,
            )
            prev_mean = sum(prev_forecast.ensemble_highs) / len(prev_forecast.ensemble_highs)
            curr_mean = sum(fc.ensemble_highs) / len(fc.ensemble_highs)
            shift = curr_mean - prev_mean
            tag = "NEW RUN" if is_new_run else "same run"
            if abs(shift) > 0.5:
                log.info(
                    f"[SHIFT] {city}: {shift:+.1f}F ({tag}) "
                    f"({prev_mean:.0f}->{curr_mean:.0f})"
                )

    return fc
