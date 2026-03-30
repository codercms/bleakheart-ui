from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class EcgSeries:
    timestamps_s: np.ndarray
    timestamps_utc: pd.DatetimeIndex
    ecg_uv: np.ndarray
    source_path: Path
    timestamp_column: str
    value_column: str


@dataclass
class RrSeries:
    timestamps_s: np.ndarray
    timestamps_utc: pd.DatetimeIndex
    rr_ms: np.ndarray
    heart_rate_bpm: np.ndarray
    source_path: Path
    timestamp_column: str
    rr_column: str
    hr_column: str | None


@dataclass
class HrSeries:
    timestamps_s: np.ndarray
    timestamps_utc: pd.DatetimeIndex
    heart_rate_bpm: np.ndarray
    source_path: Path
    timestamp_column: str
    hr_column: str


def _normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())


def _pick_column(columns: Iterable[str], candidates: list[str], required: bool = True) -> str | None:
    normalized = {_normalize_name(c): c for c in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    if required:
        raise ValueError(f"Could not find required column among: {list(columns)}")
    return None


def _parse_timestamps_to_utc_seconds(values: pd.Series) -> tuple[np.ndarray, pd.DatetimeIndex]:
    if values.empty:
        return np.asarray([], dtype=np.float64), pd.DatetimeIndex([], tz="UTC")

    numeric = pd.to_numeric(values, errors="coerce")
    use_numeric = float(numeric.notna().mean()) > 0.8

    if use_numeric:
        arr = numeric.to_numpy(dtype=np.float64)
        finite = np.isfinite(arr)
        if not finite.any():
            return np.asarray([], dtype=np.float64), pd.DatetimeIndex([], tz="UTC")
        valid = arr[finite]
        median = float(np.nanmedian(np.abs(valid)))
        if median >= 1e15:
            scale = 1e9
        elif median >= 1e12:
            scale = 1e3
        elif median >= 1e10:
            scale = 1e3
        else:
            scale = 1.0
        seconds = arr / scale
        dt = pd.to_datetime(seconds, unit="s", utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(values, utc=True, errors="coerce")
        seconds = dt.view("int64") / 1e9

    valid_mask = np.isfinite(seconds) & ~pd.isna(dt)
    seconds = seconds[valid_mask]
    dt = dt[valid_mask]
    if seconds.size == 0:
        return np.asarray([], dtype=np.float64), pd.DatetimeIndex([], tz="UTC")

    order = np.argsort(seconds)
    seconds_sorted = np.asarray(seconds, dtype=np.float64)[order]
    dt_sorted = pd.DatetimeIndex(dt).take(order)

    diff = np.diff(seconds_sorted)
    keep = np.concatenate(([True], diff > 0))
    return seconds_sorted[keep], dt_sorted[keep]


def _to_utc_seconds_raw(values: pd.Series) -> tuple[np.ndarray, pd.DatetimeIndex]:
    if values.empty:
        return np.asarray([], dtype=np.float64), pd.DatetimeIndex([], tz="UTC")

    numeric = pd.to_numeric(values, errors="coerce")
    use_numeric = float(numeric.notna().mean()) > 0.8

    if use_numeric:
        arr = numeric.to_numpy(dtype=np.float64)
        finite = np.isfinite(arr)
        seconds = np.full(arr.shape, np.nan, dtype=np.float64)
        if finite.any():
            valid = arr[finite]
            median = float(np.nanmedian(np.abs(valid)))
            if median >= 1e15:
                scale = 1e9
            elif median >= 1e12:
                scale = 1e3
            elif median >= 1e10:
                scale = 1e3
            else:
                scale = 1.0
            seconds[finite] = arr[finite] / scale
        dt = pd.to_datetime(seconds, unit="s", utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(values, utc=True, errors="coerce")
        seconds = dt.view("int64") / 1e9
        seconds = np.asarray(seconds, dtype=np.float64)
        seconds[pd.isna(dt)] = np.nan

    return seconds, pd.DatetimeIndex(dt)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"CSV has no rows: {path}")
    return df


def load_ecg_csv(path: Path) -> EcgSeries:
    df = _read_csv(path)
    ts_col = _pick_column(df.columns, ["timestamps", "timestamp", "times", "timesec", "time", "timestampunix"])

    ecg_col = _pick_column(
        df.columns,
        ["ecguv", "ecg", "ecgvalue", "rawecg", "ecgmicrovolt", "ecgsignal"],
    )

    seconds_raw, dt_raw = _to_utc_seconds_raw(df[ts_col])
    values_raw = pd.to_numeric(df[ecg_col], errors="coerce").to_numpy(dtype=np.float64)
    finite = np.isfinite(seconds_raw) & np.isfinite(values_raw) & (~pd.isna(dt_raw))
    if not finite.any():
        raise ValueError(f"No usable ECG rows in {path}")

    seconds = seconds_raw[finite]
    values = values_raw[finite]
    dt_utc = dt_raw[finite]
    order = np.argsort(seconds)
    seconds = seconds[order]
    values = values[order]
    dt_utc = dt_utc.take(order)
    keep = np.concatenate(([True], np.diff(seconds) > 0))

    return EcgSeries(
        timestamps_s=seconds[keep],
        timestamps_utc=dt_utc[keep],
        ecg_uv=values[keep],
        source_path=path,
        timestamp_column=ts_col,
        value_column=ecg_col,
    )


def load_rr_csv(path: Path) -> RrSeries:
    df = _read_csv(path)
    ts_col = _pick_column(df.columns, ["timestamps", "timestamp", "times", "timesec", "time", "timestampunix", "timestamps"])
    rr_col = _pick_column(df.columns, ["rrms", "rr", "rrinterval", "rrintervalms", "rrintervals"])
    hr_col = _pick_column(df.columns, ["heartratebpm", "hrbpm", "heartrate", "heart_rate_bpm"], required=False)

    seconds_raw, dt_raw = _to_utc_seconds_raw(df[ts_col])
    rr_raw = pd.to_numeric(df[rr_col], errors="coerce").to_numpy(dtype=np.float64)
    if hr_col is not None:
        hr_raw = pd.to_numeric(df[hr_col], errors="coerce").to_numpy(dtype=np.float64)
    else:
        hr_raw = np.asarray([], dtype=np.float64)

    finite = np.isfinite(seconds_raw) & np.isfinite(rr_raw) & (~pd.isna(dt_raw))
    if not finite.any():
        raise ValueError(f"No usable RR rows in {path}")

    seconds = seconds_raw[finite]
    dt_utc = dt_raw[finite]
    rr = rr_raw[finite]

    if hr_raw.size:
        hr = hr_raw[finite]
    else:
        hr = 60000.0 / rr

    hr = np.where(np.isfinite(hr), hr, 60000.0 / rr)
    order = np.argsort(seconds)
    seconds = seconds[order]
    rr = rr[order]
    hr = hr[order]
    dt_utc = dt_utc.take(order)
    keep = np.concatenate(([True], np.diff(seconds) > 0))

    return RrSeries(
        timestamps_s=seconds[keep],
        timestamps_utc=dt_utc[keep],
        rr_ms=rr[keep],
        heart_rate_bpm=hr[keep],
        source_path=path,
        timestamp_column=ts_col,
        rr_column=rr_col,
        hr_column=hr_col,
    )


def load_hr_csv(path: Path) -> HrSeries:
    df = _read_csv(path)
    ts_col = _pick_column(df.columns, ["timestamps", "timestamp", "times", "time", "timesec"])
    hr_col = _pick_column(df.columns, ["heartratebpm", "hrbpm", "heartrate", "heart_rate_bpm"])

    seconds_raw, dt_raw = _to_utc_seconds_raw(df[ts_col])
    hr_raw = pd.to_numeric(df[hr_col], errors="coerce").to_numpy(dtype=np.float64)
    finite = np.isfinite(seconds_raw) & np.isfinite(hr_raw) & (~pd.isna(dt_raw))
    if not finite.any():
        raise ValueError(f"No usable HR rows in {path}")

    seconds = seconds_raw[finite]
    dt_utc = dt_raw[finite]
    hr = hr_raw[finite]
    order = np.argsort(seconds)
    seconds = seconds[order]
    hr = hr[order]
    dt_utc = dt_utc.take(order)
    keep = np.concatenate(([True], np.diff(seconds) > 0))

    return HrSeries(
        timestamps_s=seconds[keep],
        timestamps_utc=dt_utc[keep],
        heart_rate_bpm=hr[keep],
        source_path=path,
        timestamp_column=ts_col,
        hr_column=hr_col,
    )


def subset_by_time_window(
    timestamps_s: np.ndarray,
    values: np.ndarray,
    start_s: float,
    end_s: float,
) -> tuple[np.ndarray, np.ndarray]:
    mask = (timestamps_s >= start_s) & (timestamps_s <= end_s)
    return timestamps_s[mask], values[mask]


def overlap_window(
    ecg_timestamps_s: np.ndarray,
    rr_timestamps_s: np.ndarray,
) -> tuple[float, float]:
    if ecg_timestamps_s.size == 0 or rr_timestamps_s.size == 0:
        raise ValueError("Cannot compute overlap on empty timestamps")
    start = max(float(ecg_timestamps_s[0]), float(rr_timestamps_s[0]))
    end = min(float(ecg_timestamps_s[-1]), float(rr_timestamps_s[-1]))
    if end <= start:
        raise ValueError("No overlap between ECG and RR streams")
    return start, end


def unix_seconds_to_utc_iso(ts_s: float) -> str:
    return pd.Timestamp(ts_s, unit="s", tz="UTC").isoformat()
