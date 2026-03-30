from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.signal import welch


@dataclass
class RrAnalysisResult:
    metrics: dict[str, Any]
    rr_timestamps_s: np.ndarray
    rr_ms: np.ndarray
    heart_rate_bpm: np.ndarray
    psd_freqs_hz: np.ndarray
    psd_power: np.ndarray
    hrv_5min_rows: list[dict[str, Any]]


def _mad(x: np.ndarray) -> float:
    med = float(np.median(x))
    return max(float(np.median(np.abs(x - med))), 1e-9)


def clean_rr_series(rr_timestamps_s: np.ndarray, rr_ms: np.ndarray) -> dict[str, Any]:
    if rr_ms.size == 0:
        return {
            "mask_clean": np.asarray([], dtype=bool),
            "rr_timestamps_clean_s": np.asarray([], dtype=np.float64),
            "rr_clean_ms": np.asarray([], dtype=np.float64),
            "out_of_range_count": 0,
            "local_outlier_count": 0,
            "artifact_burden": 1.0,
        }

    physiologic = (rr_ms >= 280.0) & (rr_ms <= 2200.0)
    rr_phys = rr_ms[physiologic]
    if rr_phys.size < 5:
        clean = physiologic
        local_outlier_count = 0
    else:
        med = float(np.median(rr_phys))
        mad = _mad(rr_phys)
        z = np.abs(rr_ms - med) / (1.4826 * mad)
        ratio_ok = (rr_ms >= 0.45 * med) & (rr_ms <= 1.9 * med)
        clean = physiologic & ratio_ok & (z <= 6.0)
        local_outlier_count = int(np.sum(physiologic & (~(ratio_ok & (z <= 6.0)))))

    clean_count = int(np.sum(clean))
    total = int(rr_ms.size)
    artifact_burden = float(1.0 - (clean_count / max(total, 1)))

    return {
        "mask_clean": clean,
        "rr_timestamps_clean_s": rr_timestamps_s[clean],
        "rr_clean_ms": rr_ms[clean],
        "out_of_range_count": int(np.sum(~physiologic)),
        "local_outlier_count": local_outlier_count,
        "artifact_burden": artifact_burden,
    }


def time_domain_hrv(rr_ms: np.ndarray) -> dict[str, float | None]:
    if rr_ms.size < 2:
        return {
            "mean_nn_ms": None,
            "median_nn_ms": None,
            "sdnn_ms": None,
            "rmssd_ms": None,
            "pnn50_pct": None,
        }
    diff = np.diff(rr_ms)
    return {
        "mean_nn_ms": float(np.mean(rr_ms)),
        "median_nn_ms": float(np.median(rr_ms)),
        "sdnn_ms": float(np.std(rr_ms, ddof=1)) if rr_ms.size > 1 else None,
        "rmssd_ms": float(np.sqrt(np.mean(diff**2))) if diff.size else None,
        "pnn50_pct": float(np.mean(np.abs(diff) > 50.0) * 100.0) if diff.size else None,
    }


def frequency_domain_hrv(rr_timestamps_s: np.ndarray, rr_ms: np.ndarray) -> dict[str, Any]:
    if rr_ms.size < 20 or rr_timestamps_s.size < 20:
        return {
            "lf_power_ms2": None,
            "hf_power_ms2": None,
            "lf_hf_ratio": None,
            "resampled_fs_hz": None,
            "_psd_freqs_hz": np.asarray([], dtype=np.float64),
            "_psd_power": np.asarray([], dtype=np.float64),
        }

    t = rr_timestamps_s - rr_timestamps_s[0]
    monotonic = np.concatenate(([True], np.diff(t) > 0))
    t = t[monotonic]
    rr_ms = rr_ms[monotonic]

    if rr_ms.size < 20:
        return {
            "lf_power_ms2": None,
            "hf_power_ms2": None,
            "lf_hf_ratio": None,
            "resampled_fs_hz": None,
            "_psd_freqs_hz": np.asarray([], dtype=np.float64),
            "_psd_power": np.asarray([], dtype=np.float64),
        }

    fs_resampled = 4.0
    t_uniform = np.arange(t[0], t[-1], 1.0 / fs_resampled)
    if t_uniform.size < 64:
        return {
            "lf_power_ms2": None,
            "hf_power_ms2": None,
            "lf_hf_ratio": None,
            "resampled_fs_hz": fs_resampled,
            "_psd_freqs_hz": np.asarray([], dtype=np.float64),
            "_psd_power": np.asarray([], dtype=np.float64),
        }

    rr_uniform = np.interp(t_uniform, t, rr_ms)
    rr_centered = rr_uniform - np.mean(rr_uniform)
    freqs, power = welch(rr_centered, fs=fs_resampled, nperseg=min(1024, rr_centered.size))

    lf_mask = (freqs >= 0.04) & (freqs < 0.15)
    hf_mask = (freqs >= 0.15) & (freqs < 0.40)

    lf = float(np.trapezoid(power[lf_mask], freqs[lf_mask])) if np.any(lf_mask) else 0.0
    hf = float(np.trapezoid(power[hf_mask], freqs[hf_mask])) if np.any(hf_mask) else 0.0

    return {
        "lf_power_ms2": lf,
        "hf_power_ms2": hf,
        "lf_hf_ratio": float(lf / hf) if hf > 1e-9 else None,
        "resampled_fs_hz": fs_resampled,
        "_psd_freqs_hz": freqs,
        "_psd_power": power,
    }


def poincare_metrics(rr_ms: np.ndarray) -> dict[str, float | None]:
    if rr_ms.size < 3:
        return {"sd1_ms": None, "sd2_ms": None, "sd1_sd2_ratio": None}
    x = rr_ms[:-1]
    y = rr_ms[1:]
    diff = y - x
    summ = y + x
    sd1 = float(np.sqrt(np.var(diff, ddof=1) / 2.0))
    sd2 = float(np.sqrt(np.var(summ, ddof=1) / 2.0))
    return {"sd1_ms": sd1, "sd2_ms": sd2, "sd1_sd2_ratio": (sd1 / sd2 if sd2 > 1e-9 else None)}


def rolling_windows(
    timestamps_s: np.ndarray,
    values: np.ndarray,
    window_s: int,
    start_s: float,
    end_s: float,
) -> list[tuple[float, float, np.ndarray]]:
    out: list[tuple[float, float, np.ndarray]] = []
    cursor = start_s
    while cursor < end_s:
        stop = min(end_s, cursor + window_s)
        mask = (timestamps_s >= cursor) & (timestamps_s < stop)
        out.append((cursor, stop, values[mask]))
        cursor = stop
    return out


def hrv_windows_5min(
    rr_timestamps_s: np.ndarray,
    rr_ms: np.ndarray,
    window_s: int = 300,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if rr_ms.size == 0:
        summary = {
            "window_count": 0,
            "stationary_window_count": 0,
            "sdnn_median_5min_ms": None,
            "rmssd_median_5min_ms": None,
        }
        return [], summary

    rows: list[dict[str, Any]] = []
    start_s = float(rr_timestamps_s[0])
    end_s = float(rr_timestamps_s[-1])

    cursor = start_s
    while cursor < end_s:
        stop = min(end_s, cursor + window_s)
        mask = (rr_timestamps_s >= cursor) & (rr_timestamps_s < stop)
        rr_win = rr_ms[mask]

        td = time_domain_hrv(rr_win)
        fd = frequency_domain_hrv(rr_timestamps_s[mask], rr_win)
        hr = 60000.0 / rr_win if rr_win.size else np.asarray([], dtype=np.float64)

        stationary = False
        exclude_reason = ""
        if rr_win.size < 180:
            exclude_reason = "low_beat_count"
        elif hr.size and float(np.std(hr) / max(np.mean(hr), 1e-6)) > 0.15:
            exclude_reason = "high_hr_variability"
        else:
            stationary = True

        rows.append(
            {
                "window_start_s": float(cursor),
                "window_end_s": float(stop),
                "beat_count": int(rr_win.size),
                "hr_mean_bpm": float(np.mean(hr)) if hr.size else None,
                "mean_nn_ms": td["mean_nn_ms"],
                "median_nn_ms": td["median_nn_ms"],
                "sdnn_ms": td["sdnn_ms"],
                "rmssd_ms": td["rmssd_ms"],
                "pnn50_pct": td["pnn50_pct"],
                "lf_power_ms2": fd["lf_power_ms2"],
                "hf_power_ms2": fd["hf_power_ms2"],
                "lf_hf_ratio": fd["lf_hf_ratio"],
                "stationary_flag": stationary,
                "exclude_reason": exclude_reason,
            }
        )
        cursor = stop

    stationary_rows = [x for x in rows if bool(x["stationary_flag"])]
    sdnn_vals = [float(x["sdnn_ms"]) for x in stationary_rows if x["sdnn_ms"] is not None]
    rmssd_vals = [float(x["rmssd_ms"]) for x in stationary_rows if x["rmssd_ms"] is not None]

    summary = {
        "window_count": int(len(rows)),
        "stationary_window_count": int(len(stationary_rows)),
        "sdnn_median_5min_ms": float(np.median(sdnn_vals)) if sdnn_vals else None,
        "rmssd_median_5min_ms": float(np.median(rmssd_vals)) if rmssd_vals else None,
    }
    return rows, summary


def analyze_device_rr(
    rr_timestamps_s: np.ndarray,
    rr_ms: np.ndarray,
    heart_rate_bpm: np.ndarray,
) -> RrAnalysisResult:
    if rr_ms.size == 0:
        empty = np.asarray([], dtype=np.float64)
        return RrAnalysisResult(
            metrics={"status": "insufficient_data", "rr_count": 0},
            rr_timestamps_s=empty,
            rr_ms=empty,
            heart_rate_bpm=empty,
            psd_freqs_hz=empty,
            psd_power=empty,
            hrv_5min_rows=[],
        )

    clean = clean_rr_series(rr_timestamps_s, rr_ms)
    mask_clean = clean["mask_clean"]
    rr_t_clean = clean["rr_timestamps_clean_s"]
    rr_clean = clean["rr_clean_ms"]

    if heart_rate_bpm.size == rr_ms.size:
        hr_clean = heart_rate_bpm[mask_clean]
    else:
        hr_clean = 60000.0 / rr_clean

    td = time_domain_hrv(rr_clean)
    fd = frequency_domain_hrv(rr_t_clean, rr_clean)
    nonlinear = poincare_metrics(rr_clean)
    rows_5min, summary_5min = hrv_windows_5min(rr_t_clean, rr_clean, window_s=300)

    metrics: dict[str, Any] = {
        "status": "ok",
        "rr_count": int(rr_ms.size),
        "rr_count_clean": int(rr_clean.size),
        "rr_out_of_range_count": int(clean["out_of_range_count"]),
        "rr_local_outlier_count": int(clean["local_outlier_count"]),
        "artifact_burden": float(clean["artifact_burden"]),
        "duration_s": float(rr_timestamps_s[-1] - rr_timestamps_s[0]) if rr_timestamps_s.size else 0.0,
        "hr_bpm_mean": float(np.mean(hr_clean)) if hr_clean.size else None,
        "hr_bpm_min": float(np.min(hr_clean)) if hr_clean.size else None,
        "hr_bpm_max": float(np.max(hr_clean)) if hr_clean.size else None,
    }
    metrics.update(td)
    metrics.update({k: v for k, v in fd.items() if not k.startswith("_")})
    metrics.update(nonlinear)
    metrics.update(summary_5min)

    return RrAnalysisResult(
        metrics=metrics,
        rr_timestamps_s=rr_t_clean,
        rr_ms=rr_clean,
        heart_rate_bpm=hr_clean,
        psd_freqs_hz=np.asarray(fd.get("_psd_freqs_hz", np.asarray([], dtype=np.float64))),
        psd_power=np.asarray(fd.get("_psd_power", np.asarray([], dtype=np.float64))),
        hrv_5min_rows=rows_5min,
    )
