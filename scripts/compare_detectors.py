#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None

from scipy.signal import butter, filtfilt, find_peaks

try:
    import neurokit2 as nk
except Exception:  # pragma: no cover
    nk = None

try:
    from wfdb import processing as wfdb_processing
except Exception:  # pragma: no cover
    wfdb_processing = None


# ---------- IO helpers ----------

def _pick_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    if required:
        raise ValueError(f"Could not find any of columns: {candidates}. Available: {list(df.columns)}")
    return None


def read_ecg_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    ts_col = _pick_column(df, ["timestamp_s", "timestamp", "time_s", "time", "ts"])
    ecg_col = _pick_column(df, ["ecg_uV", "ecg_uv", "ecg", "value_uV", "value_uv", "value"])
    out = pd.DataFrame({
        "timestamp_s": pd.to_numeric(df[ts_col], errors="coerce"),
        "ecg_uV": pd.to_numeric(df[ecg_col], errors="coerce"),
    }).dropna()
    out = out.sort_values("timestamp_s").reset_index(drop=True)
    if out.empty:
        raise ValueError("ECG CSV has no valid rows after parsing")
    return out


def read_rr_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    ts_col = _pick_column(df, ["timestamp_s", "timestamp", "time_s", "time", "ts"])
    rr_col = _pick_column(df, ["rr_ms", "rr", "rrinterval_ms", "rr_interval_ms", "ibi_ms"])
    hr_col = _pick_column(df, ["heart_rate_bpm", "hr_bpm", "heart_rate", "hr"], required=False)
    out = pd.DataFrame({
        "timestamp_s": pd.to_numeric(df[ts_col], errors="coerce"),
        "rr_ms": pd.to_numeric(df[rr_col], errors="coerce"),
    })
    if hr_col:
        out["heart_rate_bpm"] = pd.to_numeric(df[hr_col], errors="coerce")
    out = out.dropna(subset=["timestamp_s", "rr_ms"]).sort_values("timestamp_s").reset_index(drop=True)
    if out.empty:
        raise ValueError("RR CSV has no valid rows after parsing")
    return out


# ---------- Signal processing ----------

def estimate_fs(timestamp_s: np.ndarray) -> float:
    if len(timestamp_s) < 3:
        raise ValueError("Need at least 3 ECG samples to estimate fs")
    dt = np.diff(timestamp_s)
    dt = dt[np.isfinite(dt) & (dt > 0)]
    if len(dt) == 0:
        raise ValueError("Could not estimate fs from timestamps")
    return 1.0 / float(np.median(dt))


def bandpass_qrs(signal: np.ndarray, fs: float, low_hz: float = 5.0, high_hz: float = 20.0, order: int = 2) -> np.ndarray:
    nyq = 0.5 * fs
    low = max(0.001, low_hz / nyq)
    high = min(0.999, high_hz / nyq)
    if not (0 < low < high < 1):
        return signal.astype(float, copy=True)
    b, a = butter(order, [low, high], btype="bandpass")
    return filtfilt(b, a, signal)


def highpass_display(signal: np.ndarray, fs: float, cutoff_hz: float = 0.5, order: int = 2) -> np.ndarray:
    nyq = 0.5 * fs
    wn = max(0.001, min(0.999, cutoff_hz / nyq))
    b, a = butter(order, wn, btype="highpass")
    return filtfilt(b, a, signal)


def refine_peaks_local_max(raw_ecg: np.ndarray, peak_idx: np.ndarray, fs: float, window_ms: float = 80.0) -> np.ndarray:
    if len(peak_idx) == 0:
        return peak_idx.astype(int)
    half = max(1, int(round(window_ms * fs / 1000.0)))
    refined: list[int] = []
    used = set()
    for idx in peak_idx:
        lo = max(0, int(idx) - half)
        hi = min(len(raw_ecg), int(idx) + half + 1)
        local = int(lo + np.argmax(raw_ecg[lo:hi]))
        if local not in used:
            refined.append(local)
            used.add(local)
    refined = np.array(sorted(refined), dtype=int)
    if len(refined) <= 1:
        return refined
    min_distance = max(1, int(round(0.25 * fs)))
    deduped = [int(refined[0])]
    for idx in refined[1:]:
        if idx - deduped[-1] >= min_distance:
            deduped.append(int(idx))
        elif raw_ecg[idx] > raw_ecg[deduped[-1]]:
            deduped[-1] = int(idx)
    return np.asarray(deduped, dtype=int)


# ---------- Detectors ----------

def detect_peaks_neurokit(ecg_uV: np.ndarray, fs: float) -> np.ndarray:
    if nk is None:
        raise RuntimeError("neurokit2 is not installed")
    cleaned = nk.ecg_clean(ecg_uV, sampling_rate=fs, method="neurokit")
    _, info = nk.ecg_peaks(cleaned, sampling_rate=fs, method="neurokit")
    peaks = np.asarray(info.get("ECG_R_Peaks", []), dtype=int)
    return peaks


def detect_peaks_baseline(ecg_uV: np.ndarray, fs: float) -> np.ndarray:
    detector_sig = bandpass_qrs(ecg_uV.astype(float), fs=fs)
    abs_sig = np.abs(detector_sig)
    prominence = max(20.0, float(np.percentile(abs_sig, 90) * 0.35))
    distance = max(1, int(round(0.30 * fs)))
    peaks, _ = find_peaks(detector_sig, distance=distance, prominence=prominence)
    peaks = refine_peaks_local_max(ecg_uV, peaks, fs=fs)
    return peaks


def detect_peaks_wfdb_xqrs(ecg_uV: np.ndarray, fs: float) -> np.ndarray:
    if wfdb_processing is None:
        raise RuntimeError("wfdb is not installed")
    sig_mV = np.asarray(ecg_uV, dtype=float) / 1000.0
    peaks = wfdb_processing.xqrs_detect(sig=sig_mV, fs=fs, verbose=False)
    return np.asarray(peaks, dtype=int)


def detect_peaks_wfdb_gqrs(ecg_uV: np.ndarray, fs: float) -> np.ndarray:
    if wfdb_processing is None:
        raise RuntimeError("wfdb is not installed")
    sig_mV = np.asarray(ecg_uV, dtype=float) / 1000.0
    peaks = wfdb_processing.gqrs_detect(sig=sig_mV, fs=fs)
    return np.asarray(peaks, dtype=int)


def detect_peaks_scripts2_like(ecg_uV: np.ndarray, fs: float) -> tuple[np.ndarray, str]:
    """
    Lightweight approximation of scripts2 behavior:
    prefer neurokit2, then refine on raw ECG, else scipy fallback.
    """
    try:
        peaks = detect_peaks_neurokit(ecg_uV, fs)
        peaks = refine_peaks_local_max(ecg_uV, peaks, fs=fs)
        return peaks, "neurokit2_refined"
    except Exception:
        peaks = detect_peaks_baseline(ecg_uV, fs)
        return peaks, "scipy_fallback"


# ---------- Metrics ----------

def peaks_to_rr_ms(timestamp_s: np.ndarray, peaks: np.ndarray) -> pd.DataFrame:
    if len(peaks) < 2:
        return pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"])
    r_times = timestamp_s[peaks]
    rr_ms = np.diff(r_times) * 1000.0
    rr_times = r_times[1:]
    hr_bpm = 60000.0 / rr_ms
    return pd.DataFrame({"timestamp_s": rr_times, "rr_ms": rr_ms, "heart_rate_bpm": hr_bpm})


def basic_hrv(rr_ms: np.ndarray) -> dict[str, float | None]:
    rr_ms = np.asarray(rr_ms, dtype=float)
    rr_ms = rr_ms[np.isfinite(rr_ms) & (rr_ms > 0)]
    if len(rr_ms) == 0:
        return {k: None for k in ["count", "mean_nn_ms", "median_nn_ms", "sdnn_ms", "rmssd_ms", "pnn50_pct", "hr_mean_bpm", "hr_min_bpm", "hr_max_bpm"]}
    hr = 60000.0 / rr_ms
    diff_rr = np.diff(rr_ms)
    rmssd = float(np.sqrt(np.mean(diff_rr**2))) if len(diff_rr) else None
    pnn50 = float(np.mean(np.abs(diff_rr) > 50.0) * 100.0) if len(diff_rr) else None
    return {
        "count": int(len(rr_ms)),
        "mean_nn_ms": float(np.mean(rr_ms)),
        "median_nn_ms": float(np.median(rr_ms)),
        "sdnn_ms": float(np.std(rr_ms, ddof=1)) if len(rr_ms) > 1 else None,
        "rmssd_ms": rmssd,
        "pnn50_pct": pnn50,
        "hr_mean_bpm": float(np.mean(hr)),
        "hr_min_bpm": float(np.min(hr)),
        "hr_max_bpm": float(np.max(hr)),
    }


def match_peak_times(a_s: np.ndarray, b_s: np.ndarray, tolerance_ms: float = 75.0) -> dict[str, float | int]:
    tol = tolerance_ms / 1000.0
    a = np.asarray(a_s, dtype=float)
    b = np.asarray(b_s, dtype=float)
    i = j = 0
    matched = 0
    errors_ms: list[float] = []
    while i < len(a) and j < len(b):
        delta = a[i] - b[j]
        if abs(delta) <= tol:
            matched += 1
            errors_ms.append(abs(delta) * 1000.0)
            i += 1
            j += 1
        elif delta < 0:
            i += 1
        else:
            j += 1
    precision = matched / len(a) if len(a) else 0.0
    recall = matched / len(b) if len(b) else 0.0
    return {
        "matched": matched,
        "a_count": int(len(a)),
        "b_count": int(len(b)),
        "precision": precision,
        "recall": recall,
        "mean_timing_error_ms": float(np.mean(errors_ms)) if errors_ms else None,
        "p95_timing_error_ms": float(np.percentile(errors_ms, 95)) if errors_ms else None,
    }


def compare_rr_streams(a_rr: pd.DataFrame, b_rr: pd.DataFrame, tolerance_ms: float = 150.0) -> dict[str, float | int | None]:
    if a_rr.empty or b_rr.empty:
        return {"matched": 0, "mae_ms": None, "median_ae_ms": None, "p95_ae_ms": None}
    a = a_rr[["timestamp_s", "rr_ms"]].sort_values("timestamp_s").to_numpy()
    b = b_rr[["timestamp_s", "rr_ms"]].sort_values("timestamp_s").to_numpy()
    tol_s = tolerance_ms / 1000.0
    i = j = 0
    errors: list[float] = []
    while i < len(a) and j < len(b):
        dt = a[i, 0] - b[j, 0]
        if abs(dt) <= tol_s:
            errors.append(abs(float(a[i, 1] - b[j, 1])))
            i += 1
            j += 1
        elif dt < 0:
            i += 1
        else:
            j += 1
    return {
        "matched": len(errors),
        "mae_ms": float(np.mean(errors)) if errors else None,
        "median_ae_ms": float(np.median(errors)) if errors else None,
        "p95_ae_ms": float(np.percentile(errors, 95)) if errors else None,
    }


# ---------- Main workflow ----------
@dataclass
class DetectorResult:
    name: str
    peaks: np.ndarray
    rr_df: pd.DataFrame
    notes: str | None = None


def save_detector_outputs(out_dir: Path, ecg_df: pd.DataFrame, result: DetectorResult) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    peak_times = ecg_df["timestamp_s"].to_numpy()[result.peaks] if len(result.peaks) else np.array([], dtype=float)
    pd.DataFrame({
        "r_peak_index": result.peaks.astype(int),
        "timestamp_s": peak_times,
    }).to_csv(out_dir / "r_peaks.csv", index=False)
    result.rr_df.to_csv(out_dir / "rr.csv", index=False)
    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump({
            "detector": result.name,
            "peak_count": int(len(result.peaks)),
            "hrv": basic_hrv(result.rr_df["rr_ms"].to_numpy() if not result.rr_df.empty else np.array([])),
            "notes": result.notes,
        }, f, indent=2, ensure_ascii=False)


def plot_overlay(out_path: Path, ecg_df: pd.DataFrame, results: list[DetectorResult], fs: float) -> None:
    if plt is None:
        return
    t0 = float(ecg_df["timestamp_s"].iloc[0])
    t = ecg_df["timestamp_s"].to_numpy() - t0
    y = highpass_display(ecg_df["ecg_uV"].to_numpy(dtype=float), fs=fs)
    plt.figure(figsize=(14, 5))
    plt.plot(t, y, linewidth=0.8, label="ECG (highpass display)")
    markers = ["o", "x", "^", "s", "d", "P", "*", "v"]
    for i, res in enumerate(results):
        if len(res.peaks) == 0:
            continue
        px = t[res.peaks]
        py = y[res.peaks]
        plt.scatter(px, py, s=20, marker=markers[i % len(markers)], label=res.name)
    plt.xlabel("Seconds from segment start")
    plt.ylabel("µV (display-filtered)")
    plt.title("R-peak detector overlay")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def run() -> None:
    ap = argparse.ArgumentParser(description="Compare ECG R-peak detectors on a Polar H10 segment")
    ap.add_argument("--ecg", required=True, type=Path, help="Path to RawECG_recording.csv")
    ap.add_argument("--rr", required=False, type=Path, help="Optional path to RRinterval_recording.csv")
    ap.add_argument("--start", required=True, help="Segment start, either Unix seconds or ISO-8601 UTC")
    ap.add_argument("--end", required=True, help="Segment end, either Unix seconds or ISO-8601 UTC")
    ap.add_argument("--out", required=True, type=Path, help="Output directory")
    ap.add_argument("--tolerance-ms", type=float, default=75.0, help="Peak matching tolerance in ms")
    ap.add_argument("--with-wfdb", action="store_true", help="Include wfdb xqrs/gqrs detectors")
    args = ap.parse_args()

    ecg_df = read_ecg_csv(args.ecg)
    rr_df = read_rr_csv(args.rr) if args.rr else None

    def parse_time(v: str) -> float:
        try:
            return float(v)
        except ValueError:
            ts = pd.Timestamp(v)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return float(ts.timestamp())

    start_s = parse_time(args.start)
    end_s = parse_time(args.end)
    if not start_s < end_s:
        raise ValueError("--start must be < --end")

    seg_ecg = ecg_df[(ecg_df["timestamp_s"] >= start_s) & (ecg_df["timestamp_s"] < end_s)].reset_index(drop=True)
    if len(seg_ecg) < 10:
        raise ValueError("Selected ECG segment is too short or empty")
    seg_rr = None
    if rr_df is not None:
        seg_rr = rr_df[(rr_df["timestamp_s"] >= start_s) & (rr_df["timestamp_s"] < end_s)].reset_index(drop=True)

    fs = estimate_fs(seg_ecg["timestamp_s"].to_numpy())
    raw_ecg = seg_ecg["ecg_uV"].to_numpy(dtype=float)

    scripts2_peaks, scripts2_detector = detect_peaks_scripts2_like(raw_ecg, fs)
    neuro_peaks = detect_peaks_neurokit(raw_ecg, fs) if nk is not None else np.array([], dtype=int)
    neuro_peaks = refine_peaks_local_max(raw_ecg, neuro_peaks, fs=fs) if len(neuro_peaks) else neuro_peaks
    baseline_peaks = detect_peaks_baseline(raw_ecg, fs)

    results = [
        DetectorResult(name=f"scripts2_like_{scripts2_detector}", peaks=scripts2_peaks, rr_df=peaks_to_rr_ms(seg_ecg["timestamp_s"].to_numpy(), scripts2_peaks)),
        DetectorResult(name="neurokit2", peaks=neuro_peaks, rr_df=peaks_to_rr_ms(seg_ecg["timestamp_s"].to_numpy(), neuro_peaks), notes=None if nk is not None else "neurokit2 unavailable"),
        DetectorResult(name="baseline_scipy", peaks=baseline_peaks, rr_df=peaks_to_rr_ms(seg_ecg["timestamp_s"].to_numpy(), baseline_peaks)),
    ]

    if args.with_wfdb:
        try:
            wfdb_xqrs = detect_peaks_wfdb_xqrs(raw_ecg, fs)
            wfdb_xqrs = refine_peaks_local_max(raw_ecg, wfdb_xqrs, fs=fs)
            results.append(
                DetectorResult(
                    name="wfdb_xqrs",
                    peaks=wfdb_xqrs,
                    rr_df=peaks_to_rr_ms(seg_ecg["timestamp_s"].to_numpy(), wfdb_xqrs),
                )
            )
        except Exception as exc:
            results.append(
                DetectorResult(
                    name="wfdb_xqrs",
                    peaks=np.array([], dtype=int),
                    rr_df=pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"]),
                    notes=f"xqrs_failed: {exc}",
                )
            )
        try:
            wfdb_gqrs = detect_peaks_wfdb_gqrs(raw_ecg, fs)
            wfdb_gqrs = refine_peaks_local_max(raw_ecg, wfdb_gqrs, fs=fs)
            results.append(
                DetectorResult(
                    name="wfdb_gqrs",
                    peaks=wfdb_gqrs,
                    rr_df=peaks_to_rr_ms(seg_ecg["timestamp_s"].to_numpy(), wfdb_gqrs),
                )
            )
        except Exception as exc:
            results.append(
                DetectorResult(
                    name="wfdb_gqrs",
                    peaks=np.array([], dtype=int),
                    rr_df=pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"]),
                    notes=f"gqrs_failed: {exc}",
                )
            )

    args.out.mkdir(parents=True, exist_ok=True)
    seg_ecg.to_csv(args.out / "segment_ecg.csv", index=False)
    if seg_rr is not None:
        seg_rr.to_csv(args.out / "segment_rr_device.csv", index=False)

    for res in results:
        save_detector_outputs(args.out / res.name, seg_ecg, res)

    comparisons: dict[str, dict] = {}
    peak_times = {
        res.name: seg_ecg["timestamp_s"].to_numpy()[res.peaks] if len(res.peaks) else np.array([], dtype=float)
        for res in results
    }
    for a, b in itertools.combinations(results, 2):
        key = f"{a.name}__vs__{b.name}"
        comparisons[key] = {
            "peak_match": match_peak_times(peak_times[a.name], peak_times[b.name], tolerance_ms=args.tolerance_ms),
            "rr_compare": compare_rr_streams(a.rr_df, b.rr_df),
        }

    if seg_rr is not None:
        for res in results:
            key = f"{res.name}__vs__device_rr"
            comparisons[key] = {"rr_compare": compare_rr_streams(res.rr_df, seg_rr)}

    aggregate = {
        "segment": {
            "start_s": start_s,
            "end_s": end_s,
            "duration_s": end_s - start_s,
            "ecg_sample_count": int(len(seg_ecg)),
            "rr_device_count": int(len(seg_rr)) if seg_rr is not None else None,
            "fs_hz": fs,
        },
        "detectors": {
            res.name: {
                "peak_count": int(len(res.peaks)),
                "hrv": basic_hrv(res.rr_df["rr_ms"].to_numpy() if not res.rr_df.empty else np.array([])),
            }
            for res in results
        },
        "comparisons": comparisons,
    }
    with open(args.out / "comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2, ensure_ascii=False)

    plot_overlay(args.out / "detector_overlay.png", seg_ecg, results, fs=fs)

    print(f"Segment fs: {fs:.3f} Hz")
    for res in results:
        hrv = basic_hrv(res.rr_df["rr_ms"].to_numpy() if not res.rr_df.empty else np.array([]))
        print(f"- {res.name}: peaks={len(res.peaks)} rr={len(res.rr_df)} hr_mean={hrv['hr_mean_bpm']}")
    print(f"Saved outputs to: {args.out}")


if __name__ == "__main__":
    run()
