from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def _save(fig: plt.Figure, path: Path) -> str:
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return str(path)


def plot_hr_trend(
    output_dir: Path,
    rr_timestamps_s: np.ndarray,
    rr_ms_device: np.ndarray,
    rr_timestamps_ecg_s: np.ndarray,
    rr_ms_ecg: np.ndarray,
) -> str | None:
    if rr_ms_device.size == 0:
        return None
    path = output_dir / "hr_trend.png"
    t0 = rr_timestamps_s[0]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot((rr_timestamps_s - t0) / 60.0, 60000.0 / rr_ms_device, linewidth=0.8, label="Device RR-derived HR")
    if rr_ms_ecg.size:
        ax.plot((rr_timestamps_ecg_s - t0) / 60.0, 60000.0 / rr_ms_ecg, linewidth=0.7, alpha=0.8, label="ECG-derived HR")
    ax.set_xlabel("Time (minutes)")
    ax.set_ylabel("Heart rate (bpm)")
    ax.set_title("Heart Rate Trend")
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    return _save(fig, path)


def plot_tachogram_rr(
    output_dir: Path,
    rr_timestamps_s: np.ndarray,
    rr_ms_device: np.ndarray,
    rr_timestamps_ecg_s: np.ndarray,
    rr_ms_ecg: np.ndarray,
) -> str | None:
    if rr_ms_device.size == 0:
        return None
    path = output_dir / "tachogram_rr.png"
    t0 = rr_timestamps_s[0]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot((rr_timestamps_s - t0) / 60.0, rr_ms_device, linewidth=0.8, label="Device RR")
    if rr_ms_ecg.size:
        ax.plot((rr_timestamps_ecg_s - t0) / 60.0, rr_ms_ecg, linewidth=0.7, alpha=0.8, label="ECG-derived RR")
    ax.set_xlabel("Time (minutes)")
    ax.set_ylabel("RR interval (ms)")
    ax.set_title("RR Tachogram")
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    return _save(fig, path)


def plot_poincare(output_dir: Path, rr_ms: np.ndarray) -> str | None:
    if rr_ms.size < 3:
        return None
    path = output_dir / "poincare.png"
    x = rr_ms[:-1]
    y = rr_ms[1:]
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.scatter(x, y, s=8, alpha=0.35)
    lo = float(min(np.min(x), np.min(y)))
    hi = float(max(np.max(x), np.max(y)))
    ax.plot([lo, hi], [lo, hi], linestyle="--", linewidth=1.0)
    ax.set_xlabel("RR(n) ms")
    ax.set_ylabel("RR(n+1) ms")
    ax.set_title("Poincare Plot")
    ax.grid(alpha=0.3)
    return _save(fig, path)


def plot_hrv_psd(output_dir: Path, freqs_hz: np.ndarray, power: np.ndarray) -> str | None:
    if freqs_hz.size == 0 or power.size == 0:
        return None
    path = output_dir / "hrv_psd.png"
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(freqs_hz, power, linewidth=1.0)
    ax.axvspan(0.04, 0.15, alpha=0.12, color="#4c8")
    ax.axvspan(0.15, 0.40, alpha=0.12, color="#88c")
    ax.set_xlim(0.0, min(0.5, float(np.max(freqs_hz))))
    ax.set_xlabel("Frequency (Hz)")
    ax.set_ylabel("Power (ms^2/Hz)")
    ax.set_title("HRV PSD")
    ax.grid(alpha=0.3)
    return _save(fig, path)


def _choose_representative_segments(segment_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not segment_rows:
        return []

    clean = next((x for x in segment_rows if x.get("confidence_level") == "high"), None)
    suspicious = next(
        (
            x
            for x in segment_rows
            if bool(x.get("review_recommended"))
            and (
                bool(x.get("af_like_candidate"))
                or int(x.get("irregular_rr_episode_count", 0)) > 0
                or x.get("screening_class") == "artifact_candidate"
            )
        ),
        None,
    )
    low_quality = next(
        (x for x in segment_rows if x.get("confidence_level") == "low" and x.get("screening_class") != "coverage_gap"),
        None,
    )

    chosen = [x for x in [clean, suspicious, low_quality] if x is not None]
    if len(chosen) < 3:
        for x in segment_rows:
            if x not in chosen:
                chosen.append(x)
            if len(chosen) == 3:
                break
    return chosen[:3]


def plot_ecg_strips(
    output_dir: Path,
    ecg_timestamps_s: np.ndarray,
    ecg_display_uv: np.ndarray,
    r_peak_indices: np.ndarray,
    segment_rows: list[dict[str, Any]],
) -> str | None:
    if ecg_timestamps_s.size == 0 or ecg_display_uv.size == 0:
        return None

    chosen = _choose_representative_segments(segment_rows)
    if not chosen:
        duration = float(ecg_timestamps_s[-1] - ecg_timestamps_s[0])
        starts = [float(ecg_timestamps_s[0]), float(ecg_timestamps_s[0] + duration * 0.5), float(ecg_timestamps_s[-1] - 10.0)]
        chosen = [
            {"segment_start_s": s, "segment_end_s": s + 10.0, "confidence_level": "unknown", "review_recommended": False}
            for s in starts
        ]

    path = output_dir / "ecg_strips.png"
    fig, axes = plt.subplots(len(chosen), 1, figsize=(12, 2.7 * len(chosen)), sharey=True)
    if len(chosen) == 1:
        axes = [axes]

    peak_times = ecg_timestamps_s[r_peak_indices] if r_peak_indices.size else np.asarray([], dtype=np.float64)

    for ax, row in zip(axes, chosen):
        start = float(row.get("segment_start_s", ecg_timestamps_s[0]))
        stop = min(float(row.get("segment_end_s", start + 10.0)), float(ecg_timestamps_s[-1]))
        if stop - start > 12.0:
            stop = start + 10.0

        mask = (ecg_timestamps_s >= start) & (ecg_timestamps_s <= stop)
        t = ecg_timestamps_s[mask]
        y = ecg_display_uv[mask]
        if t.size == 0:
            continue

        t_rel = t - t[0]
        ax.plot(t_rel, y, linewidth=0.9, color="#1f4ea3")

        if peak_times.size:
            pmask = (peak_times >= start) & (peak_times <= stop)
            p_t = peak_times[pmask]
            if p_t.size:
                p_rel = p_t - t[0]
                p_idx = np.searchsorted(t, p_t, side="left")
                p_idx = np.clip(p_idx, 0, y.size - 1)
                ax.scatter(p_rel, y[p_idx], s=11, color="#ff6b6b", alpha=0.8)

        label = (
            f"{row.get('confidence_level', 'unknown')}"
            f" | review={bool(row.get('review_recommended'))}"
            f" | class={row.get('screening_class', 'none')}"
        )
        ax.set_title(f"ECG Strip [{label}] {start:.1f}s-{stop:.1f}s")
        ax.set_ylabel("uV")
        ax.grid(alpha=0.25)

    axes[-1].set_xlabel("Time (s)")
    return _save(fig, path)


def write_all_plots(
    output_dir: Path,
    rr_timestamps_s: np.ndarray,
    rr_ms_device: np.ndarray,
    rr_timestamps_ecg_s: np.ndarray,
    rr_ms_ecg: np.ndarray,
    psd_freqs_hz: np.ndarray,
    psd_power: np.ndarray,
    ecg_timestamps_s: np.ndarray,
    ecg_display_uv: np.ndarray,
    r_peak_indices: np.ndarray,
    segment_rows: list[dict[str, Any]],
) -> dict[str, str]:
    outputs: dict[str, str] = {}
    plots = {
        "hr_trend.png": plot_hr_trend(output_dir, rr_timestamps_s, rr_ms_device, rr_timestamps_ecg_s, rr_ms_ecg),
        "tachogram_rr.png": plot_tachogram_rr(output_dir, rr_timestamps_s, rr_ms_device, rr_timestamps_ecg_s, rr_ms_ecg),
        "poincare.png": plot_poincare(output_dir, rr_ms_device),
        "hrv_psd.png": plot_hrv_psd(output_dir, psd_freqs_hz, psd_power),
        "ecg_strips.png": plot_ecg_strips(
            output_dir,
            ecg_timestamps_s,
            ecg_display_uv,
            r_peak_indices,
            segment_rows,
        ),
    }
    for name, path in plots.items():
        if path:
            outputs[name] = path
    return outputs
