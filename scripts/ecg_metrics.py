from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.signal import butter, filtfilt, find_peaks

try:
    import neurokit2 as nk  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    nk = None


@dataclass
class EcgProcessingResult:
    metrics: dict[str, Any]
    detector_signal_uv: np.ndarray
    display_signal_uv: np.ndarray
    r_peak_indices: np.ndarray
    r_peak_timestamps_s: np.ndarray
    rr_timestamps_s: np.ndarray
    rr_ms: np.ndarray
    hr_bpm: np.ndarray


def _clamp01(value: float) -> float:
    return float(np.clip(value, 0.0, 1.0))


def estimate_fs_hz(timestamps_s: np.ndarray) -> dict[str, float]:
    dts = np.diff(timestamps_s)
    dts = dts[dts > 0]
    if dts.size == 0:
        return {"fs_median_hz": 0.0, "fs_mean_hz": 0.0, "dt_min_s": 0.0, "dt_max_s": 0.0}
    return {
        "fs_median_hz": float(1.0 / np.median(dts)),
        "fs_mean_hz": float(1.0 / np.mean(dts)),
        "dt_min_s": float(np.min(dts)),
        "dt_max_s": float(np.max(dts)),
    }


def _apply_bandpass(signal_uv: np.ndarray, fs_hz: float, low_hz: float, high_hz: float, order: int = 2) -> np.ndarray:
    if signal_uv.size < 5 or fs_hz <= 2.0:
        return signal_uv.copy()
    nyq = fs_hz / 2.0
    low = max(0.01, low_hz / nyq)
    high = min(0.99, high_hz / nyq)
    if high <= low:
        return signal_uv.copy()
    b, a = butter(order, [low, high], btype="band")
    return filtfilt(b, a, signal_uv)


def preprocess_for_detection(ecg_uv: np.ndarray, fs_hz: float) -> np.ndarray:
    return _apply_bandpass(ecg_uv, fs_hz, low_hz=3.0, high_hz=min(25.0, fs_hz * 0.45), order=2)


def preprocess_for_display(ecg_uv: np.ndarray, fs_hz: float) -> np.ndarray:
    return _apply_bandpass(ecg_uv, fs_hz, low_hz=0.5, high_hz=min(40.0, fs_hz * 0.45), order=2)


def _mad(x: np.ndarray) -> float:
    med = float(np.median(x))
    return max(float(np.median(np.abs(x - med))), 1e-9)


def _detect_peaks_neurokit(detector_signal_uv: np.ndarray, fs_hz: float) -> np.ndarray:
    if nk is None:
        return np.asarray([], dtype=np.int64)
    if detector_signal_uv.size < int(fs_hz * 4):
        return np.asarray([], dtype=np.int64)
    try:
        _, info = nk.ecg_peaks(detector_signal_uv, sampling_rate=max(10, int(round(fs_hz))))
        peaks = np.asarray(info.get("ECG_R_Peaks", []), dtype=np.int64)
        return peaks[(peaks >= 0) & (peaks < detector_signal_uv.size)]
    except Exception:
        return np.asarray([], dtype=np.int64)


def _detect_peaks_fallback(detector_signal_uv: np.ndarray, fs_hz: float, expected_hr_bpm: float | None) -> np.ndarray:
    z = (detector_signal_uv - np.median(detector_signal_uv)) / _mad(detector_signal_uv)
    envelope = np.abs(z)
    window = max(3, int(fs_hz * 0.08))
    smooth = np.convolve(envelope, np.ones(window) / window, mode="same")

    if expected_hr_bpm and expected_hr_bpm > 0:
        rr_s = 60.0 / expected_hr_bpm
        min_distance_s = min(0.9, max(0.30, rr_s * 0.45))
    else:
        min_distance_s = 0.32

    peaks, _ = find_peaks(
        smooth,
        distance=max(1, int(fs_hz * min_distance_s)),
        height=max(1.0, float(np.percentile(smooth, 88))),
        prominence=max(0.4, float(np.percentile(smooth, 88)) * 0.2),
    )

    if peaks.size < 6:
        peaks, _ = find_peaks(
            smooth,
            distance=max(1, int(fs_hz * 0.25)),
            height=max(0.8, float(np.percentile(smooth, 80))),
            prominence=0.3,
        )
    return np.asarray(peaks, dtype=np.int64)


def refine_peaks_with_local_max(
    source_signal_uv: np.ndarray,
    peak_indices: np.ndarray,
    fs_hz: float,
) -> np.ndarray:
    if peak_indices.size == 0:
        return peak_indices
    radius = max(1, int(fs_hz * 0.08))
    refined: list[int] = []
    for peak_idx in peak_indices:
        left = max(0, int(peak_idx) - radius)
        right = min(source_signal_uv.size, int(peak_idx) + radius + 1)
        if right <= left:
            continue
        local = source_signal_uv[left:right]
        refined.append(int(left + np.argmax(local)))

    if not refined:
        return np.asarray([], dtype=np.int64)

    unique = np.unique(np.asarray(refined, dtype=np.int64))
    if unique.size <= 1:
        return unique

    min_gap = max(1, int(fs_hz * 0.22))
    kept = [int(unique[0])]
    for idx in unique[1:]:
        if idx - kept[-1] >= min_gap:
            kept.append(int(idx))
        elif source_signal_uv[idx] > source_signal_uv[kept[-1]]:
            kept[-1] = int(idx)
    return np.asarray(kept, dtype=np.int64)


def _clean_rr(rr_ms: np.ndarray, rr_timestamps_s: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if rr_ms.size == 0:
        empty_mask = np.asarray([], dtype=bool)
        return rr_ms, rr_timestamps_s, empty_mask

    physiologic = (rr_ms >= 280.0) & (rr_ms <= 2200.0)
    rr_phys = rr_ms[physiologic]
    if rr_phys.size < 5:
        mask = physiologic
    else:
        med = float(np.median(rr_phys))
        mad = _mad(rr_phys)
        robust_z = np.abs(rr_ms - med) / (1.4826 * mad)
        mask = physiologic & (robust_z <= 6.0) & (rr_ms >= 0.45 * med) & (rr_ms <= 1.9 * med)
    return rr_ms[mask], rr_timestamps_s[mask], mask


def _estimate_quality(detector_signal_uv: np.ndarray, rr_ms_raw: np.ndarray, rr_ms_clean: np.ndarray) -> tuple[float, float, float]:
    if detector_signal_uv.size == 0:
        return 0.0, 1.0, 0.0
    centered = detector_signal_uv - np.median(detector_signal_uv)
    power = float(np.mean(centered**2))
    slope = np.diff(centered)
    hf_noise = float(np.mean(slope**2)) if slope.size else power
    noise_ratio = hf_noise / max(power, 1e-9)

    outlier_ratio = 1.0
    if rr_ms_raw.size:
        outlier_ratio = 1.0 - (rr_ms_clean.size / rr_ms_raw.size)

    score = 1.0
    score -= min(0.6, max(0.0, noise_ratio - 1.3) * 0.15)
    score -= min(0.5, outlier_ratio * 0.8)
    score = float(np.clip(score, 0.0, 1.0))
    return score, float(noise_ratio), float(outlier_ratio)


def _moving_average(signal_uv: np.ndarray, window_samples: int) -> np.ndarray:
    if signal_uv.size == 0:
        return signal_uv
    if window_samples <= 1:
        return signal_uv.copy()
    kernel = np.ones(window_samples, dtype=np.float64) / window_samples
    return np.convolve(signal_uv, kernel, mode="same")


def _template_similarity_score(
    signal_uv: np.ndarray,
    peak_indices: np.ndarray,
    fs_hz: float,
) -> float:
    if peak_indices.size < 5 or signal_uv.size < 16 or fs_hz <= 2.0:
        return 0.5

    left = max(1, int(fs_hz * 0.12))
    right = max(2, int(fs_hz * 0.20))
    snippets: list[np.ndarray] = []
    for idx in peak_indices:
        l = int(idx) - left
        r = int(idx) + right
        if l < 0 or r >= signal_uv.size:
            continue
        beat = signal_uv[l:r].astype(np.float64)
        beat -= np.mean(beat)
        norm = np.linalg.norm(beat)
        if norm > 1e-9:
            snippets.append(beat / norm)
    if len(snippets) < 4:
        return 0.5

    mat = np.vstack(snippets)
    template = np.median(mat, axis=0)
    template -= np.mean(template)
    t_norm = np.linalg.norm(template)
    if t_norm <= 1e-9:
        return 0.5
    template /= t_norm

    corrs = np.clip(mat @ template, -1.0, 1.0)
    median_corr = float(np.median(corrs))
    score = (median_corr + 1.0) / 2.0
    return _clamp01(score)


def segment_quality_breakdown(
    ecg_timestamps_s: np.ndarray,
    display_signal_uv: np.ndarray,
    r_peak_indices: np.ndarray,
    segment_start_s: float,
    segment_end_s: float,
    matched_ratio: float,
    rr_mae_ms: float | None,
    missing_peak_burden: float,
) -> dict[str, float]:
    seg_mask = (ecg_timestamps_s >= segment_start_s) & (ecg_timestamps_s < segment_end_s)
    seg_signal = display_signal_uv[seg_mask]
    seg_time = ecg_timestamps_s[seg_mask]

    if seg_signal.size < 8 or seg_time.size < 8:
        return {
            "baseline_noise_score": 0.0,
            "hf_noise_score": 0.0,
            "peak_consistency_score": 0.0,
            "template_similarity_score": 0.0,
            "agreement_score": 0.0,
            "ecg_quality_score_final": 0.0,
        }

    fs_hz = estimate_fs_hz(seg_time)["fs_median_hz"]
    if fs_hz <= 2.0:
        fs_hz = 130.0

    baseline_window = max(5, int(fs_hz * 0.75))
    baseline = _moving_average(seg_signal, baseline_window)
    baseline_residual = seg_signal - baseline
    baseline_ratio = float(np.std(baseline) / max(np.std(seg_signal), 1e-9))
    baseline_noise_score = _clamp01(1.0 - np.clip((baseline_ratio - 0.06) / 0.45, 0.0, 1.0))

    hf_proxy = np.diff(baseline_residual)
    hf_ratio = float(np.std(hf_proxy) / max(np.std(seg_signal), 1e-9)) if hf_proxy.size else 1.0
    hf_noise_score = _clamp01(1.0 - np.clip((hf_ratio - 0.18) / 1.15, 0.0, 1.0))

    seg_peaks = r_peak_indices[(ecg_timestamps_s[r_peak_indices] >= segment_start_s) & (ecg_timestamps_s[r_peak_indices] < segment_end_s)]
    if seg_peaks.size >= 4:
        peak_amp = np.abs(display_signal_uv[seg_peaks])
        amp_cv = float(np.std(peak_amp) / max(np.mean(peak_amp), 1e-9))
        peak_consistency_score = _clamp01(1.0 - np.clip((amp_cv - 0.10) / 0.95, 0.0, 1.0))
    else:
        peak_consistency_score = 0.35

    template_similarity = _template_similarity_score(display_signal_uv, seg_peaks, fs_hz)

    mae_component = 0.0 if rr_mae_ms is None else _clamp01(1.0 - np.clip((rr_mae_ms - 18.0) / 150.0, 0.0, 1.0))
    agreement_score = _clamp01(0.65 * matched_ratio + 0.35 * mae_component)

    missing_penalty = _clamp01(1.0 - np.clip(missing_peak_burden / 0.55, 0.0, 1.0))
    quality = (
        0.22 * baseline_noise_score
        + 0.18 * hf_noise_score
        + 0.20 * peak_consistency_score
        + 0.20 * template_similarity
        + 0.20 * agreement_score
    ) * (0.55 + 0.45 * missing_penalty)

    return {
        "baseline_noise_score": float(baseline_noise_score),
        "hf_noise_score": float(hf_noise_score),
        "peak_consistency_score": float(peak_consistency_score),
        "template_similarity_score": float(template_similarity),
        "agreement_score": float(agreement_score),
        "ecg_quality_score_final": float(_clamp01(quality)),
    }


def analyze_ecg(
    timestamps_s: np.ndarray,
    ecg_uv: np.ndarray,
    expected_hr_bpm: float | None,
) -> EcgProcessingResult:
    if timestamps_s.size < 4 or ecg_uv.size < 4:
        empty_f = np.asarray([], dtype=np.float64)
        empty_i = np.asarray([], dtype=np.int64)
        return EcgProcessingResult(
            metrics={"status": "insufficient_data", "sample_count": int(ecg_uv.size)},
            detector_signal_uv=ecg_uv.copy(),
            display_signal_uv=ecg_uv.copy(),
            r_peak_indices=empty_i,
            r_peak_timestamps_s=empty_f,
            rr_timestamps_s=empty_f,
            rr_ms=empty_f,
            hr_bpm=empty_f,
        )

    fs = estimate_fs_hz(timestamps_s)
    fs_hz = fs["fs_median_hz"]

    detector_signal = preprocess_for_detection(ecg_uv, fs_hz)
    display_signal = preprocess_for_display(ecg_uv, fs_hz)

    peaks_nk = _detect_peaks_neurokit(detector_signal, fs_hz)
    peaks_fb = _detect_peaks_fallback(detector_signal, fs_hz, expected_hr_bpm)

    if peaks_nk.size >= 6:
        peaks_initial = peaks_nk
        detector_used = "neurokit2"
    else:
        peaks_initial = peaks_fb
        detector_used = "fallback_find_peaks"

    peaks_refined = refine_peaks_with_local_max(display_signal, peaks_initial, fs_hz)

    peak_times = timestamps_s[peaks_refined] if peaks_refined.size else np.asarray([], dtype=np.float64)
    rr_ms_raw = np.diff(peak_times) * 1000.0 if peak_times.size > 1 else np.asarray([], dtype=np.float64)
    rr_t_raw = peak_times[1:] if peak_times.size > 1 else np.asarray([], dtype=np.float64)
    rr_ms, rr_timestamps, clean_mask = _clean_rr(rr_ms_raw, rr_t_raw)
    hr_bpm = 60000.0 / rr_ms if rr_ms.size else np.asarray([], dtype=np.float64)

    quality_score, noise_ratio, rr_outlier_burden = _estimate_quality(detector_signal, rr_ms_raw, rr_ms)
    beat_density_per_min = float(peaks_refined.size / max((timestamps_s[-1] - timestamps_s[0]) / 60.0, 1e-6))

    metrics: dict[str, Any] = {
        "status": "ok",
        "sample_count": int(ecg_uv.size),
        "duration_s": float(timestamps_s[-1] - timestamps_s[0]),
        "sampling_rate_hz_median": fs["fs_median_hz"],
        "sampling_rate_hz_mean": fs["fs_mean_hz"],
        "dt_min_s": fs["dt_min_s"],
        "dt_max_s": fs["dt_max_s"],
        "detector_used": detector_used,
        "r_peak_count": int(peaks_refined.size),
        "rr_count_raw": int(rr_ms_raw.size),
        "rr_count_clean": int(rr_ms.size),
        "rr_outlier_removed_count": int(rr_ms_raw.size - rr_ms.size),
        "rr_outlier_burden": float(rr_outlier_burden),
        "heart_rate_bpm_mean_from_ecg": float(np.mean(hr_bpm)) if hr_bpm.size else None,
        "heart_rate_bpm_min_from_ecg": float(np.min(hr_bpm)) if hr_bpm.size else None,
        "heart_rate_bpm_max_from_ecg": float(np.max(hr_bpm)) if hr_bpm.size else None,
        "rr_ms_mean_from_ecg": float(np.mean(rr_ms)) if rr_ms.size else None,
        "rr_ms_median_from_ecg": float(np.median(rr_ms)) if rr_ms.size else None,
        "beat_density_per_min": beat_density_per_min,
        "ecg_quality_score": quality_score,
        "ecg_noise_ratio": float(noise_ratio),
        "rr_clean_mask_true_count": int(np.sum(clean_mask)),
    }

    return EcgProcessingResult(
        metrics=metrics,
        detector_signal_uv=detector_signal,
        display_signal_uv=display_signal,
        r_peak_indices=peaks_refined,
        r_peak_timestamps_s=peak_times,
        rr_timestamps_s=rr_timestamps,
        rr_ms=rr_ms,
        hr_bpm=hr_bpm,
    )
