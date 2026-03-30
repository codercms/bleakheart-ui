from __future__ import annotations

from typing import Any

import numpy as np

RHYTHM_CLASSES = {
    "tachy_candidate",
    "brady_candidate",
    "irregular_rr_candidate",
    "af_like_candidate",
    "ectopy_like_candidate",
    "pause_candidate",
}
SCREENING_PRIORITY_ORDER = ("coverage_gap", "artifact_candidate", "rhythm_candidate", "none")


def _find_episodes(mask: np.ndarray, timestamps_s: np.ndarray, min_duration_s: float) -> list[tuple[float, float, float]]:
    if mask.size == 0 or timestamps_s.size == 0:
        return []

    episodes: list[tuple[float, float, float]] = []
    start_idx: int | None = None
    for idx, flag in enumerate(mask):
        if flag and start_idx is None:
            start_idx = idx
        elif not flag and start_idx is not None:
            start = float(timestamps_s[start_idx])
            end = float(timestamps_s[idx - 1])
            duration = max(0.0, end - start)
            if duration >= min_duration_s:
                episodes.append((start, end, duration))
            start_idx = None

    if start_idx is not None:
        start = float(timestamps_s[start_idx])
        end = float(timestamps_s[-1])
        duration = max(0.0, end - start)
        if duration >= min_duration_s:
            episodes.append((start, end, duration))
    return episodes


def _rolling_median(arr: np.ndarray, window: int) -> np.ndarray:
    if arr.size == 0:
        return arr
    out = np.empty_like(arr)
    half = max(1, window // 2)
    for i in range(arr.size):
        left = max(0, i - half)
        right = min(arr.size, i + half + 1)
        out[i] = np.median(arr[left:right])
    return out


def _screen_rr_pattern(rr_ms: np.ndarray, rr_timestamps_s: np.ndarray) -> dict[str, Any]:
    if rr_ms.size < 4:
        return {
            "tachycardia_episode_count": 0,
            "tachycardia_duration_s": 0.0,
            "bradycardia_episode_count": 0,
            "bradycardia_duration_s": 0.0,
            "irregular_rr_episode_count": 0,
            "irregular_rr_duration_s": 0.0,
            "pause_candidate_count": 0,
            "ectopy_like_candidate_count": 0,
            "af_like_candidate": False,
            "premature_beat_candidate_count": 0,
        }

    hr = 60000.0 / rr_ms

    tachy = hr > 100.0
    brady = hr < 50.0

    rr_diff = np.abs(np.diff(rr_ms))
    rr_change = np.concatenate(([0.0], rr_diff))
    local_med = _rolling_median(rr_ms, window=9)

    irregular_mask = (rr_change > np.maximum(80.0, 0.17 * local_med))
    irregular_mask = irregular_mask | (np.abs(rr_ms - local_med) > np.maximum(90.0, 0.2 * local_med))

    pause_mask = rr_ms > np.maximum(1800.0, 1.85 * local_med)

    short_mask = rr_ms < 0.82 * local_med
    long_following = np.zeros_like(short_mask)
    if short_mask.size > 1:
        long_following[:-1] = rr_ms[1:] > 1.12 * local_med[:-1]
    ectopy = short_mask & long_following

    tachy_episodes = _find_episodes(tachy, rr_timestamps_s, min_duration_s=10.0)
    brady_episodes = _find_episodes(brady, rr_timestamps_s, min_duration_s=10.0)
    irr_episodes = _find_episodes(irregular_mask, rr_timestamps_s, min_duration_s=10.0)

    rr_cv = float(np.std(rr_ms) / max(np.mean(rr_ms), 1e-9))
    pnn50 = float(np.mean(np.abs(np.diff(rr_ms)) > 50.0) * 100.0) if rr_ms.size > 2 else 0.0
    ectopy_ratio = float(np.mean(ectopy)) if ectopy.size else 0.0
    af_like = (rr_cv > 0.11) and (pnn50 > 18.0) and (ectopy_ratio < 0.25)

    return {
        "tachycardia_episode_count": int(len(tachy_episodes)),
        "tachycardia_duration_s": float(sum(x[2] for x in tachy_episodes)),
        "bradycardia_episode_count": int(len(brady_episodes)),
        "bradycardia_duration_s": float(sum(x[2] for x in brady_episodes)),
        "irregular_rr_episode_count": int(len(irr_episodes)),
        "irregular_rr_duration_s": float(sum(x[2] for x in irr_episodes)),
        "pause_candidate_count": int(np.sum(pause_mask)),
        "ectopy_like_candidate_count": int(np.sum(ectopy)),
        "premature_beat_candidate_count": int(np.sum(short_mask)),
        "af_like_candidate": bool(af_like),
    }


def _confidence_level(
    ecg_quality_score_final: float,
    matched_ratio: float,
    artifact_burden: float,
    missing_peak_burden: float,
) -> str:
    if (
        ecg_quality_score_final >= 0.75
        and matched_ratio >= 0.75
        and artifact_burden <= 0.10
        and missing_peak_burden <= 0.15
    ):
        return "high"
    if (
        ecg_quality_score_final >= 0.50
        and matched_ratio >= 0.55
        and artifact_burden <= 0.24
        and missing_peak_burden <= 0.35
    ):
        return "medium"
    return "low"


def _rhythm_screening_class(flags: dict[str, Any]) -> str:
    if bool(flags.get("af_like_candidate")):
        return "af_like_candidate"
    if int(flags.get("pause_candidate_count", 0)) > 0:
        return "pause_candidate"
    if int(flags.get("ectopy_like_candidate_count", 0)) > 0:
        return "ectopy_like_candidate"
    if int(flags.get("irregular_rr_episode_count", 0)) > 0:
        return "irregular_rr_candidate"
    if int(flags.get("tachycardia_episode_count", 0)) > 0:
        return "tachy_candidate"
    if int(flags.get("bradycardia_episode_count", 0)) > 0:
        return "brady_candidate"
    return "none"


def screen_segment(
    rr_ms: np.ndarray,
    rr_timestamps_s: np.ndarray,
    quality_scores: dict[str, float],
    matched_ratio: float,
    artifact_burden: float,
    missing_peak_burden: float,
    rr_mae_ms: float | None,
    low_confidence_matched_ratio: float,
    low_confidence_mae_ms: float,
) -> dict[str, Any]:
    # Priority order is explicit and stable:
    # 1) coverage_gap (handled by caller before this function),
    # 2) artifact_candidate (poor agreement/low-confidence dominates),
    # 3) rhythm_candidate classes,
    # 4) none.
    base = _screen_rr_pattern(rr_ms, rr_timestamps_s)

    ecg_quality_score_final = float(quality_scores.get("ecg_quality_score_final", 0.0))
    confidence = _confidence_level(ecg_quality_score_final, matched_ratio, artifact_burden, missing_peak_burden)

    poor_agreement = (matched_ratio < low_confidence_matched_ratio) or (
        rr_mae_ms is not None and rr_mae_ms > low_confidence_mae_ms
    )

    rhythm_class = _rhythm_screening_class(base)

    if poor_agreement or (confidence == "low" and rhythm_class == "none"):
        screening_class = "artifact_candidate"
        review_reason = "artifact_or_low_agreement"
    else:
        screening_class = rhythm_class
        if screening_class in RHYTHM_CLASSES:
            review_reason = "rhythm_candidate"
        elif confidence == "low":
            review_reason = "low_confidence_non_specific"
        else:
            review_reason = "none"

    review_recommended = screening_class in RHYTHM_CLASSES or screening_class == "artifact_candidate"

    out = {
        **base,
        **quality_scores,
        "confidence_level": confidence,
        "screening_class": screening_class,
        "review_recommended": bool(review_recommended),
        "review_reason": review_reason,
        "agreement_matched_ratio": float(matched_ratio),
        "artifact_burden": float(artifact_burden),
        "missing_peak_burden": float(missing_peak_burden),
        "rr_abs_error_mae_ms": None if rr_mae_ms is None else float(rr_mae_ms),
    }
    return out


def aggregate_screening(segment_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rhythm_candidate_count = int(sum(r.get("screening_class") in RHYTHM_CLASSES for r in segment_rows))
    artifact_or_low_agreement_count = int(sum(r.get("screening_class") == "artifact_candidate" for r in segment_rows))
    coverage_gap_count = int(sum(r.get("screening_class") == "coverage_gap" for r in segment_rows))

    return {
        "segment_count": int(len(segment_rows)),
        "high_confidence_segments": int(sum(x.get("confidence_level") == "high" for x in segment_rows)),
        "medium_confidence_segments": int(sum(x.get("confidence_level") == "medium" for x in segment_rows)),
        "low_confidence_segments": int(sum(x.get("confidence_level") == "low" for x in segment_rows)),
        "review_recommended_segments": int(sum(bool(x.get("review_recommended")) for x in segment_rows)),
        "rhythm_candidate_count": rhythm_candidate_count,
        "artifact_or_low_agreement_count": artifact_or_low_agreement_count,
        "coverage_gap_count": coverage_gap_count,
        "af_like_candidate_segments": int(sum(x.get("screening_class") == "af_like_candidate" for x in segment_rows)),
        "ectopy_like_candidate_count_total": int(sum(int(x.get("ectopy_like_candidate_count", 0)) for x in segment_rows)),
        "pause_candidate_count_total": int(sum(int(x.get("pause_candidate_count", 0)) for x in segment_rows)),
    }
