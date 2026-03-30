from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SUSPICION_CLASSES = {
    "af_like_candidate",
    "irregular_rr_candidate",
    "ectopy_like_candidate",
    "pause_candidate",
    "tachy_candidate",
    "brady_candidate",
    "artifact_candidate",
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    if not np.isfinite(v):
        return None
    return v


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, tuple):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _map_suspicion_type(screening_class: str) -> str | None:
    mapping = {
        "af_like_candidate": "af_like_irregularity_suspicion",
        "irregular_rr_candidate": "af_like_irregularity_suspicion",
        "ectopy_like_candidate": "premature_beat_like_suspicion",
        "pause_candidate": "pause_or_dropout_suspicion",
        "tachy_candidate": "tachy_episode_suspicion",
        "brady_candidate": "brady_episode_suspicion",
        "artifact_candidate": "artifact_suspicion",
    }
    return mapping.get(screening_class)


def _manual_checklist(suspicion_type: str) -> list[str]:
    if suspicion_type == "af_like_irregularity_suspicion":
        return [
            "Is the rhythm irregularly irregular rather than mildly variable?",
            "Is there clear repeating short-long periodicity?",
            "Is waveform quality sufficient to trust beat detection?",
            "Could movement/contact noise explain the irregularity?",
        ]
    if suspicion_type == "premature_beat_like_suspicion":
        return [
            "Is one beat clearly earlier than expected from local rhythm?",
            "Is a compensatory pause visible after the early beat?",
            "Does the suspected beat shape differ from neighboring beats?",
            "Is this isolated or part of a repeated sequence?",
        ]
    if suspicion_type == "pause_or_dropout_suspicion":
        return [
            "Is there a true long interval without visible QRS complexes?",
            "Is there contact loss, flattening, or high noise in this zone?",
            "Could one R-peak be missed by detector logic?",
            "Does another detector confirm the same gap timing?",
        ]
    if suspicion_type == "tachy_episode_suspicion":
        return [
            "Is elevated HR sustained across multiple beats, not a single outlier?",
            "Does episode onset/offset look abrupt or gradual?",
            "Is rhythm regular or irregular during fast period?",
            "Could movement/stress context explain elevated rate?",
        ]
    if suspicion_type == "brady_episode_suspicion":
        return [
            "Is slow HR sustained rather than one long RR interval?",
            "Is detector agreement acceptable in the slow segment?",
            "Could missed peaks or dropout create pseudo-brady pattern?",
            "Is rhythm morphology stable across the episode?",
        ]
    return [
        "Is there strong baseline wander, sharp spikes, or unstable contact?",
        "Do independent detectors disagree on beat timing?",
        "Is RR behavior physiologically implausible for this context?",
        "Treat this segment primarily as signal-quality-limited.",
    ]


def _false_explanations(suspicion_type: str) -> list[str]:
    if suspicion_type == "af_like_irregularity_suspicion":
        return ["respiratory sinus arrhythmia", "motion artifact", "missed/extra R-peaks"]
    if suspicion_type == "premature_beat_like_suspicion":
        return ["false R-peak", "contact artifact", "noisy baseline"]
    if suspicion_type == "pause_or_dropout_suspicion":
        return ["missed detection", "contact loss", "motion artifact"]
    if suspicion_type == "tachy_episode_suspicion":
        return ["artifact burst", "missed interval", "movement/stress context"]
    if suspicion_type == "brady_episode_suspicion":
        return ["missed R-peaks", "contact issue", "segment-edge effect"]
    return ["primary interpretation remains artifact/signal limitation"]


def _strength(
    suspicion_type: str,
    confidence_level: str,
    ecg_quality_score_final: float | None,
    matched_ratio: float | None,
    rr_mae_ms: float | None,
    supporting_count: int,
) -> str:
    score = 0.0
    if confidence_level == "high":
        score += 1.4
    elif confidence_level == "medium":
        score += 0.8
    else:
        score += 0.2

    if ecg_quality_score_final is not None:
        score += 1.2 * max(0.0, min(1.0, ecg_quality_score_final))

    if matched_ratio is not None:
        score += 1.0 * max(0.0, min(1.0, matched_ratio))
    if rr_mae_ms is not None:
        score += max(0.0, 1.0 - min(rr_mae_ms, 180.0) / 180.0)

    score += min(1.6, supporting_count * 0.4)

    if suspicion_type == "artifact_suspicion":
        bad_quality = 0.0 if ecg_quality_score_final is None else max(0.0, 1.0 - ecg_quality_score_final)
        bad_agree = 0.0 if matched_ratio is None else max(0.0, 1.0 - matched_ratio)
        bad_err = 0.0 if rr_mae_ms is None else min(rr_mae_ms / 150.0, 1.2)
        artifact_score = 0.8 + 1.4 * bad_quality + 1.1 * bad_agree + 0.7 * bad_err
        if artifact_score >= 2.2:
            return "high"
        if artifact_score >= 1.4:
            return "medium"
        return "low"

    if score >= 4.0:
        return "high"
    if score >= 2.5:
        return "medium"
    return "low"


def _primary_reason(suspicion_type: str, row: dict[str, Any]) -> str:
    q = _safe_float(row.get("ecg_quality_score_final"))
    ratio = _safe_float(row.get("agreement_matched_ratio"))
    mae = _safe_float(row.get("rr_abs_error_mae_ms"))

    q_txt = "usable" if (q is not None and q >= 0.7) else "limited"
    agree_txt = "good" if (ratio is not None and ratio >= 0.75 and (mae is None or mae <= 80.0)) else "limited"

    if suspicion_type == "af_like_irregularity_suspicion":
        return (
            "RR intervals are irregular without a simple repeating pattern; "
            f"segment quality is {q_txt} and detector agreement is {agree_txt}."
        )
    if suspicion_type == "premature_beat_like_suspicion":
        return (
            "A short-then-long RR sequence suggests an early-beat-like pattern; "
            f"segment quality is {q_txt} and detector agreement is {agree_txt}."
        )
    if suspicion_type == "pause_or_dropout_suspicion":
        return (
            "A prolonged RR/gap was detected, but this can represent either true pause-like behavior "
            "or missed detection/dropout; manual waveform review is required."
        )
    if suspicion_type == "tachy_episode_suspicion":
        return (
            "Sustained sequence of elevated HR suggests a tachy-like episode in this segment; "
            f"quality is {q_txt} and agreement is {agree_txt}."
        )
    if suspicion_type == "brady_episode_suspicion":
        return (
            "Sustained sequence of low HR suggests a brady-like episode in this segment; "
            f"quality is {q_txt} and agreement is {agree_txt}."
        )
    return (
        "Rhythm interpretation is limited by signal quality/agreement; artifact explanation is favored for this segment."
    )


def _morphology_support_level(row: dict[str, Any]) -> str:
    tmpl = _safe_float(row.get("template_similarity_score"))
    peak = _safe_float(row.get("peak_consistency_score"))
    if tmpl is None and peak is None:
        return "unavailable"
    values = [v for v in [tmpl, peak] if v is not None]
    mean_v = float(np.mean(values)) if values else 0.0
    if mean_v >= 0.8:
        return "high"
    if mean_v >= 0.65:
        return "medium"
    return "low"


def _premature_artifact_clues(
    row: dict[str, Any],
    seg: dict[str, Any],
    matched_ratio: float | None,
    rr_mae_ms: float | None,
    ecg_quality: float | None,
) -> list[str]:
    clues: list[str] = []
    if ecg_quality is not None and ecg_quality < 0.6:
        clues.append("low_segment_quality")
    if matched_ratio is not None and matched_ratio < 0.75:
        clues.append("detector_agreement_limited")
    if rr_mae_ms is not None and rr_mae_ms > 80.0:
        clues.append("high_rr_error")
    if (_safe_float(row.get("template_similarity_score")) or 0.0) < 0.6:
        clues.append("low_template_similarity")
    if (_safe_float(row.get("peak_consistency_score")) or 0.0) < 0.6:
        clues.append("low_peak_consistency")
    if (_safe_float(row.get("artifact_burden")) or 0.0) > 0.2:
        clues.append("high_artifact_burden")
    if (_safe_float(row.get("missing_peak_burden")) or 0.0) > 0.15:
        clues.append("high_missing_peak_burden")

    h0, h1, _ = _highlight_window("premature_beat_like_suspicion", seg)
    ecg_t = seg.get("ecg_t", np.array([], dtype=float))
    ecg = seg.get("ecg", np.array([], dtype=float))
    if h0 is not None and h1 is not None and isinstance(ecg_t, np.ndarray) and isinstance(ecg, np.ndarray) and ecg.size > 20:
        m = (ecg_t >= h0) & (ecg_t <= h1)
        if np.any(m):
            local = np.abs(ecg[m])
            global_ref = float(np.nanpercentile(np.abs(ecg), 95))
            local_p99 = float(np.nanpercentile(local, 99))
            if global_ref > 1e-6 and local_p99 / global_ref > 2.8:
                clues.append("extreme_amplitude_spike_near_trigger")
    return clues


def _premature_refinement(
    row: dict[str, Any],
    seg: dict[str, Any],
    matched_ratio: float | None,
    rr_mae_ms: float | None,
    ecg_quality: float | None,
) -> dict[str, Any]:
    ectopy_count = int(float(row.get("ectopy_like_candidate_count", 0) or 0))
    premature_count = int(float(row.get("premature_beat_candidate_count", 0) or 0))
    event_count = max(ectopy_count, premature_count, 0)
    is_isolated = event_count <= 1

    morphology_support = _morphology_support_level(row)
    artifact_clues = _premature_artifact_clues(row, seg, matched_ratio, rr_mae_ms, ecg_quality)
    strong_artifact = (
        "extreme_amplitude_spike_near_trigger" in artifact_clues
        or len(artifact_clues) >= 3
    )
    artifact_favored = bool(strong_artifact or (is_isolated and len(artifact_clues) >= 2))

    good_agreement = (matched_ratio is not None and matched_ratio >= 0.8) and (rr_mae_ms is None or rr_mae_ms <= 60.0)
    good_quality = ecg_quality is not None and ecg_quality >= 0.7
    repeated_pattern = event_count >= 2

    if artifact_favored and is_isolated:
        suspicion_type = "artifact_suspicion"
        interpretation_strength = "medium" if len(artifact_clues) < 4 else "high"
        primary_reason = (
            "Premature-like RR pattern is present but isolated, and artifact explanation is favored "
            "because local signal behavior is spike/noise-like."
        )
    else:
        suspicion_type = "premature_beat_like_suspicion"
        # Isolated-event penalty: isolated RR pattern alone cannot exceed low.
        if is_isolated and not (good_agreement and good_quality and morphology_support == "high" and not artifact_favored):
            interpretation_strength = "low"
        elif (
            repeated_pattern
            and good_agreement
            and good_quality
            and morphology_support == "high"
            and not artifact_favored
        ):
            interpretation_strength = "high"
        elif (
            repeated_pattern
            and (matched_ratio is not None and matched_ratio >= 0.75)
            and (ecg_quality is not None and ecg_quality >= 0.6)
            and morphology_support in {"medium", "high"}
            and len(artifact_clues) <= 1
        ):
            interpretation_strength = "medium"
        else:
            interpretation_strength = "low"

        if is_isolated:
            primary_reason = (
                "Isolated premature-beat-like RR pattern detected. Evidence remains limited for rhythm interpretation; "
                "manual review is required before treating this as non-artifact."
            )
        else:
            primary_reason = (
                "Repeated premature-like RR pattern detected with supporting segment evidence; "
                "manual review is still required in single-lead wearable data."
            )

    return {
        "suspicion_type": suspicion_type,
        "interpretation_strength": interpretation_strength,
        "event_count_supporting_pattern": event_count,
        "is_isolated_event": is_isolated,
        "artifact_favored": artifact_favored,
        "artifact_clues": artifact_clues,
        "morphology_support_level": morphology_support,
        "primary_reason_override": primary_reason,
    }


def _confidence_interpretation_note(suspicion_type: str, confidence_level: str, interpretation_strength: str) -> str:
    if suspicion_type == "artifact_suspicion":
        return (
            f"Segment confidence is {confidence_level}; interpretation strength is {interpretation_strength}. "
            "This means rhythm interpretation is limited, but artifact explanation is strongly favored."
        )
    return (
        f"Segment confidence is {confidence_level}; interpretation strength is {interpretation_strength}. "
        "These fields are different: confidence reflects signal trust, interpretation strength reflects pattern support."
    )


def _segment_data(
    start_s: float,
    end_s: float,
    ecg_t_s: np.ndarray,
    ecg_display_uv: np.ndarray,
    r_peak_indices: np.ndarray,
    rr_t_s: np.ndarray,
    rr_ms: np.ndarray,
) -> dict[str, Any]:
    ecg_mask = (ecg_t_s >= start_s) & (ecg_t_s < end_s)
    rr_mask = (rr_t_s >= start_s) & (rr_t_s < end_s)

    seg_t = ecg_t_s[ecg_mask]
    seg_ecg = ecg_display_uv[ecg_mask]
    seg_rr_t = rr_t_s[rr_mask]
    seg_rr = rr_ms[rr_mask]

    peak_t = ecg_t_s[r_peak_indices] if r_peak_indices.size else np.array([], dtype=float)
    peak_mask = (peak_t >= start_s) & (peak_t < end_s)
    seg_peak_t = peak_t[peak_mask]
    return {"ecg_t": seg_t, "ecg": seg_ecg, "rr_t": seg_rr_t, "rr_ms": seg_rr, "peak_t": seg_peak_t}


def _highlight_window(suspicion_type: str, seg: dict[str, Any]) -> tuple[float | None, float | None, str]:
    rr_t = seg["rr_t"]
    rr = seg["rr_ms"]

    if rr.size == 0:
        return None, None, "no_rr_data"

    hr = 60000.0 / rr
    local_med = np.median(rr)

    def _best_contiguous(mask: np.ndarray) -> tuple[int, int] | None:
        idx = np.where(mask)[0]
        if idx.size == 0:
            return None
        best = (int(idx[0]), int(idx[0]))
        run_start = int(idx[0])
        prev = int(idx[0])
        best_len = 1
        for k in idx[1:]:
            k = int(k)
            if k == prev + 1:
                prev = k
                continue
            run_len = prev - run_start + 1
            if run_len > best_len:
                best = (run_start, prev)
                best_len = run_len
            run_start = k
            prev = k
        run_len = prev - run_start + 1
        if run_len > best_len:
            best = (run_start, prev)
        return best

    if suspicion_type == "premature_beat_like_suspicion" and rr.size > 2:
        # Prefer explicit short-then-long pattern; if strict thresholds fail,
        # still focus on the strongest relative short->long transition.
        short = rr < 0.82 * local_med
        long_next = np.zeros_like(short, dtype=bool)
        long_next[:-1] = rr[1:] > 1.12 * local_med
        strict_idx = np.where(short & long_next)[0]
        if strict_idx.size:
            i = int(strict_idx[0])
            center = float(rr_t[i])
            return center - 2.5, center + 2.5, "short_then_compensatory_long_rr"

        rr_rel = rr / max(local_med, 1e-6)
        next_rel = np.ones_like(rr_rel)
        next_rel[:-1] = rr_rel[1:]
        score = np.maximum(0.0, 1.0 - rr_rel) * np.maximum(0.0, next_rel - 1.0)
        j = int(np.argmax(score))
        if score[j] > 0.01:
            center = float(rr_t[j])
            return center - 2.5, center + 2.5, "best_short_then_long_transition"

        # Final fallback for ectopy-like suspicion: center on shortest RR.
        j = int(np.argmin(rr))
        center = float(rr_t[j])
        return center - 2.5, center + 2.5, "shortest_rr_fallback"

    if suspicion_type == "pause_or_dropout_suspicion":
        idx = np.where(rr > max(1800.0, 1.85 * local_med))[0]
        if idx.size:
            i = int(idx[0])
            center = float(rr_t[i])
            return center - 3.0, center + 3.0, "prolonged_rr_gap"
        j = int(np.argmax(rr))
        if rr[j] > 1.35 * local_med:
            center = float(rr_t[j])
            return center - 3.0, center + 3.0, "longest_rr_fallback"

    if suspicion_type == "tachy_episode_suspicion":
        run = _best_contiguous(hr > 100.0)
        if run is not None:
            a, b = run
            return float(rr_t[a]) - 1.5, float(rr_t[b]) + 1.5, "longest_tachy_run"
        j = int(np.argmax(hr))
        if hr[j] > 95.0:
            c = float(rr_t[j])
            return c - 2.5, c + 2.5, "peak_hr_fallback"

    if suspicion_type == "brady_episode_suspicion":
        run = _best_contiguous(hr < 50.0)
        if run is not None:
            a, b = run
            return float(rr_t[a]) - 1.5, float(rr_t[b]) + 1.5, "longest_brady_run"
        j = int(np.argmin(hr))
        if hr[j] < 55.0:
            c = float(rr_t[j])
            return c - 2.5, c + 2.5, "lowest_hr_fallback"

    if suspicion_type == "af_like_irregularity_suspicion":
        if rr.size >= 5:
            # Focus on the most irregular local RR zone (short rolling window).
            w = 5
            pad = w // 2
            rr_pad = np.pad(rr, (pad, pad), mode="edge")
            loc_std = np.array([np.std(rr_pad[i : i + w]) for i in range(rr.size)], dtype=float)
            j = int(np.argmax(loc_std))
            c = float(rr_t[j])
            return c - 3.0, c + 3.0, "max_local_rr_irregularity"
        return float(rr_t[0]), float(rr_t[-1]), "irregular_rr_pattern_across_segment"

    if suspicion_type == "artifact_suspicion":
        if rr.size >= 4:
            # Local artifact proxy: combined amplitude deviation + rapid RR jumps.
            rr_dev = np.abs(rr - local_med) / max(local_med, 1e-6)
            rr_jump = np.zeros_like(rr_dev)
            rr_jump[1:] = np.abs(np.diff(rr)) / max(local_med, 1e-6)
            score = 0.65 * rr_dev + 0.35 * rr_jump
            j = int(np.argmax(score))
            c = float(rr_t[j])
            return c - 3.0, c + 3.0, "max_rr_instability_for_artifact"
        return float(rr_t[0]), float(rr_t[-1]), "poor_agreement_or_quality_pattern"

    return float(rr_t[0]), float(rr_t[-1]), "segment_review"


def _plot_suspicion_overview(
    out_png: Path,
    segment_id: int,
    suspicion_type: str,
    interpretation_strength: str,
    key_clue: str,
    annotation_note: str,
    seg: dict[str, Any],
    start_s: float,
) -> None:
    ecg_t = seg["ecg_t"]
    ecg = seg["ecg"]
    peak_t = seg["peak_t"]
    rr_t = seg["rr_t"]
    rr = seg["rr_ms"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), sharex=True)

    if ecg_t.size:
        t_rel = ecg_t - start_s
        ax1.plot(t_rel, ecg, linewidth=0.9, color="#1f4ea3")
    if peak_t.size and ecg_t.size:
        peak_rel = peak_t - start_s
        p_idx = np.searchsorted(ecg_t, peak_t, side="left")
        p_idx = np.clip(p_idx, 0, max(0, ecg.size - 1))
        if ecg.size:
            ax1.scatter(peak_rel, ecg[p_idx], s=12, color="#ff6b6b", alpha=0.85)

    h0, h1, _ = _highlight_window(suspicion_type, seg)
    if h0 is not None and h1 is not None:
        ax1.axvspan(h0 - start_s, h1 - start_s, color="#ffcc66", alpha=0.25)

    ax1.set_ylabel("ECG (uV)")
    ax1.set_title(f"segment#{segment_id} | {suspicion_type} | interpretation={interpretation_strength}")
    ax1.grid(alpha=0.25)

    if rr_t.size:
        rr_rel = rr_t - start_s
        ax2.plot(rr_rel, rr, color="#2a8f5b", linewidth=0.9)
        med = float(np.median(rr))
        ax2.axhline(med, color="#555", linestyle="--", linewidth=0.8)
    if h0 is not None and h1 is not None:
        ax2.axvspan(h0 - start_s, h1 - start_s, color="#ffcc66", alpha=0.25)
    ax2.set_xlabel("Seconds from segment start")
    ax2.set_ylabel("RR (ms)")
    ax2.grid(alpha=0.25)

    ax1.text(
        0.01,
        0.98,
        f"key clue: {key_clue}\n{annotation_note}",
        transform=ax1.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "#cccccc"},
    )

    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _plot_suspicion_zoom(
    out_png: Path,
    segment_id: int,
    suspicion_type: str,
    interpretation_strength: str,
    annotation_note: str,
    seg: dict[str, Any],
    start_s: float,
    end_s: float,
    zoom_seconds: float = 8.0,
) -> None:
    ecg_t = seg["ecg_t"]
    ecg = seg["ecg"]
    peak_t = seg["peak_t"]
    rr_t = seg["rr_t"]
    rr = seg["rr_ms"]

    if ecg_t.size == 0 or ecg.size == 0:
        raise ValueError("No ECG data for zoom")

    h0, h1, clue = _highlight_window(suspicion_type, seg)
    if h0 is None or h1 is None:
        center = (start_s + end_s) * 0.5
    else:
        center = 0.5 * (h0 + h1)

    win = max(5.0, float(zoom_seconds))
    z0 = max(float(start_s), center - win / 2.0)
    z1 = min(float(end_s), center + win / 2.0)
    if z1 - z0 < 2.0:
        z0 = float(start_s)
        z1 = float(end_s)

    mask = (ecg_t >= z0) & (ecg_t <= z1)
    zt = ecg_t[mask]
    zy = ecg[mask]
    if zt.size == 0:
        raise ValueError("Empty zoom slice")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 5.6), sharex=True)
    t_rel = zt - z0
    ax1.plot(t_rel, zy, linewidth=1.0, color="#1f4ea3")

    if peak_t.size:
        pmask = (peak_t >= z0) & (peak_t <= z1)
        pz = peak_t[pmask]
        if pz.size:
            pidx = np.searchsorted(zt, pz, side="left")
            pidx = np.clip(pidx, 0, max(0, zy.size - 1))
            ax1.scatter(pz - z0, zy[pidx], s=16, color="#ff6b6b", alpha=0.9)

    if h0 is not None and h1 is not None:
        hx0 = max(z0, h0)
        hx1 = min(z1, h1)
        if hx1 > hx0:
            ax1.axvspan(hx0 - z0, hx1 - z0, color="#ffcc66", alpha=0.28)
            ax2.axvspan(hx0 - z0, hx1 - z0, color="#ffcc66", alpha=0.28)

    rr_mask = (rr_t >= z0) & (rr_t <= z1) if rr_t.size else np.array([], dtype=bool)
    zrr_t = rr_t[rr_mask] if rr_t.size else np.array([], dtype=float)
    zrr = rr[rr_mask] if rr_t.size else np.array([], dtype=float)
    if zrr_t.size:
        ax2.plot(zrr_t - z0, zrr, color="#2a8f5b", linewidth=1.0)
        ax2.axhline(float(np.median(zrr)), color="#555", linestyle="--", linewidth=0.8)

    ax1.set_title(
        f"segment#{segment_id} | {suspicion_type} | zoom ({z1 - z0:.1f}s) | interpretation={interpretation_strength}"
    )
    ax1.set_ylabel("ECG (uV)")
    ax1.grid(alpha=0.25)
    ax2.set_xlabel("Seconds in zoom window")
    ax2.set_ylabel("RR (ms)")
    ax2.grid(alpha=0.25)
    ax1.text(
        0.01,
        0.97,
        f"focus clue: {clue}\n{annotation_note}",
        transform=ax1.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "#cccccc"},
    )
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _to_jsonable(row.get(k)) for k in columns})


def _write_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Diagnostic Suspicion Review",
        "",
        "This section contains non-diagnostic rhythm-pattern suspicions for manual review.",
        "These are screening interpretations, not medical diagnoses.",
        "`confidence_level` describes trust in segment analysis / signal usability.",
        "`interpretation_strength` describes support strength for the selected suspicion interpretation.",
        "",
    ]
    if not rows:
        lines.append("No suspicion entries generated.")
    else:
        for row in rows:
            lines.extend(
                [
                    f"## Segment {row['segment_id']}",
                    f"- Suspicion type: `{row['suspicion_type']}`",
                    f"- Segment confidence: `{row['confidence_level']}`",
                    f"- Interpretation strength: `{row['interpretation_strength']}`",
                    f"- Event count supporting pattern: `{row.get('event_count_supporting_pattern', 0)}`",
                    f"- Is isolated event: `{row.get('is_isolated_event', False)}`",
                    f"- Morphology support level: `{row.get('morphology_support_level', 'not_applicable')}`",
                    f"- Artifact favored: `{row.get('artifact_favored', False)}`",
                    f"- Artifact clues: {', '.join(row.get('artifact_clues', [])) if row.get('artifact_clues') else 'none'}",
                    f"- Time (UTC): `{row['segment_start_utc']}` -> `{row['segment_end_utc']}`",
                    f"- Why flagged: {row['primary_reason']}",
                    f"- Confidence vs interpretation note: {row['confidence_vs_interpretation_note']}",
                    f"- Rhythm interpretation limited: `{row['rhythm_interpretation_limited']}`",
                    f"- Artifact explanation favored: `{row['artifact_explanation_favored']}`",
                    f"- Manual checklist: {' | '.join(row['manual_checklist'])}",
                    f"- Possible false explanations: {', '.join(row['possible_false_explanations'])}",
                    f"- PNG: `{row['png_path']}`",
                    f"- Zoom PNG: `{row['png_zoom_path']}`",
                    "",
                ]
            )
            if row["suspicion_type"] == "artifact_suspicion":
                lines.extend(
                    [
                        "- Rhythm interpretation limited: artifact-oriented explanation is prioritized for this segment.",
                        "- Premature-like RR cues may exist, but artifact explanation is favored where quality/agreement clues dominate.",
                        "",
                    ]
                )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_diagnostic_suspicions(
    output_dir: Path,
    segment_rows: list[dict[str, Any]],
    ecg_t_s: np.ndarray,
    ecg_display_uv: np.ndarray,
    r_peak_indices: np.ndarray,
    rr_t_s: np.ndarray,
    rr_ms: np.ndarray,
) -> dict[str, Any]:
    out_png_dir = output_dir / "diagnostic_suspicions_png"
    out_png_dir.mkdir(parents=True, exist_ok=True)

    suspicion_rows: list[dict[str, Any]] = []

    for idx, row in enumerate(segment_rows):
        screening_class = str(row.get("screening_class", "none"))
        segment_status = str(row.get("segment_status", "unknown"))
        if screening_class not in SUSPICION_CLASSES:
            continue
        if segment_status != "ok":
            continue

        suspicion_type = _map_suspicion_type(screening_class)
        if suspicion_type is None:
            continue

        start_s = float(row["segment_start_s"])
        end_s = float(row["segment_end_s"])
        confidence_level = str(row.get("confidence_level", "low"))
        matched_ratio = _safe_float(row.get("agreement_matched_ratio"))
        rr_mae_ms = _safe_float(row.get("rr_abs_error_mae_ms"))
        ecg_quality = _safe_float(row.get("ecg_quality_score_final"))

        supporting_clues: list[str] = []
        if int(row.get("tachycardia_episode_count", 0)) > 0:
            supporting_clues.append("sustained_tachy_pattern")
        if int(row.get("bradycardia_episode_count", 0)) > 0:
            supporting_clues.append("sustained_brady_pattern")
        if int(row.get("irregular_rr_episode_count", 0)) > 0:
            supporting_clues.append("irregular_rr_episode")
        if int(row.get("pause_candidate_count", 0)) > 0:
            supporting_clues.append("prolonged_rr_gap")
        if int(row.get("ectopy_like_candidate_count", 0)) > 0:
            supporting_clues.append("short_then_compensatory_long_rr")
        if matched_ratio is not None and matched_ratio >= 0.75:
            supporting_clues.append("good_detector_agreement")
        elif matched_ratio is not None:
            supporting_clues.append("detector_agreement_limited")
        if ecg_quality is not None and ecg_quality >= 0.75:
            supporting_clues.append("quality_high")
        elif ecg_quality is not None and ecg_quality >= 0.55:
            supporting_clues.append("quality_medium")
        else:
            supporting_clues.append("quality_limited")

        interpretation_strength = _strength(
            suspicion_type=suspicion_type,
            confidence_level=confidence_level,
            ecg_quality_score_final=ecg_quality,
            matched_ratio=matched_ratio,
            rr_mae_ms=rr_mae_ms,
            supporting_count=len(supporting_clues),
        )

        primary_reason = _primary_reason(suspicion_type, row)
        checklist = _manual_checklist(suspicion_type)
        false_exp = _false_explanations(suspicion_type)

        agreement_summary = (
            f"matched_ratio={matched_ratio}, rr_mae_ms={rr_mae_ms}" if matched_ratio is not None else "agreement_unavailable"
        )

        seg = _segment_data(
            start_s=start_s,
            end_s=end_s,
            ecg_t_s=ecg_t_s,
            ecg_display_uv=ecg_display_uv,
            r_peak_indices=r_peak_indices,
            rr_t_s=rr_t_s,
            rr_ms=rr_ms,
        )

        event_count_supporting_pattern = 0
        is_isolated_event = False
        artifact_favored = suspicion_type == "artifact_suspicion"
        artifact_clues: list[str] = []
        morphology_support_level = "not_applicable"
        if suspicion_type == "premature_beat_like_suspicion":
            prem = _premature_refinement(
                row=row,
                seg=seg,
                matched_ratio=matched_ratio,
                rr_mae_ms=rr_mae_ms,
                ecg_quality=ecg_quality,
            )
            suspicion_type = str(prem["suspicion_type"])
            interpretation_strength = str(prem["interpretation_strength"])
            event_count_supporting_pattern = int(prem["event_count_supporting_pattern"])
            is_isolated_event = bool(prem["is_isolated_event"])
            artifact_favored = bool(prem["artifact_favored"])
            artifact_clues = list(prem["artifact_clues"])
            morphology_support_level = str(prem["morphology_support_level"])
            primary_reason = str(prem["primary_reason_override"])
            if is_isolated_event and "isolated_event" not in supporting_clues:
                supporting_clues.append("isolated_event")
            if artifact_favored and "artifact_favored_for_premature_pattern" not in supporting_clues:
                supporting_clues.append("artifact_favored_for_premature_pattern")
            if morphology_support_level != "not_applicable":
                supporting_clues.append(f"morphology_support_{morphology_support_level}")
            if suspicion_type == "artifact_suspicion":
                checklist = _manual_checklist("artifact_suspicion")
                false_exp = _false_explanations("artifact_suspicion")
        else:
            event_count_supporting_pattern = int(
                max(
                    float(row.get("ectopy_like_candidate_count", 0) or 0),
                    float(row.get("premature_beat_candidate_count", 0) or 0),
                )
            )

        confidence_note = _confidence_interpretation_note(suspicion_type, confidence_level, interpretation_strength)

        overlay_note_parts: list[str] = []
        if is_isolated_event:
            overlay_note_parts.append("isolated event")
        if artifact_favored:
            overlay_note_parts.append("artifact favored")
        overlay_note_parts.append(f"pattern evidence: {interpretation_strength}")
        overlay_note = " | ".join(overlay_note_parts)

        png_name = f"segment_{idx:03d}_{suspicion_type}.png"
        png_rel = (Path("diagnostic_suspicions_png") / png_name).as_posix()
        png_abs = str(out_png_dir / png_name)
        zoom_name = f"segment_{idx:03d}_{suspicion_type}_zoom.png"
        zoom_rel = (Path("diagnostic_suspicions_png") / zoom_name).as_posix()
        zoom_abs = str(out_png_dir / zoom_name)

        try:
            _plot_suspicion_overview(
                out_png=Path(png_abs),
                segment_id=idx,
                suspicion_type=suspicion_type,
                interpretation_strength=interpretation_strength,
                key_clue=supporting_clues[0] if supporting_clues else "pattern_flag",
                annotation_note=overlay_note,
                seg=seg,
                start_s=start_s,
            )
            _plot_suspicion_zoom(
                out_png=Path(zoom_abs),
                segment_id=idx,
                suspicion_type=suspicion_type,
                interpretation_strength=interpretation_strength,
                annotation_note=overlay_note,
                seg=seg,
                start_s=start_s,
                end_s=end_s,
                zoom_seconds=8.0,
            )
            png_path = png_rel
            png_zoom_path = zoom_rel
        except Exception:
            png_path = ""
            png_zoom_path = ""
            png_abs = ""
            zoom_abs = ""

        suspicion_rows.append(
            {
                "segment_id": idx,
                "segment_start_s": start_s,
                "segment_end_s": end_s,
                "segment_start_utc": row.get("segment_start_utc"),
                "segment_end_utc": row.get("segment_end_utc"),
                "screening_class": screening_class,
                "confidence_level": confidence_level,
                "ecg_quality_score_final": ecg_quality,
                "suspicion_type": suspicion_type,
                "interpretation_strength": interpretation_strength,
                "suspicion_strength": interpretation_strength,
                "confidence_vs_interpretation_note": confidence_note,
                "primary_reason": primary_reason,
                "supporting_clues": supporting_clues,
                "manual_checklist": checklist,
                "possible_false_explanations": false_exp,
                "detector_agreement_summary": agreement_summary,
                "rhythm_interpretation_limited": suspicion_type == "artifact_suspicion",
                "artifact_explanation_favored": suspicion_type == "artifact_suspicion" or artifact_favored,
                "event_count_supporting_pattern": event_count_supporting_pattern,
                "is_isolated_event": is_isolated_event,
                "artifact_favored": artifact_favored,
                "artifact_clues": artifact_clues,
                "morphology_support_level": morphology_support_level,
                "recommended_review": True,
                "png_path": png_path,
                "png_zoom_path": png_zoom_path,
                "png_abspath": png_abs,
                "png_zoom_abspath": zoom_abs,
            }
        )

    csv_rows: list[dict[str, Any]] = []
    for row in suspicion_rows:
        csv_rows.append(
            {
                **row,
                "supporting_clues": json.dumps(row["supporting_clues"], ensure_ascii=False),
                "manual_checklist": json.dumps(row["manual_checklist"], ensure_ascii=False),
                "possible_false_explanations": json.dumps(row["possible_false_explanations"], ensure_ascii=False),
                "artifact_clues": json.dumps(row["artifact_clues"], ensure_ascii=False),
            }
        )

    csv_columns = [
        "segment_id",
        "segment_start_s",
        "segment_end_s",
        "segment_start_utc",
        "segment_end_utc",
        "screening_class",
        "confidence_level",
        "ecg_quality_score_final",
        "suspicion_type",
        "interpretation_strength",
        "confidence_vs_interpretation_note",
        "primary_reason",
        "supporting_clues",
        "manual_checklist",
        "possible_false_explanations",
        "detector_agreement_summary",
        "rhythm_interpretation_limited",
        "artifact_explanation_favored",
        "event_count_supporting_pattern",
        "is_isolated_event",
        "artifact_favored",
        "artifact_clues",
        "morphology_support_level",
        "recommended_review",
        "png_path",
        "png_zoom_path",
        "png_abspath",
        "png_zoom_abspath",
    ]

    _write_csv(output_dir / "diagnostic_suspicions.csv", csv_rows, csv_columns)
    _write_markdown(output_dir / "diagnostic_suspicions.md", suspicion_rows)

    with (output_dir / "diagnostic_suspicions.json").open("w", encoding="utf-8") as f:
        json.dump(_to_jsonable(suspicion_rows), f, indent=2, ensure_ascii=False)

    counts_by_type: dict[str, int] = {}
    counts_by_strength: dict[str, int] = {}
    for row in suspicion_rows:
        counts_by_type[row["suspicion_type"]] = counts_by_type.get(row["suspicion_type"], 0) + 1
        counts_by_strength[row["interpretation_strength"]] = counts_by_strength.get(row["interpretation_strength"], 0) + 1

    return {
        "count": int(len(suspicion_rows)),
        "counts_by_type": counts_by_type,
        "counts_by_interpretation_strength": counts_by_strength,
        "counts_by_strength": counts_by_strength,
        "semantics_note": {
            "confidence_level": "trust in segment analysis/signal usability",
            "interpretation_strength": "support strength for selected suspicion interpretation",
        },
        "png_dir": str(out_png_dir),
        "files": {
            "csv": str(output_dir / "diagnostic_suspicions.csv"),
            "md": str(output_dir / "diagnostic_suspicions.md"),
            "json": str(output_dir / "diagnostic_suspicions.json"),
        },
    }
