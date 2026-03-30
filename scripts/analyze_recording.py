from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from ecg_metrics import analyze_ecg, segment_quality_breakdown
from hrv_metrics import analyze_device_rr
from io_utils import (
    load_ecg_csv,
    load_hr_csv,
    load_rr_csv,
    overlap_window,
    subset_by_time_window,
    unix_seconds_to_utc_iso,
)
from diagnostic_suspicions import generate_diagnostic_suspicions
from plotting import write_all_plots
from report_writer import (
    write_hrv_5min_windows_csv,
    write_metrics_json,
    write_segment_metrics_csv,
    write_summary_markdown,
    write_suspicious_segments_csv,
)
from rhythm_screening import aggregate_screening, screen_segment

RELIABILITY_MAX_MATCH_TIME_DELTA_S = 0.65
# Tuned conservatively for wearable single-lead Polar H10 recordings in non-clinical settings.
RELIABILITY_MIN_MATCHED_RATIO = 0.70
RELIABILITY_MAX_MAE_MS = 90.0
RELIABILITY_MAX_P95_MS = 200.0
LOW_CONFIDENCE_MATCHED_RATIO = 0.55
LOW_CONFIDENCE_MAE_MS = 120.0


def _cross_signal_consistency(
    rr_t_ecg_s: np.ndarray,
    rr_ecg_ms: np.ndarray,
    rr_t_device_s: np.ndarray,
    rr_device_ms: np.ndarray,
    max_delta_s: float = RELIABILITY_MAX_MATCH_TIME_DELTA_S,
) -> dict[str, Any]:
    if rr_ecg_ms.size == 0 or rr_device_ms.size == 0:
        return {
            "matched_count": 0,
            "matched_ratio": 0.0,
            "rr_abs_error_mean_ms": None,
            "rr_abs_error_median_ms": None,
            "rr_abs_error_p95_ms": None,
            "reliability_gate": "fail",
        }

    errors: list[float] = []
    for t_ecg, rr_val in zip(rr_t_ecg_s, rr_ecg_ms):
        pos = int(np.searchsorted(rr_t_device_s, t_ecg))
        candidates: list[int] = []
        if pos < rr_t_device_s.size:
            candidates.append(pos)
        if pos > 0:
            candidates.append(pos - 1)
        if not candidates:
            continue
        idx = min(candidates, key=lambda i: abs(rr_t_device_s[i] - t_ecg))
        if abs(rr_t_device_s[idx] - t_ecg) <= max_delta_s:
            errors.append(float(abs(rr_device_ms[idx] - rr_val)))

    if not errors:
        return {
            "matched_count": 0,
            "matched_ratio": 0.0,
            "rr_abs_error_mean_ms": None,
            "rr_abs_error_median_ms": None,
            "rr_abs_error_p95_ms": None,
            "reliability_gate": "fail",
        }

    err = np.asarray(errors, dtype=np.float64)
    ratio = float(err.size / max(1, rr_ecg_ms.size))
    mean = float(np.mean(err))
    median = float(np.median(err))
    p95 = float(np.percentile(err, 95))

    gate = "pass" if (
        ratio >= RELIABILITY_MIN_MATCHED_RATIO and mean <= RELIABILITY_MAX_MAE_MS and p95 <= RELIABILITY_MAX_P95_MS
    ) else "fail"

    return {
        "matched_count": int(err.size),
        "matched_ratio": ratio,
        "rr_abs_error_mean_ms": mean,
        "rr_abs_error_median_ms": median,
        "rr_abs_error_p95_ms": p95,
        "reliability_gate": gate,
    }


def _segment_edges(start_s: float, end_s: float, segment_minutes: int) -> list[tuple[float, float]]:
    segment_s = max(60, int(segment_minutes * 60))
    edges: list[tuple[float, float]] = []
    cursor = start_s
    while cursor < end_s:
        stop = min(end_s, cursor + segment_s)
        edges.append((cursor, stop))
        cursor = stop
    return edges


def _segment_consistency(
    rr_t_ecg_s: np.ndarray,
    rr_ecg_ms: np.ndarray,
    rr_t_device_s: np.ndarray,
    rr_device_ms: np.ndarray,
) -> tuple[float, float | None, float]:
    cross = _cross_signal_consistency(rr_t_ecg_s, rr_ecg_ms, rr_t_device_s, rr_device_ms)
    matched_ratio = float(cross["matched_ratio"])
    mae = cross["rr_abs_error_mean_ms"]
    missing_peak_burden = float(1.0 - matched_ratio)
    return matched_ratio, mae, missing_peak_burden


def _build_segment_rows(
    overlap_start_s: float,
    overlap_end_s: float,
    segment_minutes: int,
    ecg_timestamps_s: np.ndarray,
    ecg_display_signal_uv: np.ndarray,
    ecg_r_peak_indices: np.ndarray,
    rr_t_device_s: np.ndarray,
    rr_device_ms: np.ndarray,
    rr_t_ecg_s: np.ndarray,
    rr_ecg_ms: np.ndarray,
    rr_artifact_burden_global: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seg_start, seg_end in _segment_edges(overlap_start_s, overlap_end_s, segment_minutes):
        rr_mask = (rr_t_device_s >= seg_start) & (rr_t_device_s < seg_end)
        ecg_rr_mask = (rr_t_ecg_s >= seg_start) & (rr_t_ecg_s < seg_end)

        rr_seg_t = rr_t_device_s[rr_mask]
        rr_seg = rr_device_ms[rr_mask]
        rr_ecg_seg_t = rr_t_ecg_s[ecg_rr_mask]
        rr_ecg_seg = rr_ecg_ms[ecg_rr_mask]

        if rr_seg.size == 0:
            rows.append(
                {
                    "segment_start_s": float(seg_start),
                    "segment_end_s": float(seg_end),
                    "segment_start_utc": unix_seconds_to_utc_iso(seg_start),
                    "segment_end_utc": unix_seconds_to_utc_iso(seg_end),
                    "segment_status": "coverage_gap",
                    "screening_class": "coverage_gap",
                    "review_recommended": False,
                    "review_reason": "no_rr_coverage",
                    "confidence_level": "low",
                    "beat_count": 0,
                    "agreement_matched_ratio": 0.0,
                    "artifact_burden": 1.0,
                    "missing_peak_burden": 1.0,
                    "rr_abs_error_mae_ms": None,
                    "baseline_noise_score": None,
                    "hf_noise_score": None,
                    "peak_consistency_score": None,
                    "template_similarity_score": None,
                    "agreement_score": None,
                    "ecg_quality_score_final": None,
                    "quality_not_applicable": True,
                }
            )
            continue

        matched_ratio, rr_mae_ms, missing_peak_burden = _segment_consistency(
            rr_ecg_seg_t,
            rr_ecg_seg,
            rr_seg_t,
            rr_seg,
        )

        local_artifact_burden = float(np.mean(rr_seg > 2200.0)) if rr_seg.size else 1.0
        local_artifact_burden = max(local_artifact_burden, rr_artifact_burden_global)

        quality_scores = segment_quality_breakdown(
            ecg_timestamps_s=ecg_timestamps_s,
            display_signal_uv=ecg_display_signal_uv,
            r_peak_indices=ecg_r_peak_indices,
            segment_start_s=seg_start,
            segment_end_s=seg_end,
            matched_ratio=matched_ratio,
            rr_mae_ms=rr_mae_ms,
            missing_peak_burden=missing_peak_burden,
        )

        screening = screen_segment(
            rr_ms=rr_seg,
            rr_timestamps_s=rr_seg_t,
            quality_scores=quality_scores,
            matched_ratio=matched_ratio,
            artifact_burden=local_artifact_burden,
            missing_peak_burden=missing_peak_burden,
            rr_mae_ms=rr_mae_ms,
            low_confidence_matched_ratio=LOW_CONFIDENCE_MATCHED_RATIO,
            low_confidence_mae_ms=LOW_CONFIDENCE_MAE_MS,
        )

        row = {
            "segment_start_s": float(seg_start),
            "segment_end_s": float(seg_end),
            "segment_start_utc": unix_seconds_to_utc_iso(seg_start),
            "segment_end_utc": unix_seconds_to_utc_iso(seg_end),
            "segment_status": "ok",
            "beat_count": int(rr_seg.size),
            "hr_mean_bpm": float(np.mean(60000.0 / rr_seg)),
            "hr_min_bpm": float(np.min(60000.0 / rr_seg)),
            "hr_max_bpm": float(np.max(60000.0 / rr_seg)),
            "rr_median_ms": float(np.median(rr_seg)),
            "rr_iqr_ms": float(np.subtract(*np.percentile(rr_seg, [75, 25]))),
            "quality_not_applicable": False,
            **screening,
        }
        rows.append(row)

    return rows


def _compute_quality_aggregate(segment_rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid_scores: list[float] = []
    excluded_reasons: list[str] = []
    for row in segment_rows:
        status = str(row.get("segment_status", "unknown"))
        quality_not_applicable = bool(row.get("quality_not_applicable", False))
        score = row.get("ecg_quality_score_final")

        if status != "ok":
            excluded_reasons.append(f"{status}")
            continue
        if quality_not_applicable:
            excluded_reasons.append("quality_not_applicable")
            continue
        if score is None:
            excluded_reasons.append("quality_missing")
            continue
        if not np.isfinite(float(score)):
            excluded_reasons.append("quality_non_finite")
            continue
        valid_scores.append(float(score))

    reason_counts: dict[str, int] = {}
    for reason in excluded_reasons:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    if valid_scores:
        arr = np.asarray(valid_scores, dtype=np.float64)
        return {
            "ecg_quality_segment_count_valid": int(arr.size),
            "ecg_quality_segment_count_excluded": int(len(excluded_reasons)),
            "ecg_quality_score_median": float(np.median(arr)),
            "ecg_quality_score_p10": float(np.percentile(arr, 10)),
            "ecg_quality_score_p90": float(np.percentile(arr, 90)),
            "excluded_quality_segments_reasons": reason_counts,
        }

    return {
        "ecg_quality_segment_count_valid": 0,
        "ecg_quality_segment_count_excluded": int(len(excluded_reasons)),
        "ecg_quality_score_median": None,
        "ecg_quality_score_p10": None,
        "ecg_quality_score_p90": None,
        "excluded_quality_segments_reasons": reason_counts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polar H10 single-lead ECG/RR screening analysis (non-diagnostic)")
    parser.add_argument("--ecg", type=Path, required=True, help="Path to ECG CSV (e.g. RawECG_recording.csv)")
    parser.add_argument("--rr", type=Path, required=True, help="Path to RR CSV (e.g. RRinterval_recording.csv)")
    parser.add_argument("--hr", type=Path, default=None, help="Optional path to HeartRate CSV")
    parser.add_argument("--out", type=Path, required=True, help="Output directory")
    parser.add_argument("--segment-minutes", type=int, default=5, help="Segment length in minutes")
    parser.add_argument("--analysis-mode", choices=["comprehensive"], default="comprehensive")
    parser.add_argument(
        "--with-diagnostic-suspicions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Generate explainable diagnostic-style suspicion layer (non-diagnostic).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ecg = load_ecg_csv(args.ecg.resolve())
    rr = load_rr_csv(args.rr.resolve())
    hr = load_hr_csv(args.hr.resolve()) if args.hr else None

    overlap_start_s, overlap_end_s = overlap_window(ecg.timestamps_s, rr.timestamps_s)

    ecg_t, ecg_uv = subset_by_time_window(ecg.timestamps_s, ecg.ecg_uv, overlap_start_s, overlap_end_s)
    rr_t, rr_ms = subset_by_time_window(rr.timestamps_s, rr.rr_ms, overlap_start_s, overlap_end_s)
    rr_hr_t, rr_hr = subset_by_time_window(rr.timestamps_s, rr.heart_rate_bpm, overlap_start_s, overlap_end_s)

    if rr_hr_t.size != rr_t.size:
        rr_hr = 60000.0 / rr_ms

    if hr is not None and hr.timestamps_s.size:
        hr_t, hr_values = subset_by_time_window(hr.timestamps_s, hr.heart_rate_bpm, overlap_start_s, overlap_end_s)
        if hr_values.size >= rr_hr.size and hr_values.size > 0:
            rr_hr = np.interp(rr_t, hr_t, hr_values)

    expected_hr_bpm = float(np.nanmedian(rr_hr)) if rr_hr.size else None

    ecg_result = analyze_ecg(ecg_t, ecg_uv, expected_hr_bpm=expected_hr_bpm)
    rr_result = analyze_device_rr(rr_t, rr_ms, rr_hr)

    cross = _cross_signal_consistency(
        ecg_result.rr_timestamps_s,
        ecg_result.rr_ms,
        rr_result.rr_timestamps_s,
        rr_result.rr_ms,
    )

    segment_rows = _build_segment_rows(
        overlap_start_s=overlap_start_s,
        overlap_end_s=overlap_end_s,
        segment_minutes=max(1, int(args.segment_minutes)),
        ecg_timestamps_s=ecg_t,
        ecg_display_signal_uv=ecg_result.display_signal_uv,
        ecg_r_peak_indices=ecg_result.r_peak_indices,
        rr_t_device_s=rr_result.rr_timestamps_s,
        rr_device_ms=rr_result.rr_ms,
        rr_t_ecg_s=ecg_result.rr_timestamps_s,
        rr_ecg_ms=ecg_result.rr_ms,
        rr_artifact_burden_global=float(rr_result.metrics.get("artifact_burden", 1.0)),
    )

    # Canonical aggregation source of truth for all downstream counts and summaries.
    screening_agg = aggregate_screening(segment_rows)
    quality_agg = _compute_quality_aggregate(segment_rows)
    ecg_result.metrics["ecg_quality_score"] = quality_agg["ecg_quality_score_median"]
    ecg_result.metrics["ecg_quality_score_p10"] = quality_agg["ecg_quality_score_p10"]
    ecg_result.metrics["ecg_quality_score_p90"] = quality_agg["ecg_quality_score_p90"]

    adjusted_mode = "normal_screening_usage" if cross["reliability_gate"] == "pass" else "suppressed_flags_due_to_low_agreement"

    all_metrics: dict[str, Any] = {
        "metadata": {
            "analysis_mode": args.analysis_mode,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "ecg_path": str(args.ecg.resolve()),
            "rr_path": str(args.rr.resolve()),
            "hr_path": str(args.hr.resolve()) if args.hr else None,
            "ecg_timestamp_column": ecg.timestamp_column,
            "ecg_value_column": ecg.value_column,
            "rr_timestamp_column": rr.timestamp_column,
            "rr_value_column": rr.rr_column,
            "rr_hr_column": rr.hr_column,
            "overlap_start_s": float(overlap_start_s),
            "overlap_end_s": float(overlap_end_s),
            "overlap_start_utc": unix_seconds_to_utc_iso(overlap_start_s),
            "overlap_end_utc": unix_seconds_to_utc_iso(overlap_end_s),
            "overlap_duration_s": float(overlap_end_s - overlap_start_s),
            "segment_minutes": int(args.segment_minutes),
            "disclaimer": "screening_only_not_for_diagnosis",
            "limitations": [
                "single_lead_ecg",
                "wearable_chest_strap_context",
                "approx_130hz_sampling_rate",
                "non_clinical_environment",
            ],
        },
        "ecg_overlap": {"metrics": ecg_result.metrics},
        "rr_overlap": {"metrics": rr_result.metrics},
        "cross_signal_consistency": cross,
        "segment_confidence": {
            "high": screening_agg["high_confidence_segments"],
            "medium": screening_agg["medium_confidence_segments"],
            "low": screening_agg["low_confidence_segments"],
        },
        "screening": {
            **screening_agg,
            "adjusted_screening_mode": adjusted_mode,
            "thresholds": {
                "RELIABILITY_MIN_MATCHED_RATIO": RELIABILITY_MIN_MATCHED_RATIO,
                "RELIABILITY_MAX_MAE_MS": RELIABILITY_MAX_MAE_MS,
                "RELIABILITY_MAX_P95_MS": RELIABILITY_MAX_P95_MS,
                "LOW_CONFIDENCE_MATCHED_RATIO": LOW_CONFIDENCE_MATCHED_RATIO,
                "LOW_CONFIDENCE_MAE_MS": LOW_CONFIDENCE_MAE_MS,
            },
        },
        "quality_aggregate": quality_agg,
    }

    suspicion_summary = {
        "count": 0,
        "counts_by_type": {},
        "counts_by_strength": {},
        "png_dir": None,
        "files": {},
    }
    if args.with_diagnostic_suspicions:
        suspicion_summary = generate_diagnostic_suspicions(
            output_dir=out_dir,
            segment_rows=segment_rows,
            ecg_t_s=ecg_t,
            ecg_display_uv=ecg_result.display_signal_uv,
            r_peak_indices=ecg_result.r_peak_indices,
            rr_t_s=rr_result.rr_timestamps_s,
            rr_ms=rr_result.rr_ms,
        )
    all_metrics["diagnostic_suspicions"] = suspicion_summary

    plots = write_all_plots(
        output_dir=out_dir,
        rr_timestamps_s=rr_result.rr_timestamps_s,
        rr_ms_device=rr_result.rr_ms,
        rr_timestamps_ecg_s=ecg_result.rr_timestamps_s,
        rr_ms_ecg=ecg_result.rr_ms,
        psd_freqs_hz=rr_result.psd_freqs_hz,
        psd_power=rr_result.psd_power,
        ecg_timestamps_s=ecg_t,
        ecg_display_uv=ecg_result.display_signal_uv,
        r_peak_indices=ecg_result.r_peak_indices,
        segment_rows=segment_rows,
    )

    metrics_json = write_metrics_json(out_dir, all_metrics)
    segment_csv = write_segment_metrics_csv(out_dir, segment_rows)
    suspicious_csv = write_suspicious_segments_csv(out_dir, segment_rows)
    hrv_csv = write_hrv_5min_windows_csv(out_dir, rr_result.hrv_5min_rows)
    summary_md = write_summary_markdown(out_dir, all_metrics, segment_rows, plots)

    print("Analysis completed.")
    print(f"Output directory: {out_dir}")
    print(f"- {metrics_json.name}")
    print(f"- {segment_csv.name}")
    print(f"- {suspicious_csv.name}")
    print(f"- {hrv_csv.name}")
    print(f"- {summary_md.name}")
    if args.with_diagnostic_suspicions:
        print("- diagnostic_suspicions.csv")
        print("- diagnostic_suspicions.md")
        print("- diagnostic_suspicions.json")
    for name in sorted(plots):
        print(f"- {name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
