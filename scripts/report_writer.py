from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonable(x) for x in obj]
    if isinstance(obj, tuple):
        return [_jsonable(x) for x in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def write_metrics_json(output_dir: Path, metrics: dict[str, Any]) -> Path:
    path = output_dir / "metrics.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(_jsonable(metrics), f, indent=2, ensure_ascii=False)
    return path


def _write_csv(path: Path, rows: list[dict[str, Any]], default_columns: list[str]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as f:
        if rows:
            fieldnames = list(rows[0].keys())
            seen = set(fieldnames)
            for row in rows[1:]:
                for key in row.keys():
                    if key not in seen:
                        fieldnames.append(key)
                        seen.add(key)
        else:
            fieldnames = default_columns
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_jsonable(row))
    return path


def write_segment_metrics_csv(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    return _write_csv(
        output_dir / "segment_metrics.csv",
        rows,
        [
            "segment_start_s",
            "segment_end_s",
            "segment_start_utc",
            "segment_end_utc",
            "segment_status",
            "confidence_level",
            "screening_class",
            "review_recommended",
            "review_reason",
            "baseline_noise_score",
            "hf_noise_score",
            "peak_consistency_score",
            "template_similarity_score",
            "agreement_score",
            "ecg_quality_score_final",
            "agreement_matched_ratio",
            "artifact_burden",
            "missing_peak_burden",
        ],
    )


def write_suspicious_segments_csv(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    suspicious = [
        r
        for r in rows
        if r.get("screening_class") in {
            "tachy_candidate",
            "brady_candidate",
            "irregular_rr_candidate",
            "af_like_candidate",
            "ectopy_like_candidate",
            "pause_candidate",
            "artifact_candidate",
        }
    ]
    return _write_csv(
        output_dir / "suspicious_segments.csv",
        suspicious,
        [
            "segment_start_s",
            "segment_end_s",
            "segment_start_utc",
            "segment_end_utc",
            "segment_status",
            "confidence_level",
            "screening_class",
            "review_recommended",
            "review_reason",
            "agreement_matched_ratio",
            "rr_abs_error_mae_ms",
            "ecg_quality_score_final",
        ],
    )


def write_hrv_5min_windows_csv(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    return _write_csv(
        output_dir / "hrv_5min_windows.csv",
        rows,
        [
            "window_start_s",
            "window_end_s",
            "beat_count",
            "hr_mean_bpm",
            "mean_nn_ms",
            "median_nn_ms",
            "sdnn_ms",
            "rmssd_ms",
            "pnn50_pct",
            "lf_power_ms2",
            "hf_power_ms2",
            "lf_hf_ratio",
            "stationary_flag",
            "exclude_reason",
        ],
    )


def write_summary_markdown(
    output_dir: Path,
    all_metrics: dict[str, Any],
    segment_rows: list[dict[str, Any]],
    plot_paths: dict[str, str],
) -> Path:
    meta = all_metrics["metadata"]
    rr = all_metrics["rr_overlap"]["metrics"]
    cross = all_metrics["cross_signal_consistency"]
    screening = all_metrics["screening"]
    seg_conf = all_metrics.get("segment_confidence", {})
    quality_agg = all_metrics.get("quality_aggregate", {})
    susp = all_metrics.get("diagnostic_suspicions", {})
    coverage_rows = [r for r in segment_rows if r.get("screening_class") == "coverage_gap"]
    coverage_locations = [
        f"{r.get('segment_start_utc')} -> {r.get('segment_end_utc')}" for r in coverage_rows[:5]
    ]
    coverage_note = "none" if not coverage_locations else "; ".join(coverage_locations)
    coverage_impact = (
        "Coverage gaps reduce recording completeness and can lower confidence for affected time ranges."
        if coverage_rows
        else "No meaningful coverage gaps detected."
    )

    lines = [
        "# Polar H10 ECG/RR Screening Summary",
        "",
        "## Disclaimer",
        "This output is screening and engineering analysis only, not medical diagnosis.",
        "Single-lead wearable ECG (~130 Hz) has substantial limitations for rhythm interpretation.",
        "Any candidate flags are non-diagnostic and require manual review in context.",
        "",
        "## Overlap",
        f"- Overlap start (UTC): `{meta['overlap_start_utc']}`",
        f"- Overlap end (UTC): `{meta['overlap_end_utc']}`",
        f"- Overlap duration: `{meta['overlap_duration_s']:.1f} s`",
        "",
        "## Key Findings",
        f"- Device HR mean/min/max: `{rr.get('hr_bpm_mean')}` / `{rr.get('hr_bpm_min')}` / `{rr.get('hr_bpm_max')}` bpm",
        f"- HRV meanNN/SDNN/RMSSD/pNN50: `{rr.get('mean_nn_ms')}` / `{rr.get('sdnn_ms')}` / `{rr.get('rmssd_ms')}` / `{rr.get('pnn50_pct')}`",
        f"- HRV LF/HF: `{rr.get('lf_power_ms2')}` / `{rr.get('hf_power_ms2')}` / `{rr.get('lf_hf_ratio')}`",
        "",
        "## Cross-Signal Consistency",
        f"- Matched beats: `{cross.get('matched_count')}`",
        f"- Matched ratio: `{cross.get('matched_ratio')}`",
        f"- RR absolute error mean/median/p95: `{cross.get('rr_abs_error_mean_ms')}` / `{cross.get('rr_abs_error_median_ms')}` / `{cross.get('rr_abs_error_p95_ms')}` ms",
        f"- Reliability gate: `{cross.get('reliability_gate')}`",
        "",
        "## Screening Categories",
        f"- Rhythm candidate segments: `{screening.get('rhythm_candidate_count')}`",
        f"- Artifact/low-agreement segments: `{screening.get('artifact_or_low_agreement_count')}`",
        f"- Coverage-gap segments: `{screening.get('coverage_gap_count')}`",
        f"- Review recommended segments: `{screening.get('review_recommended_segments')}`",
        "",
        "## Data Coverage / Recording Completeness",
        f"- Coverage gap segments: `{screening.get('coverage_gap_count')}`",
        f"- Coverage gap examples (UTC): `{coverage_note}`",
        "- Coverage gaps are treated as data completeness issues, not rhythm findings.",
        f"- Reliability impact: `{coverage_impact}`",
        "",
        "## Confidence Triage",
        f"- High confidence segments: `{seg_conf.get('high')}`",
        f"- Medium confidence segments: `{seg_conf.get('medium')}`",
        f"- Low confidence segments: `{seg_conf.get('low')}`",
        "",
        "## ECG Quality Aggregation",
        f"- Valid quality segments: `{quality_agg.get('ecg_quality_segment_count_valid')}`",
        f"- Excluded quality segments: `{quality_agg.get('ecg_quality_segment_count_excluded')}`",
        f"- ECG quality median/p10/p90: `{quality_agg.get('ecg_quality_score_median')}` / `{quality_agg.get('ecg_quality_score_p10')}` / `{quality_agg.get('ecg_quality_score_p90')}`",
        f"- Excluded reasons: `{quality_agg.get('excluded_quality_segments_reasons')}`",
        "",
        "## Diagnostic Suspicion Review",
        f"- Suspicion entries: `{susp.get('count')}`",
        f"- Suspicion types: `{susp.get('counts_by_type')}`",
        f"- Interpretation strength counts: `{susp.get('counts_by_interpretation_strength', susp.get('counts_by_strength'))}`",
        f"- Semantics note: `{susp.get('semantics_note')}`",
        "- These are pattern suspicions for review, not diagnoses.",
        "",
        "## Interpretation",
        "Confidence reflects trust in signal-processing reliability, not diagnosis likelihood.",
        "Low agreement or low quality segments are triaged primarily as artifact/low-agreement.",
        "LF/HF interpretation on long non-stationary recordings is limited and should be treated cautiously.",
        "",
        "## Files",
        "- `metrics.json`",
        "- `segment_metrics.csv`",
        "- `suspicious_segments.csv`",
        "- `hrv_5min_windows.csv`",
        "- `hr_trend.png`, `tachogram_rr.png`, `poincare.png`, `hrv_psd.png`, `ecg_strips.png`",
        "",
        f"Generated at UTC: `{datetime.now(timezone.utc).isoformat()}`",
    ]

    if plot_paths:
        lines.append("")
        lines.append("Generated plot paths:")
        for _, path in sorted(plot_paths.items()):
            lines.append(f"- `{path}`")

    path = output_dir / "summary.md"
    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path
