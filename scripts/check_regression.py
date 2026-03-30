from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

RHYTHM_CLASSES = {
    "tachy_candidate",
    "brady_candidate",
    "irregular_rr_candidate",
    "af_like_candidate",
    "ectopy_like_candidate",
    "pause_candidate",
}


def _close(a: float | None, b: float | None, tol: float = 1e-6) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if not (math.isfinite(a) and math.isfinite(b)):
        return False
    return abs(a - b) <= tol


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lightweight regression checks for scripts2 analysis output")
    parser.add_argument("--out", type=Path, required=True, help="Analysis output directory")
    args = parser.parse_args()

    out = args.out.resolve()
    required = [
        out / "metrics.json",
        out / "summary.md",
        out / "segment_metrics.csv",
        out / "suspicious_segments.csv",
        out / "hrv_5min_windows.csv",
        out / "hr_trend.png",
        out / "tachogram_rr.png",
        out / "poincare.png",
        out / "hrv_psd.png",
        out / "ecg_strips.png",
    ]
    for path in required:
        _assert(path.exists(), f"Missing required output: {path}")

    metrics = _read_json(out / "metrics.json")
    summary_text = (out / "summary.md").read_text(encoding="utf-8")
    seg = pd.read_csv(out / "segment_metrics.csv")
    susp = pd.read_csv(out / "suspicious_segments.csv")

    screening = metrics["screening"]
    quality = metrics.get("quality_aggregate", {})

    seg_count = int(seg.shape[0])
    _assert(seg_count == int(screening["segment_count"]), "segment_count mismatch metrics vs segment_metrics.csv")

    class_counts = seg["screening_class"].value_counts().to_dict()
    rhythm_count = int(sum(class_counts.get(cls, 0) for cls in RHYTHM_CLASSES))
    artifact_count = int(class_counts.get("artifact_candidate", 0))
    coverage_count = int(class_counts.get("coverage_gap", 0))

    _assert(rhythm_count == int(screening["rhythm_candidate_count"]), "rhythm_candidate_count mismatch")
    _assert(
        artifact_count == int(screening["artifact_or_low_agreement_count"]),
        "artifact_or_low_agreement_count mismatch",
    )
    _assert(coverage_count == int(screening["coverage_gap_count"]), "coverage_gap_count mismatch")

    _assert("coverage_gap" not in set(susp.get("screening_class", [])), "coverage_gap leaked into suspicious_segments.csv")
    _assert(
        int(susp.shape[0]) == rhythm_count + artifact_count,
        "suspicious row count must equal rhythm_candidate_count + artifact_or_low_agreement_count",
    )

    seg_ok = seg[seg["segment_status"] == "ok"].copy()
    score = pd.to_numeric(seg_ok["ecg_quality_score_final"], errors="coerce")
    valid_mask = score.notna()
    valid_scores = score[valid_mask]
    quality_not_applicable_series = (
        seg_ok["quality_not_applicable"].astype(str).str.lower().isin({"true", "1", "yes"})
        if "quality_not_applicable" in seg_ok.columns
        else pd.Series([False] * seg_ok.shape[0], index=seg_ok.index)
    )

    excluded_reason_counts = {
        "coverage_gap": int((seg["segment_status"] != "ok").sum()),
        "quality_not_applicable": int(quality_not_applicable_series.sum()),
        "quality_missing": int((seg_ok["ecg_quality_score_final"].isna()).sum()),
    }
    excluded_reason_counts = {k: v for k, v in excluded_reason_counts.items() if v > 0}

    _assert(
        int(quality.get("ecg_quality_segment_count_valid", -1)) == int(valid_scores.shape[0]),
        "ecg_quality_segment_count_valid mismatch",
    )
    _assert(
        int(quality.get("ecg_quality_segment_count_excluded", -1)) == int(seg.shape[0] - valid_scores.shape[0]),
        "ecg_quality_segment_count_excluded mismatch",
    )

    if valid_scores.shape[0] > 0:
        med = float(valid_scores.median())
        p10 = float(valid_scores.quantile(0.10))
        p90 = float(valid_scores.quantile(0.90))
        _assert(_close(float(quality["ecg_quality_score_median"]), med, tol=1e-5), "quality median mismatch")
        _assert(_close(float(quality["ecg_quality_score_p10"]), p10, tol=1e-5), "quality p10 mismatch")
        _assert(_close(float(quality["ecg_quality_score_p90"]), p90, tol=1e-5), "quality p90 mismatch")

    metrics_excluded = quality.get("excluded_quality_segments_reasons", {})
    for reason, count in excluded_reason_counts.items():
        _assert(int(metrics_excluded.get(reason, 0)) == int(count), f"excluded reason mismatch: {reason}")

    # Summary consistency (presence of canonical counts)
    for value in [
        screening["rhythm_candidate_count"],
        screening["artifact_or_low_agreement_count"],
        screening["coverage_gap_count"],
        screening["review_recommended_segments"],
        metrics["segment_confidence"]["high"],
        metrics["segment_confidence"]["medium"],
        metrics["segment_confidence"]["low"],
    ]:
        _assert(str(value) in summary_text, f"Summary.md missing expected count value: {value}")

    print("Regression check: OK")
    print(f"Segments by class: {class_counts}")
    print(f"Suspicious rows: {int(susp.shape[0])}")
    print(
        "Quality stats: "
        f"valid={quality.get('ecg_quality_segment_count_valid')} "
        f"excluded={quality.get('ecg_quality_segment_count_excluded')} "
        f"median={quality.get('ecg_quality_score_median')}"
    )
    print(
        "Aggregate counts: "
        f"rhythm={screening['rhythm_candidate_count']} "
        f"artifact={screening['artifact_or_low_agreement_count']} "
        f"coverage={screening['coverage_gap_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
