from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from ecg_metrics import analyze_ecg
from io_utils import load_ecg_csv, load_rr_csv

from compare_detectors import (  # reuse existing detector-compare helpers
    basic_hrv,
    compare_rr_streams,
    detect_peaks_baseline,
    detect_peaks_neurokit,
    detect_peaks_wfdb_gqrs,
    detect_peaks_wfdb_xqrs,
    match_peak_times,
    peaks_to_rr_ms,
    refine_peaks_local_max,
)

# High-disagreement thresholds (engineering consistency checks, not medical thresholds)
HIGH_DISAGREE_S2_NK_RR_MAE_MS = 25.0
HIGH_DISAGREE_S2_GQRS_RR_MAE_MS = 30.0
HIGH_DISAGREE_MIN_PEAK_RECALL = 0.95
HIGH_DISAGREE_S2_DEVICE_RR_MAE_MS = 80.0

MIN_ECG_SAMPLES_PER_SEGMENT = 500


@dataclass
class DetectorRun:
    name: str
    peaks: np.ndarray
    rr_df: pd.DataFrame
    hrv: dict[str, Any]
    notes: str | None = None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _median_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float(np.median(s.to_numpy(dtype=float)))


def _run_detectors(
    t_s: np.ndarray,
    ecg_uv: np.ndarray,
    expected_hr_bpm: float | None,
    with_wfdb: bool,
    include_baseline: bool,
) -> dict[str, DetectorRun]:
    out: dict[str, DetectorRun] = {}

    # scripts2_real path (actual project detector logic)
    s2 = analyze_ecg(t_s, ecg_uv, expected_hr_bpm=expected_hr_bpm)
    s2_peaks = s2.r_peak_indices.astype(int)
    s2_rr = pd.DataFrame({"timestamp_s": s2.rr_timestamps_s, "rr_ms": s2.rr_ms})
    if not s2_rr.empty:
        s2_rr["heart_rate_bpm"] = 60000.0 / s2_rr["rr_ms"]
    out["scripts2_real"] = DetectorRun(
        name="scripts2_real",
        peaks=s2_peaks,
        rr_df=s2_rr,
        hrv=basic_hrv(s2_rr["rr_ms"].to_numpy() if not s2_rr.empty else np.array([])),
        notes=f"detector_used={s2.metrics.get('detector_used')}",
    )

    # neurokit2 standalone
    try:
        nk_peaks = detect_peaks_neurokit(ecg_uv, fs=float(1.0 / np.median(np.diff(t_s))))
        nk_peaks = refine_peaks_local_max(ecg_uv, nk_peaks, fs=float(1.0 / np.median(np.diff(t_s))))
        nk_rr = peaks_to_rr_ms(t_s, nk_peaks)
        out["neurokit2"] = DetectorRun(
            name="neurokit2",
            peaks=nk_peaks,
            rr_df=nk_rr,
            hrv=basic_hrv(nk_rr["rr_ms"].to_numpy() if not nk_rr.empty else np.array([])),
        )
    except Exception as exc:
        out["neurokit2"] = DetectorRun(
            name="neurokit2",
            peaks=np.array([], dtype=int),
            rr_df=pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"]),
            hrv=basic_hrv(np.array([])),
            notes=f"failed:{exc}",
        )

    if with_wfdb:
        fs = float(1.0 / np.median(np.diff(t_s)))
        for name, fn in [("wfdb_gqrs", detect_peaks_wfdb_gqrs), ("wfdb_xqrs", detect_peaks_wfdb_xqrs)]:
            try:
                peaks = fn(ecg_uv, fs)
                peaks = refine_peaks_local_max(ecg_uv, peaks, fs=fs)
                rr = peaks_to_rr_ms(t_s, peaks)
                out[name] = DetectorRun(
                    name=name,
                    peaks=peaks,
                    rr_df=rr,
                    hrv=basic_hrv(rr["rr_ms"].to_numpy() if not rr.empty else np.array([])),
                )
            except Exception as exc:
                out[name] = DetectorRun(
                    name=name,
                    peaks=np.array([], dtype=int),
                    rr_df=pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"]),
                    hrv=basic_hrv(np.array([])),
                    notes=f"failed:{exc}",
                )

    if include_baseline:
        fs = float(1.0 / np.median(np.diff(t_s)))
        try:
            peaks = detect_peaks_baseline(ecg_uv, fs)
            rr = peaks_to_rr_ms(t_s, peaks)
            out["baseline_scipy"] = DetectorRun(
                name="baseline_scipy",
                peaks=peaks,
                rr_df=rr,
                hrv=basic_hrv(rr["rr_ms"].to_numpy() if not rr.empty else np.array([])),
            )
        except Exception as exc:
            out["baseline_scipy"] = DetectorRun(
                name="baseline_scipy",
                peaks=np.array([], dtype=int),
                rr_df=pd.DataFrame(columns=["timestamp_s", "rr_ms", "heart_rate_bpm"]),
                hrv=basic_hrv(np.array([])),
                notes=f"failed:{exc}",
            )

    return out


def _plot_hist_rr_mae(device_df: pd.DataFrame, out_path: Path) -> None:
    plt.figure(figsize=(10, 5))
    for det, grp in device_df.groupby("detector"):
        vals = pd.to_numeric(grp["rr_mae_vs_device_ms"], errors="coerce").dropna().to_numpy(dtype=float)
        if vals.size:
            plt.hist(vals, bins=25, alpha=0.4, label=det)
    plt.xlabel("RR MAE vs device (ms)")
    plt.ylabel("Count")
    plt.title("RR MAE vs Device RR Histogram")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def _plot_box_by_detector(device_df: pd.DataFrame, out_path: Path) -> None:
    data = []
    labels = []
    for det, grp in device_df.groupby("detector"):
        vals = pd.to_numeric(grp["rr_mae_vs_device_ms"], errors="coerce").dropna().to_numpy(dtype=float)
        if vals.size:
            data.append(vals)
            labels.append(det)
    if not data:
        return
    plt.figure(figsize=(10, 5))
    plt.boxplot(data, tick_labels=labels, showfliers=False)
    plt.ylabel("RR MAE vs device (ms)")
    plt.title("RR MAE by Detector")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def _plot_box_by_class(device_df: pd.DataFrame, out_path: Path) -> None:
    tmp = device_df[device_df["detector"] == "scripts2_real"].copy()
    data = []
    labels = []
    for cls, grp in tmp.groupby("screening_class"):
        vals = pd.to_numeric(grp["rr_mae_vs_device_ms"], errors="coerce").dropna().to_numpy(dtype=float)
        if vals.size:
            data.append(vals)
            labels.append(cls)
    if not data:
        return
    plt.figure(figsize=(10, 5))
    plt.boxplot(data, tick_labels=labels, showfliers=False)
    plt.ylabel("RR MAE vs device (ms)")
    plt.title("scripts2_real RR MAE by screening_class")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def _plot_scatter_quality_vs_disagreement(per_seg_df: pd.DataFrame, out_path: Path) -> None:
    x = pd.to_numeric(per_seg_df["ecg_quality_score_final"], errors="coerce")
    y = pd.to_numeric(per_seg_df["s2_vs_nk_rr_mae_ms"], errors="coerce")
    mask = x.notna() & y.notna()
    if not mask.any():
        return
    plt.figure(figsize=(8, 5))
    plt.scatter(x[mask], y[mask], s=28, alpha=0.8)
    plt.xlabel("ECG quality score final")
    plt.ylabel("scripts2_real vs neurokit2 RR MAE (ms)")
    plt.title("Quality vs Detector Disagreement")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def _plot_high_disagreement_by_class(high_df: pd.DataFrame, out_path: Path) -> None:
    if high_df.empty or "screening_class" not in high_df.columns:
        counts = pd.Series(dtype=int)
    else:
        counts = high_df["screening_class"].value_counts()
    plt.figure(figsize=(8, 4))
    if counts.empty:
        plt.bar(["none"], [0])
    else:
        plt.bar(counts.index.astype(str), counts.values)
    plt.ylabel("High disagreement segments")
    plt.title("High Disagreement by screening_class")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Batch detector-consistency validation across segment_metrics windows")
    p.add_argument("--ecg", type=Path, required=True)
    p.add_argument("--rr", type=Path, required=True)
    p.add_argument("--segment-metrics", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--with-wfdb", action="store_true")
    p.add_argument("--include-baseline", action="store_true")
    p.add_argument("--max-segments", type=int, default=None)
    p.add_argument("--only-screening-class", type=str, default=None)
    p.add_argument("--only-confidence", type=str, default=None)
    return p


def main() -> int:
    args = build_parser().parse_args()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ecg = load_ecg_csv(args.ecg.resolve())
    rr = load_rr_csv(args.rr.resolve())
    seg_df = pd.read_csv(args.segment_metrics.resolve())

    required_cols = [
        "segment_start_s",
        "segment_end_s",
        "screening_class",
        "confidence_level",
        "segment_status",
    ]
    missing = [c for c in required_cols if c not in seg_df.columns]
    if missing:
        raise ValueError(f"segment_metrics.csv missing required columns: {missing}")

    if args.only_screening_class:
        seg_df = seg_df[seg_df["screening_class"].astype(str) == args.only_screening_class]
    if args.only_confidence:
        seg_df = seg_df[seg_df["confidence_level"].astype(str) == args.only_confidence]
    if args.max_segments is not None:
        seg_df = seg_df.head(max(0, int(args.max_segments)))

    per_seg_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    device_rows: list[dict[str, Any]] = []
    high_rows: list[dict[str, Any]] = []

    skipped_reasons: dict[str, int] = {}
    analyzable_segments = 0

    for i, row in seg_df.reset_index(drop=True).iterrows():
        seg_id = int(i)
        start_s = float(row["segment_start_s"])
        end_s = float(row["segment_end_s"])
        screening_class = str(row.get("screening_class", "none"))
        confidence = str(row.get("confidence_level", "unknown"))
        segment_status = str(row.get("segment_status", "unknown"))
        review_reason = str(row.get("review_reason", ""))
        quality_not_applicable = _to_bool(row.get("quality_not_applicable", False))
        quality_score = row.get("ecg_quality_score_final")

        base_meta = {
            "segment_id": seg_id,
            "segment_start_s": start_s,
            "segment_end_s": end_s,
            "duration_s": end_s - start_s,
            "screening_class": screening_class,
            "confidence_level": confidence,
            "segment_status": segment_status,
            "review_reason": review_reason,
            "quality_not_applicable": quality_not_applicable,
            "ecg_quality_score_final": quality_score,
        }

        if screening_class == "coverage_gap" or segment_status != "ok":
            skipped_reasons["coverage_or_non_ok_status"] = skipped_reasons.get("coverage_or_non_ok_status", 0) + 1
            per_seg_rows.append({**base_meta, "analyzable": False, "skip_reason": "coverage_or_non_ok_status"})
            continue

        ecg_mask = (ecg.timestamps_s >= start_s) & (ecg.timestamps_s < end_s)
        rr_mask = (rr.timestamps_s >= start_s) & (rr.timestamps_s < end_s)

        seg_t = ecg.timestamps_s[ecg_mask]
        seg_x = ecg.ecg_uv[ecg_mask]
        seg_rr_t = rr.timestamps_s[rr_mask]
        seg_rr_ms = rr.rr_ms[rr_mask]
        seg_rr_df = pd.DataFrame({"timestamp_s": seg_rr_t, "rr_ms": seg_rr_ms})

        if seg_t.size < MIN_ECG_SAMPLES_PER_SEGMENT:
            skipped_reasons["insufficient_ecg_samples"] = skipped_reasons.get("insufficient_ecg_samples", 0) + 1
            per_seg_rows.append({**base_meta, "analyzable": False, "skip_reason": "insufficient_ecg_samples"})
            continue
        if seg_rr_df.empty:
            skipped_reasons["no_device_rr"] = skipped_reasons.get("no_device_rr", 0) + 1
            per_seg_rows.append({**base_meta, "analyzable": False, "skip_reason": "no_device_rr"})
            continue

        expected_hr_bpm = None
        rr_valid = seg_rr_df["rr_ms"].to_numpy(dtype=float)
        rr_valid = rr_valid[np.isfinite(rr_valid) & (rr_valid > 0)]
        if rr_valid.size:
            expected_hr_bpm = float(np.nanmedian(60000.0 / rr_valid))

        det_runs = _run_detectors(
            t_s=seg_t,
            ecg_uv=seg_x,
            expected_hr_bpm=expected_hr_bpm,
            with_wfdb=args.with_wfdb,
            include_baseline=args.include_baseline,
        )

        analyzable_segments += 1

        seg_summary = {**base_meta, "analyzable": True, "skip_reason": ""}
        for det_name, det in det_runs.items():
            seg_summary[f"{det_name}_peak_count"] = int(det.peaks.size)
            seg_summary[f"{det_name}_rr_count"] = int(det.rr_df.shape[0])
            seg_summary[f"{det_name}_hr_mean_bpm"] = det.hrv.get("hr_mean_bpm")
            seg_summary[f"{det_name}_hr_min_bpm"] = det.hrv.get("hr_min_bpm")
            seg_summary[f"{det_name}_hr_max_bpm"] = det.hrv.get("hr_max_bpm")
            seg_summary[f"{det_name}_mean_nn_ms"] = det.hrv.get("mean_nn_ms")
            seg_summary[f"{det_name}_sdnn_ms"] = det.hrv.get("sdnn_ms")
            seg_summary[f"{det_name}_rmssd_ms"] = det.hrv.get("rmssd_ms")
            seg_summary[f"{det_name}_notes"] = det.notes or ""

        # pairwise comparisons
        det_names = list(det_runs.keys())
        for idx_a in range(len(det_names)):
            for idx_b in range(idx_a + 1, len(det_names)):
                a_name = det_names[idx_a]
                b_name = det_names[idx_b]
                a = det_runs[a_name]
                b = det_runs[b_name]

                a_peak_t = seg_t[a.peaks] if a.peaks.size else np.array([], dtype=float)
                b_peak_t = seg_t[b.peaks] if b.peaks.size else np.array([], dtype=float)
                pm = match_peak_times(a_peak_t, b_peak_t, tolerance_ms=75.0)
                rr_cmp = compare_rr_streams(a.rr_df, b.rr_df, tolerance_ms=150.0)
                pair_rows.append(
                    {
                        **base_meta,
                        "analyzable": True,
                        "detector_a": a_name,
                        "detector_b": b_name,
                        "peak_matched": pm["matched"],
                        "peak_a_count": pm["a_count"],
                        "peak_b_count": pm["b_count"],
                        "peak_precision": pm["precision"],
                        "peak_recall": pm["recall"],
                        "peak_mean_timing_error_ms": pm["mean_timing_error_ms"],
                        "peak_p95_timing_error_ms": pm["p95_timing_error_ms"],
                        "rr_matched": rr_cmp["matched"],
                        "rr_mae_ms": rr_cmp["mae_ms"],
                        "rr_median_ae_ms": rr_cmp["median_ae_ms"],
                        "rr_p95_ae_ms": rr_cmp["p95_ae_ms"],
                    }
                )

                if a_name == "scripts2_real" and b_name == "neurokit2":
                    seg_summary["s2_vs_nk_rr_mae_ms"] = rr_cmp["mae_ms"]
                    seg_summary["s2_vs_nk_peak_recall"] = pm["recall"]
                if a_name == "scripts2_real" and b_name == "wfdb_gqrs":
                    seg_summary["s2_vs_gqrs_rr_mae_ms"] = rr_cmp["mae_ms"]
                    seg_summary["s2_vs_gqrs_peak_recall"] = pm["recall"]

        # device comparisons
        for det_name, det in det_runs.items():
            rr_cmp = compare_rr_streams(det.rr_df, seg_rr_df, tolerance_ms=150.0)
            device_rows.append(
                {
                    **base_meta,
                    "analyzable": True,
                    "detector": det_name,
                    "rr_matched_vs_device": rr_cmp["matched"],
                    "rr_mae_vs_device_ms": rr_cmp["mae_ms"],
                    "rr_median_ae_vs_device_ms": rr_cmp["median_ae_ms"],
                    "rr_p95_ae_vs_device_ms": rr_cmp["p95_ae_ms"],
                }
            )
            if det_name == "scripts2_real":
                seg_summary["s2_vs_device_rr_mae_ms"] = rr_cmp["mae_ms"]

        # high disagreement heuristic
        high_reasons: list[str] = []
        s2nk = seg_summary.get("s2_vs_nk_rr_mae_ms")
        s2g = seg_summary.get("s2_vs_gqrs_rr_mae_ms")
        s2nk_recall = seg_summary.get("s2_vs_nk_peak_recall")
        s2_dev = seg_summary.get("s2_vs_device_rr_mae_ms")

        if s2nk is not None and float(s2nk) > HIGH_DISAGREE_S2_NK_RR_MAE_MS:
            high_reasons.append("s2_vs_nk_rr_mae_high")
        if s2g is not None and float(s2g) > HIGH_DISAGREE_S2_GQRS_RR_MAE_MS:
            high_reasons.append("s2_vs_gqrs_rr_mae_high")
        if s2nk_recall is not None and float(s2nk_recall) < HIGH_DISAGREE_MIN_PEAK_RECALL:
            high_reasons.append("s2_vs_nk_peak_recall_low")
        if s2_dev is not None and float(s2_dev) > HIGH_DISAGREE_S2_DEVICE_RR_MAE_MS:
            high_reasons.append("s2_vs_device_rr_mae_high")

        seg_summary["high_disagreement"] = bool(high_reasons)
        seg_summary["high_disagreement_reasons"] = "|".join(high_reasons)

        if high_reasons:
            severity = 0.0
            severity += float(s2nk or 0.0)
            severity += float(s2g or 0.0)
            severity += float(s2_dev or 0.0)
            high_rows.append({**seg_summary, "high_disagreement_severity": severity})

        per_seg_rows.append(seg_summary)

    per_seg_df = pd.DataFrame(per_seg_rows)
    pair_df = pd.DataFrame(pair_rows)
    device_df = pd.DataFrame(device_rows)
    high_df = pd.DataFrame(high_rows)
    if not high_df.empty:
        high_df = high_df.sort_values("high_disagreement_severity", ascending=False)

    per_seg_df.to_csv(out_dir / "per_segment_validation.csv", index=False)
    pair_df.to_csv(out_dir / "detector_pairwise_metrics.csv", index=False)
    device_df.to_csv(out_dir / "device_rr_comparison.csv", index=False)
    high_df.to_csv(out_dir / "high_disagreement_segments.csv", index=False)

    # aggregate summaries
    total_segments = int(seg_df.shape[0])
    skipped_segments = int(total_segments - analyzable_segments)

    pair_agg: dict[str, Any] = {}
    if not pair_df.empty:
        pair_df["pair"] = pair_df["detector_a"].astype(str) + "__vs__" + pair_df["detector_b"].astype(str)
        for pair_name, grp in pair_df.groupby("pair"):
            pair_agg[pair_name] = {
                "segment_count": int(grp.shape[0]),
                "rr_mae_median_ms": _median_or_none(grp["rr_mae_ms"]),
                "peak_precision_median": _median_or_none(grp["peak_precision"]),
                "peak_recall_median": _median_or_none(grp["peak_recall"]),
            }

    dev_agg: dict[str, Any] = {}
    if not device_df.empty:
        for det, grp in device_df.groupby("detector"):
            dev_agg[str(det)] = {
                "segment_count": int(grp.shape[0]),
                "rr_mae_median_ms": _median_or_none(grp["rr_mae_vs_device_ms"]),
                "rr_p95_median_ms": _median_or_none(grp["rr_p95_ae_vs_device_ms"]),
            }

    class_agg: dict[str, Any] = {}
    analyzable = per_seg_df[per_seg_df["analyzable"] == True].copy()
    if not analyzable.empty:
        for cls, grp in analyzable.groupby("screening_class"):
            cls_dict: dict[str, Any] = {
                "segment_count": int(grp.shape[0]),
                "high_disagreement_count": int(grp.get("high_disagreement", False).astype(bool).sum()),
                "s2_vs_nk_rr_mae_median_ms": _median_or_none(grp["s2_vs_nk_rr_mae_ms"]),
                "s2_vs_gqrs_rr_mae_median_ms": _median_or_none(grp["s2_vs_gqrs_rr_mae_ms"])
                if "s2_vs_gqrs_rr_mae_ms" in grp.columns
                else None,
                "s2_vs_device_rr_mae_median_ms": _median_or_none(grp["s2_vs_device_rr_mae_ms"]),
            }
            class_agg[str(cls)] = cls_dict

    summary = {
        "validation_type": "engineering_detector_agreement",
        "disclaimer": "Detector agreement is not clinical truth. Device RR is practical reference, not absolute ground truth.",
        "inputs": {
            "ecg": str(args.ecg.resolve()),
            "rr": str(args.rr.resolve()),
            "segment_metrics": str(args.segment_metrics.resolve()),
        },
        "settings": {
            "with_wfdb": bool(args.with_wfdb),
            "include_baseline": bool(args.include_baseline),
            "high_disagreement_thresholds": {
                "HIGH_DISAGREE_S2_NK_RR_MAE_MS": HIGH_DISAGREE_S2_NK_RR_MAE_MS,
                "HIGH_DISAGREE_S2_GQRS_RR_MAE_MS": HIGH_DISAGREE_S2_GQRS_RR_MAE_MS,
                "HIGH_DISAGREE_MIN_PEAK_RECALL": HIGH_DISAGREE_MIN_PEAK_RECALL,
                "HIGH_DISAGREE_S2_DEVICE_RR_MAE_MS": HIGH_DISAGREE_S2_DEVICE_RR_MAE_MS,
            },
        },
        "counts": {
            "total_segments": total_segments,
            "analyzable_segments": analyzable_segments,
            "skipped_segments": skipped_segments,
            "skipped_reasons": skipped_reasons,
            "high_disagreement_segments": int(high_df.shape[0]),
        },
        "detector_pairwise_summary": pair_agg,
        "device_rr_summary": dev_agg,
        "by_screening_class": class_agg,
        "worst_segments": high_df.head(12).to_dict(orient="records") if not high_df.empty else [],
    }

    with (out_dir / "validation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # plots
    if not device_df.empty:
        _plot_hist_rr_mae(device_df, out_dir / "hist_rr_mae_vs_device_by_detector.png")
        _plot_box_by_detector(device_df, out_dir / "boxplot_rr_mae_by_detector.png")
        _plot_box_by_class(device_df, out_dir / "boxplot_rr_mae_by_screening_class.png")
    if not per_seg_df.empty:
        _plot_scatter_quality_vs_disagreement(per_seg_df, out_dir / "scatter_quality_vs_disagreement.png")
    _plot_high_disagreement_by_class(high_df, out_dir / "bar_high_disagreement_by_class.png")

    # markdown summary
    pair_s2_nk = pair_agg.get("scripts2_real__vs__neurokit2", {})
    pair_s2_gqrs = pair_agg.get("scripts2_real__vs__wfdb_gqrs", {})
    dev_s2 = dev_agg.get("scripts2_real", {})
    worst_lines = []
    if not high_df.empty:
        for _, r in high_df.head(10).iterrows():
            worst_lines.append(
                f"- seg#{int(r['segment_id'])} {r['segment_start_s']:.3f}-{r['segment_end_s']:.3f} "
                f"class={r.get('screening_class')} conf={r.get('confidence_level')} "
                f"quality={r.get('ecg_quality_score_final')} reasons={r.get('high_disagreement_reasons')}"
            )

    md_lines = [
        "# Batch Detector Validation Summary",
        "",
        "This is engineering detector-consistency validation, not clinical validation.",
        "Agreement with device RR improves engineering confidence but is not medical diagnosis.",
        "",
        "## Executive Summary",
        f"- Total segments: `{total_segments}`",
        f"- Analyzable segments: `{analyzable_segments}`",
        f"- Skipped segments: `{skipped_segments}`",
        f"- Skipped reasons: `{skipped_reasons}`",
        f"- Median scripts2_real vs neurokit2 RR MAE: `{pair_s2_nk.get('rr_mae_median_ms')}` ms",
        f"- Median scripts2_real vs wfdb_gqrs RR MAE: `{pair_s2_gqrs.get('rr_mae_median_ms')}` ms",
        f"- Median scripts2_real vs device RR MAE: `{dev_s2.get('rr_mae_median_ms')}` ms",
        f"- High disagreement segments: `{int(high_df.shape[0])}`",
        "",
        "## Breakdown by screening_class",
    ]

    if class_agg:
        for cls, vals in class_agg.items():
            md_lines.append(
                f"- `{cls}`: n={vals['segment_count']}, high_disagreement={vals['high_disagreement_count']}, "
                f"s2~nk RR MAE median={vals['s2_vs_nk_rr_mae_median_ms']}, "
                f"s2~gqrs RR MAE median={vals['s2_vs_gqrs_rr_mae_median_ms']}, "
                f"s2~device RR MAE median={vals['s2_vs_device_rr_mae_median_ms']}"
            )
    else:
        md_lines.append("- no analyzable segments")

    md_lines.extend([
        "",
        "## Worst Segments",
    ])
    if worst_lines:
        md_lines.extend(worst_lines)
    else:
        md_lines.append("- none")

    (out_dir / "validation_summary.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print("Batch validation complete")
    print(f"Output directory: {out_dir}")
    print(f"Analyzable segments: {analyzable_segments} / {total_segments}")
    print(f"High disagreement segments: {int(high_df.shape[0])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
