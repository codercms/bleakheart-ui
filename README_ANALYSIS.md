# README_ANALYSIS

## Purpose
`scripts2/` is a non-diagnostic, engineering-oriented ECG/RR screening toolkit for Polar H10 single-lead wearable recordings.

It is designed for:
- transparent signal-processing and screening workflows,
- detector-consistency and reliability checks,
- manual review support (not clinical diagnosis).

## Medical/Interpretation Scope
- This pipeline is **screening only** and **not medical diagnosis**.
- Polar H10 is single-lead wearable ECG (~130 Hz), not a clinical 12-lead setup.
- `device RR` is used as a practical reference stream, not absolute ground truth.
- Detector agreement improves engineering confidence but does not confirm pathology.

## Repository Layout (analysis)
- `scripts2/analyze_recording.py`: main pipeline CLI.
- `scripts2/io_utils.py`: robust CSV loading, timestamp parsing, overlap handling.
- `scripts2/ecg_metrics.py`: ECG preprocessing, R-peak detection, ECG-derived RR.
- `scripts2/hrv_metrics.py`: HRV metrics and 5-minute windows.
- `scripts2/rhythm_screening.py`: non-diagnostic screening heuristics.
- `scripts2/report_writer.py`: summary/metrics/report outputs.
- `scripts2/plotting.py`: PNG plots.
- `scripts2/diagnostic_suspicions.py`: explainability layer + suspicion PNGs.
- `scripts2/check_regression.py`: output invariant checks.
- `scripts2/compare_detectors.py`: segment-level detector comparison.
- `scripts2/validate_all_segments.py`: batch detector-consistency validation.

## Installation
Install analysis dependencies:

```bash
.\.venv\Scripts\python.exe -m pip install -r requirements-analysis.txt
```

Optional (only for WFDB detector comparisons):

```bash
.\.venv\Scripts\python.exe -m pip install wfdb
```

## Main Pipeline CLI
```bash
.\.venv\Scripts\python.exe scripts2\analyze_recording.py \
  --ecg sessions\participant_001_rest_20260329_165330\RawECG_recording.csv \
  --rr sessions\participant_001_rest_20260329_165330\RRinterval_recording.csv \
  --hr sessions\participant_001_rest_20260329_165330\HeartRate_recording.csv \
  --out sessions\participant_001_rest_20260329_165330\analysis_outputs_scripts2_explain \
  --segment-minutes 5 \
  --analysis-mode comprehensive \
  --with-diagnostic-suspicions
```

`--with-diagnostic-suspicions` is enabled by default (can be disabled with `--no-with-diagnostic-suspicions`).

## Main Outputs
The analysis output directory contains:
- `summary.md`: human-readable non-diagnostic summary.
- `metrics.json`: global metrics, confidence/coverage/reliability aggregates.
- `segment_metrics.csv`: per-segment metrics and screening class.
- `suspicious_segments.csv`: review segments for rhythm/artifact candidates (coverage gaps excluded).
- `hrv_5min_windows.csv`: 5-minute HRV windows.
- Plots: `hr_trend.png`, `tachogram_rr.png`, `poincare.png`, `hrv_psd.png`, `ecg_strips.png`.

If diagnostic suspicions are enabled:
- `diagnostic_suspicions.csv`
- `diagnostic_suspicions.json`
- `diagnostic_suspicions.md`
- `diagnostic_suspicions_png/*.png` (overview + zoom per suspicion)

## Explainability Semantics
Key fields in diagnostic suspicion outputs:
- `confidence_level`: trust in segment signal usability/analysis reliability.
- `interpretation_strength`: support strength for the selected suspicion interpretation.
- `confidence_vs_interpretation_note`: explicit text clarifying the difference.

These are intentionally different concepts.

For premature-beat-like patterns, the current logic includes:
- isolated-event penalty,
- anti-artifact guard,
- artifact-favored fallback when event looks spike/contact/noise-like.

Additional explainability fields:
- `event_count_supporting_pattern`
- `is_isolated_event`
- `artifact_favored`
- `artifact_clues`
- `morphology_support_level`

## Screening Classes
`segment_metrics.csv` / screening layer may use:
- `none`
- `tachy_candidate`
- `brady_candidate`
- `irregular_rr_candidate`
- `af_like_candidate`
- `ectopy_like_candidate`
- `pause_candidate`
- `artifact_candidate`
- `coverage_gap`

`coverage_gap` is treated as data-completeness issue, not a rhythm suspicion.

## Validation Workflows
### 1) Segment-level detector comparison
```bash
.\.venv\Scripts\python.exe scripts2\compare_detectors.py \
  --ecg sessions\participant_001_rest_20260329_165330\RawECG_recording.csv \
  --rr sessions\participant_001_rest_20260329_165330\RRinterval_recording.csv \
  --start 2026-03-29T14:20:00Z \
  --end 2026-03-29T14:30:00Z \
  --out comparison\segment1 \
  --with-wfdb
```

### 2) Batch validation across segment windows
```bash
.\.venv\Scripts\python.exe scripts2\validate_all_segments.py \
  --ecg sessions\participant_001_rest_20260329_165330\RawECG_recording.csv \
  --rr sessions\participant_001_rest_20260329_165330\RRinterval_recording.csv \
  --segment-metrics sessions\participant_001_rest_20260329_165330\analysis_outputs_scripts2_explain\segment_metrics.csv \
  --out comparison\all_segments_validation \
  --with-wfdb
```

Batch validation output includes:
- `per_segment_validation.csv`
- `detector_pairwise_metrics.csv`
- `device_rr_comparison.csv`
- `high_disagreement_segments.csv`
- `validation_summary.json`
- `validation_summary.md`
- aggregate validation plots

### 3) Lightweight regression check for one output dir
```bash
.\.venv\Scripts\python.exe scripts2\check_regression.py \
  --out sessions\participant_001_rest_20260329_165330\analysis_outputs_scripts2_explain
```

## Limitations
- Single-lead wearable ECG limits morphology interpretation reliability.
- Sampling rate and motion/contact artifacts can affect R-peak timing.
- Long non-stationary recordings limit LF/HF interpretability.
- Suspicion outputs are review aids, not clinical conclusions.
