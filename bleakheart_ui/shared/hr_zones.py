from __future__ import annotations

from collections.abc import Mapping
from typing import Any


ZONE_NAMES = ("Recovery", "Base", "Aerobic", "Threshold", "Peak")
ZONE_PCTS = ((0.50, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 1.00))


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def estimate_hr_max(age_years: float, sex: str) -> int:
    age = max(1.0, min(120.0, float(age_years)))
    sx = str(sex or "").strip().lower()
    # Age-based estimates (widely used fitness formulas).
    raw = (206.0 - 0.88 * age) if sx == "female" else (208.0 - 0.70 * age)
    return int(round(max(120.0, min(220.0, raw))))


def estimate_hr_rest(age_years: float, sex: str, weight_kg: float, height_cm: float) -> int:
    age = max(1.0, min(120.0, float(age_years)))
    sx = str(sex or "").strip().lower()
    wt = max(1.0, float(weight_kg))
    h_cm = max(50.0, float(height_cm))
    h_m = h_cm / 100.0
    bmi = wt / (h_m * h_m) if h_m > 0.0 else 22.0
    base = 66.0 if sx == "female" else 62.0
    raw = base + 0.14 * (age - 30.0) + 0.32 * (bmi - 22.0)
    return int(round(max(45.0, min(95.0, raw))))


def resolve_hr_profile(profile: Mapping[str, Any] | None, fallback: Mapping[str, Any] | None = None) -> tuple[int, int, bool, bool]:
    src = profile or {}
    fb = fallback or {}
    age = _safe_float(src.get("age_years", fb.get("age_years", 30.0)), 30.0)
    sex = str(src.get("sex", fb.get("sex", "male")) or "male").strip().lower()
    weight = _safe_float(src.get("weight_kg", fb.get("weight_kg", 75.0)), 75.0)
    height = _safe_float(src.get("height_cm", fb.get("height_cm", 175.0)), 175.0)

    inferred_rest = estimate_hr_rest(age, sex, weight, height)
    inferred_max = estimate_hr_max(age, sex)

    rest_raw = src.get("hr_rest", None)
    max_raw = src.get("hr_max", None)
    has_rest = rest_raw is not None
    has_max = max_raw is not None

    rest = _safe_float(rest_raw, inferred_rest) if has_rest else float(inferred_rest)
    hr_max = _safe_float(max_raw, inferred_max) if has_max else float(inferred_max)

    rest = max(35.0, min(120.0, rest))
    hr_max = max(120.0, min(240.0, hr_max))
    if hr_max <= rest + 8.0:
        hr_max = min(240.0, rest + 35.0)

    return int(round(rest)), int(round(hr_max)), bool(has_rest), bool(has_max)


def zone_ranges_from_rest_max(hr_rest: int | float, hr_max: int | float) -> list[tuple[int, int]]:
    rest = float(hr_rest)
    max_hr = float(hr_max)
    reserve = max(20.0, max_hr - rest)
    out: list[tuple[int, int]] = []
    for lo, hi in ZONE_PCTS:
        z_lo = int(round(rest + reserve * lo))
        z_hi = int(round(rest + reserve * hi))
        if z_hi <= z_lo:
            z_hi = z_lo + 1
        out.append((z_lo, z_hi))
    return out


def zone_index_for_hr(hr_bpm: int | float, zone_ranges: list[tuple[int, int]]) -> int:
    hr = float(hr_bpm)
    if not zone_ranges:
        return 0
    for idx in range(min(4, len(zone_ranges) - 1)):
        if hr < float(zone_ranges[idx][1]):
            return idx
    return min(4, len(zone_ranges) - 1)
