from __future__ import annotations

from dataclasses import dataclass


LIVE_BPM_NEUTRAL = "#64748b"


@dataclass(frozen=True)
class LiveEffortBadge:
    label: str
    accent: str


@dataclass(frozen=True)
class _LiveEffortBand:
    lower_hrr_fraction: float
    upper_hrr_fraction_exclusive: float | None
    label: str
    accent: str


# UI-only effort badges for live BPM tile.
# This intentionally does not affect HRR training zones used by analytics/storage.
LIVE_EFFORT_BANDS: tuple[_LiveEffortBand, ...] = (
    _LiveEffortBand(lower_hrr_fraction=0.25, upper_hrr_fraction_exclusive=0.40, label="Warm-up", accent="#3aaed8"),
    _LiveEffortBand(lower_hrr_fraction=0.40, upper_hrr_fraction_exclusive=0.55, label="Easy", accent="#34b56f"),
    _LiveEffortBand(lower_hrr_fraction=0.55, upper_hrr_fraction_exclusive=0.70, label="Aerobic", accent="#d6a93a"),
    _LiveEffortBand(lower_hrr_fraction=0.70, upper_hrr_fraction_exclusive=0.85, label="Hard", accent="#df7d36"),
    _LiveEffortBand(lower_hrr_fraction=0.85, upper_hrr_fraction_exclusive=None, label="Peak", accent="#de5a5a"),
)


def _target_bpm(hr_rest_bpm: int | float, hr_max_bpm: int | float, fraction: float) -> int:
    rest = float(hr_rest_bpm)
    hr_max = float(hr_max_bpm)
    reserve = max(1.0, hr_max - rest)
    return int(round(rest + float(fraction) * reserve))


def resolve_live_effort_badge(hr_bpm: int | float, hr_rest_bpm: int | float, hr_max_bpm: int | float) -> LiveEffortBadge:
    hr = int(round(float(hr_bpm)))
    for band in LIVE_EFFORT_BANDS:
        lo = _target_bpm(hr_rest_bpm, hr_max_bpm, band.lower_hrr_fraction)
        hi = None
        if band.upper_hrr_fraction_exclusive is not None:
            hi = _target_bpm(hr_rest_bpm, hr_max_bpm, band.upper_hrr_fraction_exclusive)
        if hr < lo:
            continue
        if hi is not None and hr >= hi:
            continue
        return LiveEffortBadge(label=band.label, accent=band.accent)
    return LiveEffortBadge(label="", accent=LIVE_BPM_NEUTRAL)
