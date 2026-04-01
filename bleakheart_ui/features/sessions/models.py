from dataclasses import dataclass


@dataclass(slots=True)
class SessionSummary:
    session_id: str
    session_path: str
    start_ts: float
    end_ts: float
    duration_s: float
    profile_id: str
    profile_name: str
    activity_name: str
    kcal: float
    avg_hr_bpm: float
    max_hr_bpm: float
    min_hr_bpm: float
    has_rr: bool
    has_ecg: bool
    preview: list[float]
    indexed_at: float


@dataclass(slots=True)
class SessionSeries:
    session: SessionSummary
    zero_at_ts: float
    bpm_t: list[float]
    bpm_v: list[float]
    rr_t: list[float]
    rr_v: list[float]
    ecg_t: list[float]
    ecg_v: list[float]
    hr_rest_est: float
    hr_max_est: float
    zone_ranges_bpm: list[tuple[int, int]]
    zones_seconds: list[float]
    zones_percent: list[float]
