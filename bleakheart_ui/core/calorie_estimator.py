from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class Sex(str, Enum):
    MALE = "male"
    FEMALE = "female"


class ActivityProfile(str, Enum):
    UNKNOWN = "unknown"
    REST = "rest"
    WALKING = "walking"
    RUNNING = "running"
    CYCLING = "cycling"
    ELLIPTICAL = "elliptical"
    STRENGTH = "strength_training"
    OTHER = "other"


@dataclass(slots=True)
class UserParams:
    sex: Sex
    age_years: float
    weight_kg: float
    hr_rest: float | None = None
    hr_max: float | None = None
    vo2max: float | None = None


@dataclass(slots=True)
class Sample:
    ts_sec: float
    hr_bpm: float | None
    speed_mps: float | None = None
    grade_frac: float | None = None
    power_w: float | None = None
    cadence_rpm: float | None = None
    resistance: float | None = None


@dataclass(slots=True)
class Estimate:
    gross_kcal: float
    active_kcal: float
    method: str


@dataclass(slots=True)
class EstimateStep(Estimate):
    gross_kcal_per_min: float
    active_kcal_per_min: float
    dt_sec: float


@dataclass(slots=True)
class EstimatorOptions:
    hr_min_bpm: float = 35.0
    hr_max_bpm: float = 230.0
    dt_max_sec: float = 10.0
    hr_hold_sec: float = 4.0
    ema_alpha_1hz: float = 0.20
    elliptical_resistance_max: float = 100.0


PROFILE_MET_BANDS: dict[ActivityProfile, tuple[float, float]] = {
    ActivityProfile.REST: (1.0, 1.3),
    ActivityProfile.WALKING: (2.2, 5.5),
    ActivityProfile.RUNNING: (6.0, 14.0),
    ActivityProfile.CYCLING: (4.0, 12.0),
    # Calibrated against recorded elliptical sessions (short/mid/long) to reduce
    # short-session overestimation without regressing medium/long behavior.
    ActivityProfile.ELLIPTICAL: (2.0, 13.5),
    ActivityProfile.STRENGTH: (3.0, 7.0),
    ActivityProfile.OTHER: (2.5, 8.0),
    ActivityProfile.UNKNOWN: (2.5, 7.0),
}


def activity_profile_from_label(label: str) -> ActivityProfile:
    txt = str(label or "").strip().lower()
    if txt == "rest":
        return ActivityProfile.REST
    if txt == "walking":
        return ActivityProfile.WALKING
    if txt == "running":
        return ActivityProfile.RUNNING
    if txt == "cycling":
        return ActivityProfile.CYCLING
    if txt == "elliptical":
        return ActivityProfile.ELLIPTICAL
    if txt == "strength training":
        return ActivityProfile.STRENGTH
    if txt == "other":
        return ActivityProfile.OTHER
    return ActivityProfile.UNKNOWN


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _sex_from_value(value: str) -> Sex:
    return Sex.FEMALE if str(value or "").strip().lower() == "female" else Sex.MALE


def user_params_from_profile(profile: dict, fallback: dict | None = None) -> UserParams:
    fb = fallback or {}
    sex = _sex_from_value(profile.get("sex", fb.get("sex", "male")))
    age = float(profile.get("age_years", fb.get("age_years", 30.0)))
    wt = float(profile.get("weight_kg", fb.get("weight_kg", 75.0)))
    hr_rest = profile.get("hr_rest", fb.get("hr_rest"))
    hr_max = profile.get("hr_max", fb.get("hr_max"))
    vo2max = profile.get("vo2max", fb.get("vo2max"))
    return UserParams(
        sex=sex,
        age_years=float(age),
        weight_kg=float(wt),
        hr_rest=float(hr_rest) if hr_rest is not None else None,
        hr_max=float(hr_max) if hr_max is not None else None,
        vo2max=float(vo2max) if vo2max is not None else None,
    )


def keytel_kcal_per_min(hr_bpm: float, user: UserParams) -> float:
    hr = float(hr_bpm)
    if user.sex == Sex.FEMALE:
        kcal_min = (-20.4022 + 0.4472 * hr - 0.1263 * user.weight_kg + 0.074 * user.age_years) / 4.184
    else:
        kcal_min = (-55.0969 + 0.6309 * hr + 0.1988 * user.weight_kg + 0.2017 * user.age_years) / 4.184
    return max(0.0, kcal_min)


def hrr_fraction(hr_bpm: float, hr_rest: float, hr_max: float) -> float:
    den = float(hr_max) - float(hr_rest)
    if den <= 0.0:
        return 0.0
    return _clamp((float(hr_bpm) - float(hr_rest)) / den, 0.0, 1.0)


def _gross_active_from_met(mets: float, weight_kg: float) -> tuple[float, float]:
    gross = float(mets) * 3.5 * float(weight_kg) / 200.0
    active = max(0.0, (float(mets) - 1.0) * 3.5 * float(weight_kg) / 200.0)
    return gross, active


def _kcal_from_power_w(power_w: float, gross_only_factor: float = 0.0597) -> tuple[float, float]:
    # Approximate external work -> metabolic kcal/min with average gross efficiency ~24%.
    gross = max(0.0, float(power_w) * float(gross_only_factor))
    active = max(0.0, gross - 1.0)
    return gross, active


def _kcal_from_speed_grade(speed_mps: float, grade_frac: float, weight_kg: float) -> tuple[float, float]:
    speed_mpm = max(0.0, float(speed_mps) * 60.0)
    grade = _clamp(float(grade_frac), -0.2, 0.3)
    if speed_mpm <= 134.0:
        vo2 = 3.5 + (0.1 * speed_mpm) + (1.8 * speed_mpm * grade)
    else:
        vo2 = 3.5 + (0.2 * speed_mpm) + (0.9 * speed_mpm * grade)
    gross = max(0.0, vo2 * float(weight_kg) / 200.0)
    active = max(0.0, (vo2 - 3.5) * float(weight_kg) / 200.0)
    return gross, active


def _kcal_from_elliptical_proxy(
    cadence_rpm: float,
    resistance: float,
    resistance_max: float,
    met_low: float,
    met_high: float,
    weight_kg: float,
) -> tuple[float, float]:
    cad_norm = _clamp((float(cadence_rpm) - 50.0) / 80.0, 0.0, 1.0)
    res_norm = _clamp(float(resistance) / max(1.0, float(resistance_max)), 0.0, 1.0)
    intensity = 0.65 * cad_norm + 0.35 * res_norm
    mets = float(met_low) + intensity * (float(met_high) - float(met_low))
    return _gross_active_from_met(mets, weight_kg)


class StreamingCalorieEstimator:
    def __init__(
        self,
        *,
        user: UserParams,
        profile: ActivityProfile,
        options: EstimatorOptions | None = None,
    ):
        self.user = user
        self.profile = profile
        self.options = options or EstimatorOptions()
        self.gross_kcal = 0.0
        self.active_kcal = 0.0
        self.method = "none"
        self.method_counts: dict[str, int] = {}

        self._prev_ts: float | None = None
        self._hr_ema: float | None = None
        self._last_hr_ts: float | None = None

    def _mark_method(self, method: str):
        self.method = str(method)
        self.method_counts[self.method] = int(self.method_counts.get(self.method, 0) + 1)

    def _alpha_for_dt(self, dt_sec: float) -> float:
        base = _clamp(self.options.ema_alpha_1hz, 0.01, 1.0)
        return _clamp(1.0 - ((1.0 - base) ** max(1e-6, float(dt_sec))), 0.01, 1.0)

    def _prepare_hr(self, ts_sec: float, hr_bpm: float | None, dt_sec: float) -> float | None:
        hr = None if hr_bpm is None else float(hr_bpm)
        if hr is not None and (hr < self.options.hr_min_bpm or hr > self.options.hr_max_bpm):
            hr = None

        if hr is not None:
            if self._hr_ema is None:
                self._hr_ema = hr
            else:
                a = self._alpha_for_dt(dt_sec)
                self._hr_ema = a * hr + (1.0 - a) * float(self._hr_ema)
            self._last_hr_ts = float(ts_sec)
            return float(self._hr_ema)

        if self._hr_ema is None or self._last_hr_ts is None:
            return None
        if (float(ts_sec) - float(self._last_hr_ts)) <= float(self.options.hr_hold_sec):
            return float(self._hr_ema)
        return None

    def _workload_rates(self, sample: Sample) -> tuple[float, float, str] | None:
        if sample.power_w is not None and float(sample.power_w) > 0.0:
            g, a = _kcal_from_power_w(float(sample.power_w))
            return g, a, "workload_power"

        if sample.speed_mps is not None and float(sample.speed_mps) > 0.0:
            grade = float(sample.grade_frac) if sample.grade_frac is not None else 0.0
            g, a = _kcal_from_speed_grade(float(sample.speed_mps), grade, self.user.weight_kg)
            return g, a, "workload_speed_grade"

        if self.profile == ActivityProfile.ELLIPTICAL:
            if sample.cadence_rpm is not None and sample.resistance is not None:
                low, high = PROFILE_MET_BANDS.get(self.profile, PROFILE_MET_BANDS[ActivityProfile.ELLIPTICAL])
                g, a = _kcal_from_elliptical_proxy(
                    cadence_rpm=float(sample.cadence_rpm),
                    resistance=float(sample.resistance),
                    resistance_max=self.options.elliptical_resistance_max,
                    met_low=low,
                    met_high=high,
                    weight_kg=self.user.weight_kg,
                )
                return g, a, "workload_elliptical_proxy"
        return None

    def _hrr_rates(self, hr_bpm: float) -> tuple[float, float, str] | None:
        if self.user.hr_rest is None or self.user.hr_max is None:
            return None
        hr_rest = float(self.user.hr_rest)
        hr_max = float(self.user.hr_max)
        if hr_max <= (hr_rest + 5.0):
            return None
        f = hrr_fraction(hr_bpm, hr_rest, hr_max)
        if self.user.vo2max is not None and float(self.user.vo2max) > 3.5:
            vo2 = 3.5 + f * (float(self.user.vo2max) - 3.5)
            gross = max(0.0, vo2 * self.user.weight_kg / 200.0)
            active = max(0.0, (vo2 - 3.5) * self.user.weight_kg / 200.0)
            return gross, active, "hrr_vo2r"
        met_low, met_high = PROFILE_MET_BANDS.get(self.profile, PROFILE_MET_BANDS[ActivityProfile.UNKNOWN])
        mets = met_low + f * (met_high - met_low)
        gross, active = _gross_active_from_met(mets, self.user.weight_kg)
        return gross, active, "hrr_met_band"

    def _fallback_rates(self, hr_bpm: float) -> tuple[float, float, str]:
        gross = keytel_kcal_per_min(hr_bpm, self.user)
        active = max(0.0, gross - (3.5 * self.user.weight_kg / 200.0))
        return gross, active, "keytel_fallback"

    def update(self, sample: Sample) -> EstimateStep:
        ts = float(sample.ts_sec)
        if self._prev_ts is None:
            self._prev_ts = ts
            _ = self._prepare_hr(ts, sample.hr_bpm, 1.0)
            return EstimateStep(
                gross_kcal=float(self.gross_kcal),
                active_kcal=float(self.active_kcal),
                method=self.method,
                gross_kcal_per_min=0.0,
                active_kcal_per_min=0.0,
                dt_sec=0.0,
            )

        dt_raw = max(0.0, ts - float(self._prev_ts))
        self._prev_ts = ts
        if dt_raw <= 0.0:
            return EstimateStep(
                gross_kcal=float(self.gross_kcal),
                active_kcal=float(self.active_kcal),
                method=self.method,
                gross_kcal_per_min=0.0,
                active_kcal_per_min=0.0,
                dt_sec=0.0,
            )
        dt_sec = min(float(dt_raw), float(self.options.dt_max_sec))

        hr_for_model = self._prepare_hr(ts, sample.hr_bpm, dt_sec)

        rates = self._workload_rates(sample)
        if rates is None and hr_for_model is not None:
            rates = self._hrr_rates(hr_for_model)
        if rates is None and hr_for_model is not None:
            rates = self._fallback_rates(hr_for_model)
        if rates is None:
            return EstimateStep(
                gross_kcal=float(self.gross_kcal),
                active_kcal=float(self.active_kcal),
                method=self.method,
                gross_kcal_per_min=0.0,
                active_kcal_per_min=0.0,
                dt_sec=float(dt_sec),
            )

        gross_rate, active_rate, method = rates
        self._mark_method(method)
        self.gross_kcal += float(gross_rate) * float(dt_sec) / 60.0
        self.active_kcal += float(active_rate) * float(dt_sec) / 60.0

        return EstimateStep(
            gross_kcal=float(self.gross_kcal),
            active_kcal=float(self.active_kcal),
            method=self.method,
            gross_kcal_per_min=float(gross_rate),
            active_kcal_per_min=float(active_rate),
            dt_sec=float(dt_sec),
        )

    def estimate_session(self, samples: Iterable[Sample]) -> Estimate:
        for sample in samples:
            self.update(sample)
        return Estimate(gross_kcal=float(self.gross_kcal), active_kcal=float(self.active_kcal), method=self.method)
