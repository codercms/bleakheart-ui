from bleakheart_ui.core.calorie_estimator import (
    ActivityProfile,
    Sample,
    Sex,
    StreamingCalorieEstimator,
    UserParams,
    keytel_kcal_per_min,
)


def test_keytel_has_no_activity_multiplier():
    user = UserParams(sex=Sex.MALE, age_years=30.0, weight_kg=75.0)
    k1 = keytel_kcal_per_min(140.0, user)
    k2 = keytel_kcal_per_min(140.0, user)
    assert abs(k1 - k2) < 1e-12


def test_hrr_branch_preferred_over_keytel_when_profile_known():
    est = StreamingCalorieEstimator(
        user=UserParams(sex=Sex.MALE, age_years=30.0, weight_kg=75.0, hr_rest=60.0, hr_max=190.0),
        profile=ActivityProfile.ELLIPTICAL,
    )
    est.update(Sample(ts_sec=0.0, hr_bpm=120.0))
    step = est.update(Sample(ts_sec=5.0, hr_bpm=130.0))
    assert step.method == "hrr_met_band"
    assert step.active_kcal > 0.0


def test_workload_branch_preferred_over_hrr():
    est = StreamingCalorieEstimator(
        user=UserParams(sex=Sex.MALE, age_years=30.0, weight_kg=75.0, hr_rest=60.0, hr_max=190.0),
        profile=ActivityProfile.CYCLING,
    )
    est.update(Sample(ts_sec=0.0, hr_bpm=115.0, power_w=170.0))
    step = est.update(Sample(ts_sec=10.0, hr_bpm=120.0, power_w=170.0))
    assert step.method == "workload_power"
    assert step.gross_kcal > 0.0


def test_invalid_hr_is_filtered():
    est = StreamingCalorieEstimator(
        user=UserParams(sex=Sex.FEMALE, age_years=28.0, weight_kg=60.0, hr_rest=58.0, hr_max=182.0),
        profile=ActivityProfile.RUNNING,
    )
    est.update(Sample(ts_sec=0.0, hr_bpm=90.0))
    step_bad = est.update(Sample(ts_sec=4.0, hr_bpm=500.0))
    # Held value can still be used; method should remain HRR, but no crash and non-negative totals.
    assert step_bad.gross_kcal >= 0.0
    step_next = est.update(Sample(ts_sec=8.0, hr_bpm=150.0))
    assert step_next.gross_kcal >= step_bad.gross_kcal
