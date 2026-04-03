from bleakheart_ui.features.main.live_effort import LIVE_BPM_NEUTRAL, resolve_live_effort_badge


def test_live_effort_badges_thresholds_and_colors_for_current_profile_example():
    hr_rest = 60
    hr_max = 186
    cases = [
        (60, "", LIVE_BPM_NEUTRAL),
        (91, "", LIVE_BPM_NEUTRAL),
        (92, "Warm-up", "#3aaed8"),
        (109, "Warm-up", "#3aaed8"),
        (110, "Easy", "#34b56f"),
        (128, "Easy", "#34b56f"),
        (129, "Aerobic", "#d6a93a"),
        (147, "Aerobic", "#d6a93a"),
        (148, "Hard", "#df7d36"),
        (166, "Hard", "#df7d36"),
        (167, "Peak", "#de5a5a"),
        (186, "Peak", "#de5a5a"),
        (205, "Peak", "#de5a5a"),
    ]

    for hr, expected_label, expected_color in cases:
        badge = resolve_live_effort_badge(hr, hr_rest, hr_max)
        assert badge.label == expected_label
        assert badge.accent == expected_color


def test_live_effort_badges_shift_with_profile_hrr():
    # Same BPM should map differently when HR reserve changes.
    profile_a = resolve_live_effort_badge(150, hr_rest_bpm=60, hr_max_bpm=186)
    profile_b = resolve_live_effort_badge(150, hr_rest_bpm=50, hr_max_bpm=200)
    assert profile_a.label == "Hard"
    assert profile_b.label == "Aerobic"
