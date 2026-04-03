from bleakheart_ui.features.sessions.ui_utils import format_elapsed_tick_label


def test_elapsed_tick_mm_ss_default():
    assert format_elapsed_tick_label(125.2, 1.0) == "02:05"


def test_elapsed_tick_hh_mm_ss_for_long_sessions():
    assert format_elapsed_tick_label(3723.0, 5.0) == "01:02:03"


def test_elapsed_tick_shows_milliseconds_when_zoomed_in():
    assert format_elapsed_tick_label(12.3456, 0.2) == "00:12.346"
    assert format_elapsed_tick_label(3723.4, 0.2) == "01:02:03.400"

