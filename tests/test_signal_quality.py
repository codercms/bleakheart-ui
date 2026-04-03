from bleakheart_ui.features.main.signal_quality import (
    should_refresh_signal_tile,
    should_reset_stale_signal_poll,
)


def test_should_reset_stale_signal_poll_when_not_inflight():
    assert (
        should_reset_stale_signal_poll(
            inflight=False,
            started_mono=0.0,
            now_mono=100.0,
            timeout_s=8.0,
        )
        is False
    )


def test_should_reset_stale_signal_poll_when_started_time_missing():
    assert (
        should_reset_stale_signal_poll(
            inflight=True,
            started_mono=0.0,
            now_mono=100.0,
            timeout_s=8.0,
        )
        is True
    )


def test_should_reset_stale_signal_poll_after_timeout():
    assert (
        should_reset_stale_signal_poll(
            inflight=True,
            started_mono=10.0,
            now_mono=18.0,
            timeout_s=8.0,
        )
        is True
    )


def test_should_not_reset_stale_signal_poll_before_timeout():
    assert (
        should_reset_stale_signal_poll(
            inflight=True,
            started_mono=10.0,
            now_mono=17.99,
            timeout_s=8.0,
        )
        is False
    )


def test_should_refresh_signal_tile_only_when_connected_and_due():
    assert (
        should_refresh_signal_tile(
            connected=False,
            last_refresh_mono=100.0,
            now_mono=105.0,
            interval_s=1.0,
        )
        is False
    )
    assert (
        should_refresh_signal_tile(
            connected=True,
            last_refresh_mono=100.0,
            now_mono=100.5,
            interval_s=1.0,
        )
        is False
    )
    assert (
        should_refresh_signal_tile(
            connected=True,
            last_refresh_mono=100.0,
            now_mono=101.0,
            interval_s=1.0,
        )
        is True
    )
