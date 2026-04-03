def should_reset_stale_signal_poll(
    *,
    inflight: bool,
    started_mono: float,
    now_mono: float,
    timeout_s: float,
) -> bool:
    if not inflight:
        return False
    if started_mono <= 0.0:
        return True
    return (now_mono - started_mono) >= timeout_s


def should_refresh_signal_tile(
    *,
    connected: bool,
    last_refresh_mono: float,
    now_mono: float,
    interval_s: float,
) -> bool:
    if not connected:
        return False
    return (now_mono - last_refresh_mono) >= interval_s
