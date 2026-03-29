from collections import deque
import numpy as np

from ecg_render_core import EcgRingBuffer


class RenderController:
    def __init__(
        self,
        charts,
        *,
        hr_window_sec: float = 60.0,
        rr_window_sec: float = 60.0,
        acc_window_sec: float = 20.0,
        ecg_window_sec: float = 20.0,
        ecg_fixed_y_limits: tuple[float, float] = (-1500.0, 1500.0),
        ecg_render_delay_s: float = 0.03,
    ):
        self.charts = charts
        self.hr_window_sec = float(hr_window_sec)
        self.rr_window_sec = float(rr_window_sec)
        self.acc_window_sec = float(acc_window_sec)
        self.ecg_window_sec = float(ecg_window_sec)
        self.ecg_fixed_y_limits = (float(ecg_fixed_y_limits[0]), float(ecg_fixed_y_limits[1]))
        self.ecg_render_delay_s = float(ecg_render_delay_s)

        self.hr_points = deque(maxlen=1200)
        self.rr_points = deque(maxlen=1200)
        self.acc_points = deque(maxlen=200 * 20)
        self.ecg_points = deque(maxlen=130 * 20)
        self.ecg_ring = EcgRingBuffer(capacity=130 * 40)
        self._ecg_cached_t = np.empty(0, dtype=np.float64)
        self._ecg_cached_v = np.empty(0, dtype=np.float32)
        self._ecg_next_ts_mono = None
        self._ecg_last_ts_mono = None

        self._pending_hr = deque()
        self._pending_rr = deque()
        self._pending_acc = deque()
        self._playback_latency_s = 0.22
        self._playback_latency_hr_rr_s = 0.8

        self._hr_rr_motion_tail_s = 2.5
        self._ecg_motion_tail_s = 1.0
        self._hr_motion_anchor_t = None
        self._rr_motion_anchor_t = None
        self._ecg_motion_anchor_t = None
        self._hr_last_shift_s = 0.0
        self._rr_last_shift_s = 0.0
        self._ecg_last_shift_s = 0.0

        self._playback_offset_mono = {"hr": None, "rr": None, "acc": None}
        self._last_stream_event_mono = {"hr": None, "rr": None, "acc": None, "ecg": None}
        self._dirty_series = set()

    def reset_ecg_stream_state(self):
        self._ecg_last_ts_mono = None
        self.ecg_ring.clear()
        self.ecg_points.clear()
        self._ecg_cached_t = np.empty(0, dtype=np.float64)
        self._ecg_cached_v = np.empty(0, dtype=np.float32)
        self._ecg_next_ts_mono = None
        self._ecg_motion_anchor_t = None
        self._ecg_last_shift_s = 0.0
        self._last_stream_event_mono["ecg"] = None
        self._dirty_series.discard("ecg")

    def reset_playback_stream_state(self):
        self._pending_hr.clear()
        self._pending_rr.clear()
        self._pending_acc.clear()
        self._playback_offset_mono = {"hr": None, "rr": None, "acc": None}
        self._hr_motion_anchor_t = None
        self._rr_motion_anchor_t = None
        self._hr_last_shift_s = 0.0
        self._rr_last_shift_s = 0.0

    def enqueue_hr_sample(self, ts: float, hr_value: float, now_monotonic: float):
        _ = now_monotonic
        self._pending_hr.append((float(ts), float(hr_value)))
        self._last_stream_event_mono["hr"] = float(now_monotonic)

    def enqueue_rr_samples(self, packet_ts: float, rr_values, now_monotonic: float):
        if not rr_values:
            return
        t = float(packet_ts)
        unfolded = []
        for rr in reversed(list(rr_values)):
            rr_s = float(rr) / 1000.0
            unfolded.append((t, float(rr)))
            t -= rr_s
        _ = now_monotonic
        for ts, rr in reversed(unfolded):
            self._pending_rr.append((float(ts), float(rr)))
        self._last_stream_event_mono["rr"] = float(now_monotonic)

    def enqueue_ecg_chunk(self, end_ts: float, samples, sample_rate: float, now_monotonic: float):
        _ = end_ts
        if not samples:
            return
        sr = max(1e-3, float(sample_rate))
        step = 1.0 / sr
        if self._ecg_next_ts_mono is None:
            start_t = float(now_monotonic) - (len(samples) - 1) * step
            self._ecg_next_ts_mono = start_t
        for sample in samples:
            ts_mono = float(self._ecg_next_ts_mono)
            if self._ecg_last_ts_mono is not None and ts_mono <= float(self._ecg_last_ts_mono):
                ts_mono = float(self._ecg_last_ts_mono) + 1e-6
            self._ecg_last_ts_mono = ts_mono
            self._ecg_next_ts_mono = ts_mono + step
            self.ecg_ring.push(ts_mono, float(sample))
            self.ecg_points.append((ts_mono, float(sample)))
        self._dirty_series.add("ecg")
        self._last_stream_event_mono["ecg"] = float(now_monotonic)

    def enqueue_acc_chunk(self, end_ts: float, samples, sample_rate: float, now_monotonic: float):
        if not samples:
            return
        step = 1.0 / float(sample_rate)
        start_t = float(end_ts) - (len(samples) - 1) * step
        for i, sample in enumerate(samples):
            if isinstance(sample, (list, tuple)) and len(sample) >= 3:
                self._pending_acc.append((start_t + i * step, sample[0], sample[1], sample[2]))
        self._last_stream_event_mono["acc"] = float(now_monotonic)

    def drain_playback_streams(self, now_monotonic: float):
        if self._pending_hr:
            if self._playback_offset_mono["hr"] is None:
                first_ts = float(self._pending_hr[0][0])
                self._playback_offset_mono["hr"] = now_monotonic + self._playback_latency_hr_rr_s - first_ts
            offset = float(self._playback_offset_mono["hr"])
            moved = 0
            while self._pending_hr and (float(self._pending_hr[0][0]) + offset) <= now_monotonic:
                self.hr_points.append(self._pending_hr.popleft())
                moved += 1
            if moved:
                self._dirty_series.add("hr")

        if self._pending_rr:
            if self._playback_offset_mono["rr"] is None:
                first_ts = float(self._pending_rr[0][0])
                self._playback_offset_mono["rr"] = now_monotonic + self._playback_latency_hr_rr_s - first_ts
            offset = float(self._playback_offset_mono["rr"])
            moved = 0
            while self._pending_rr and (float(self._pending_rr[0][0]) + offset) <= now_monotonic:
                self.rr_points.append(self._pending_rr.popleft())
                moved += 1
            if moved:
                self._dirty_series.add("rr")

        if self._pending_acc:
            if self._playback_offset_mono["acc"] is None:
                first_ts = float(self._pending_acc[0][0])
                self._playback_offset_mono["acc"] = now_monotonic + self._playback_latency_s - first_ts
            offset = float(self._playback_offset_mono["acc"])
            moved = 0
            while self._pending_acc and (float(self._pending_acc[0][0]) + offset) <= now_monotonic:
                self.acc_points.append(self._pending_acc.popleft())
                moved += 1
            if moved:
                self._dirty_series.add("acc")

    def consume_dirty(self) -> set[str]:
        dirty = set(self._dirty_series)
        self._dirty_series.clear()
        return dirty

    def _is_stream_recent(self, stream_key: str, now_monotonic: float, horizon_s: float = 0.9) -> bool:
        last_evt = self._last_stream_event_mono.get(stream_key)
        if last_evt is None:
            return False
        return (float(now_monotonic) - float(last_evt)) < float(horizon_s)

    def should_motion_redraw(self, now_monotonic: float) -> bool:
        if self.ecg_points and self._is_stream_recent("ecg", now_monotonic):
            return True
        if self.hr_points and self._is_stream_recent("hr", now_monotonic, horizon_s=2.0):
            return True
        if self.rr_points and self._is_stream_recent("rr", now_monotonic, horizon_s=2.0):
            return True
        return False

    def _stream_display_head_ts(self, stream_key: str, points, now_monotonic: float) -> float | None:
        if not points:
            return None
        head = float(points[-1][0])
        offset = self._playback_offset_mono.get(stream_key)
        if offset is None:
            return head
        return max(head, float(now_monotonic) - float(offset))

    def _min_visible_shift_s(self, row_key: str, window_sec: float) -> float:
        try:
            vb_w = float(self.charts.rows[row_key].plot_widget.plotItem.vb.width())
        except Exception:
            vb_w = 0.0
        if vb_w <= 2.0:
            return max(0.005, float(window_sec) / 800.0)
        return max(0.0025, float(window_sec) / vb_w)

    def _slice_window(self, points, window_sec: float, head_t: float | None = None, right_tail_sec: float = 0.0):
        if not points:
            return [], []
        last_t = float(points[-1][0]) if head_t is None else float(head_t)
        t0 = last_t - float(window_sec)
        xs = []
        ys = []
        prev_t = None
        prev_y = None
        started = False
        for t_raw, y_raw in points:
            t = float(t_raw)
            y = float(y_raw)
            if (not started) and t >= t0:
                if prev_t is not None and t > prev_t:
                    a = (t0 - prev_t) / (t - prev_t)
                    a = max(0.0, min(1.0, a))
                    y_left = prev_y + (y - prev_y) * a
                else:
                    y_left = y
                xs.append(0.0)
                ys.append(y_left)
                started = True
            if started:
                x = t - t0
                if x > float(window_sec):
                    break
                if x >= 0.0:
                    xs.append(x)
                    ys.append(y)
            prev_t = t
            prev_y = y
        if not xs:
            return [], []
        right_x = float(window_sec)
        if xs[-1] < right_x:
            xs.append(right_x)
            ys.append(ys[-1])
        tail = max(0.0, float(right_tail_sec))
        if tail > 0.0:
            xs.append(right_x + tail)
            ys.append(ys[-1])
        return xs, ys

    def _slice_window_acc(self):
        if not self.acc_points:
            return [], [], [], []
        last_t = self.acc_points[-1][0]
        t0 = last_t - float(self.acc_window_sec)
        xs, yx, yy, yz = [], [], [], []
        for t, ax, ay, az in self.acc_points:
            if t < t0:
                continue
            xs.append(t - t0)
            yx.append(ax)
            yy.append(ay)
            yz.append(az)
        return xs, yx, yy, yz

    def redraw(self, now_monotonic: float, dirty: set[str] | None = None, motion_only: bool = False):
        dirty = dirty or set()
        hr_motion = bool(motion_only and self._is_stream_recent("hr", now_monotonic, horizon_s=2.0))
        rr_motion = bool(motion_only and self._is_stream_recent("rr", now_monotonic, horizon_s=2.0))

        if self.hr_points:
            if "hr" in dirty:
                head_t = self._stream_display_head_ts("hr", self.hr_points, now_monotonic)
                x, y = self._slice_window(self.hr_points, self.hr_window_sec, head_t=head_t, right_tail_sec=self._hr_rr_motion_tail_s)
                self.charts.set_hr(x, y, autoscale=True, set_x_range=True, reset_transform=True)
                self._hr_motion_anchor_t = head_t
                self._hr_last_shift_s = 0.0
            elif hr_motion and self._hr_motion_anchor_t is not None:
                head_t = self._stream_display_head_ts("hr", self.hr_points, now_monotonic)
                drift = float(head_t) - float(self._hr_motion_anchor_t)
                if drift >= (self._hr_rr_motion_tail_s * 0.8):
                    x, y = self._slice_window(self.hr_points, self.hr_window_sec, head_t=head_t, right_tail_sec=self._hr_rr_motion_tail_s)
                    self.charts.set_hr(x, y, autoscale=True, set_x_range=False, reset_transform=True)
                    self._hr_motion_anchor_t = head_t
                    self._hr_last_shift_s = 0.0
                else:
                    target = -drift
                    if abs(target - float(self._hr_last_shift_s)) >= self._min_visible_shift_s("HR", self.hr_window_sec):
                        self.charts.shift_hr_x(target)
                        self._hr_last_shift_s = target

        if self.rr_points:
            if "rr" in dirty:
                head_t = self._stream_display_head_ts("rr", self.rr_points, now_monotonic)
                x, y = self._slice_window(self.rr_points, self.rr_window_sec, head_t=head_t, right_tail_sec=self._hr_rr_motion_tail_s)
                self.charts.set_rr(x, y, autoscale=True, set_x_range=True, reset_transform=True)
                self._rr_motion_anchor_t = head_t
                self._rr_last_shift_s = 0.0
            elif rr_motion and self._rr_motion_anchor_t is not None:
                head_t = self._stream_display_head_ts("rr", self.rr_points, now_monotonic)
                drift = float(head_t) - float(self._rr_motion_anchor_t)
                if drift >= (self._hr_rr_motion_tail_s * 0.8):
                    x, y = self._slice_window(self.rr_points, self.rr_window_sec, head_t=head_t, right_tail_sec=self._hr_rr_motion_tail_s)
                    self.charts.set_rr(x, y, autoscale=True, set_x_range=False, reset_transform=True)
                    self._rr_motion_anchor_t = head_t
                    self._rr_last_shift_s = 0.0
                else:
                    target = -drift
                    if abs(target - float(self._rr_last_shift_s)) >= self._min_visible_shift_s("RR", self.rr_window_sec):
                        self.charts.shift_rr_x(target)
                        self._rr_last_shift_s = target

        if self.acc_points and ("acc" in dirty):
            x, yx, yy, yz = self._slice_window_acc()
            self.charts.set_acc(x, yx, yy, yz)

        ecg_recent = self._is_stream_recent("ecg", now_monotonic)
        ecg_dirty = ("ecg" in dirty)
        if self.ecg_ring.size >= 2 and (ecg_dirty or (motion_only and ecg_recent)):
            try:
                if ecg_dirty:
                    self._ecg_cached_t, self._ecg_cached_v = self.ecg_ring.snapshot()
                if self._ecg_cached_t.size < 2 or self._ecg_cached_v.size < 2:
                    return

                t_end = float(now_monotonic) - float(self.ecg_render_delay_s)
                if ecg_dirty:
                    t_start = t_end - float(self.ecg_window_sec)
                    margin = 0.35
                    keep = (self._ecg_cached_t >= (t_start - margin)) & (self._ecg_cached_t <= (t_end + margin))
                    if not np.any(keep):
                        return
                    t_vis = self._ecg_cached_t[keep]
                    v_vis = self._ecg_cached_v[keep]
                    if t_vis.size < 2 or v_vis.size < 2:
                        return

                    ecg_plot = self.charts.rows["ECG"].plot_widget
                    vb_w = int(ecg_plot.plotItem.vb.width())
                    if vb_w < 2:
                        return
                    window = float(self.ecg_window_sec)
                    scale = float(vb_w - 1) / max(1e-6, window)
                    dt = window / float(vb_w - 1)
                    t_end_q = np.floor(t_end / dt) * dt
                    tx = t_end_q - window + (np.arange(vb_w, dtype=np.float64) * dt)
                    y_cols = np.interp(tx, t_vis, v_vis, left=float(v_vis[0]), right=float(v_vis[-1]))
                    pos = np.empty((vb_w, 2), dtype=np.float32)
                    pos[:, 0] = (np.arange(vb_w, dtype=np.float32) / scale).astype(np.float32, copy=False)
                    pos[:, 1] = y_cols.astype(np.float32, copy=False)
                    tail = max(0.0, float(self._ecg_motion_tail_s))
                    if tail > 0.0:
                        pos = np.vstack((pos, np.array([[float(self.ecg_window_sec) + tail, float(pos[-1, 1])]], dtype=np.float32)))
                    self.charts.set_ecg_pos(
                        pos,
                        autoscale=True,
                        y_limits=self.ecg_fixed_y_limits,
                        x_limits=(0.0, self.ecg_window_sec),
                        set_x_range=True,
                        reset_transform=True,
                    )
                    self._ecg_motion_anchor_t = t_end
                    self._ecg_last_shift_s = 0.0
                elif motion_only and ecg_recent and self._ecg_motion_anchor_t is not None:
                    target = -(float(t_end) - float(self._ecg_motion_anchor_t))
                    min_shift = self._min_visible_shift_s("ECG", self.ecg_window_sec)
                    if abs(target - float(self._ecg_last_shift_s)) >= min_shift:
                        self.charts.shift_ecg_x(target)
                        self._ecg_last_shift_s = target
            except Exception:
                pass
