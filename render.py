import time

import numpy as np
from PySide6 import QtCore, QtGui, QtWidgets
import pyqtgraph as pg


class _PgChartRow(QtWidgets.QFrame):
    def __init__(
        self,
        parent,
        *,
        title: str,
        y_label: str,
        window_sec: float,
        line_specs,
        y_default,
        downsample_auto: bool = True,
        downsample_mode: str = "peak",
        line_antialias: bool = False,
        show_x_grid: bool = True,
        show_y_grid: bool = True,
        autoscale_min_interval_s: float = 0.0,
        autoscale_hysteresis_frac: float = 0.0,
    ):
        super().__init__(parent)
        self.window_sec = float(window_sec)
        self.y_default = (float(y_default[0]), float(y_default[1]))
        self.lines = {}
        self.autoscale_min_interval_s = float(max(0.0, autoscale_min_interval_s))
        self.autoscale_hysteresis_frac = float(max(0.0, autoscale_hysteresis_frac))
        self._last_y_autoscale_mono = 0.0

        self.setStyleSheet("QFrame { background-color: #0b1324; border: 1px solid #1f2a44; border-radius: 4px; }")
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 6)
        layout.setSpacing(4)

        title_label = QtWidgets.QLabel(title, self)
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        title_label.setStyleSheet("QLabel { color: #cbd5e1; background: transparent; border: 0px; font-size: 12pt; font-weight: 600; }")
        layout.addWidget(title_label, 0)

        self.plot_widget = pg.PlotWidget(self)
        self.plot_widget.setBackground("#0b1324")
        self.plot_widget.showGrid(x=bool(show_x_grid), y=bool(show_y_grid), alpha=0.2)
        left_axis = self.plot_widget.getAxis("left")
        bottom_axis = self.plot_widget.getAxis("bottom")
        left_axis.setLabel(text=y_label, **{"color": "#dbe5f4", "font-size": "13pt", "font-weight": "600"})
        bottom_axis.setLabel(text="seconds", **{"color": "#dbe5f4", "font-size": "12pt", "font-weight": "500"})
        left_axis.setTextPen(pg.mkPen("#dbe5f4"))
        bottom_axis.setTextPen(pg.mkPen("#dbe5f4"))
        left_axis.setPen(pg.mkPen("#8fa2be"))
        bottom_axis.setPen(pg.mkPen("#8fa2be"))
        tick_font = QtGui.QFont("Segoe UI", 13)
        left_axis.setTickFont(tick_font)
        bottom_axis.setTickFont(tick_font)
        left_axis.setStyle(tickTextOffset=8, autoExpandTextSpace=False)
        bottom_axis.setStyle(tickTextOffset=8, autoExpandTextSpace=True)
        left_axis.setWidth(84)
        self.plot_widget.setXRange(0.0, self.window_sec, padding=0.0)
        self.plot_widget.setYRange(self.y_default[0], self.y_default[1], padding=0.0)
        self.plot_widget.setDownsampling(auto=bool(downsample_auto), mode=str(downsample_mode))
        self.plot_widget.setClipToView(True)
        layout.addWidget(self.plot_widget, 1)

        for key, color, width in line_specs:
            curve = self.plot_widget.plot(
                x=np.array([0.0, self.window_sec], dtype=np.float32),
                y=np.array([0.0, 0.0], dtype=np.float32),
                pen=pg.mkPen(color=color, width=float(width)),
                antialias=bool(line_antialias),
            )
            curve.setClipToView(True)
            curve.setDownsampling(auto=bool(downsample_auto), method=str(downsample_mode))
            curve.setSkipFiniteCheck(True)
            self.lines[key] = curve

    def _auto_y_bounds(self, ys: list[np.ndarray]) -> tuple[float, float]:
        if not ys:
            return self.y_default
        y = np.concatenate([a[np.isfinite(a)] for a in ys if a.size > 0], axis=0) if ys else np.array([], dtype=np.float32)
        if y.size == 0:
            return self.y_default
        ymin = float(np.min(y))
        ymax = float(np.max(y))
        span = max(1.0, ymax - ymin)
        margin = max(5.0, span * 0.2)
        return ymin - margin, ymax + margin

    def _maybe_apply_y_range(self, ymin: float, ymax: float):
        now = time.monotonic()
        if self.autoscale_min_interval_s > 0.0:
            if (now - float(self._last_y_autoscale_mono)) < self.autoscale_min_interval_s:
                return
        if self.autoscale_hysteresis_frac > 0.0:
            try:
                cur = self.plot_widget.viewRange()[1]
                c0 = float(cur[0])
                c1 = float(cur[1])
                cspan = max(1e-6, c1 - c0)
                lo_ok = ymin >= (c0 - cspan * self.autoscale_hysteresis_frac)
                hi_ok = ymax <= (c1 + cspan * self.autoscale_hysteresis_frac)
                if lo_ok and hi_ok:
                    return
            except Exception:
                pass
        self.plot_widget.setYRange(float(ymin), float(ymax), padding=0.0)
        self._last_y_autoscale_mono = now

    def set_data(self, series: dict[str, tuple[list[float], list[float]]], *, autoscale: bool = True, y_limits: tuple[float, float] | None = None):
        y_arrays = []
        for line_key, (x, y) in series.items():
            xx = np.asarray(x, dtype=np.float32)
            yy = np.asarray(y, dtype=np.float32)
            if xx.size == 0 or yy.size == 0:
                xx = np.array([0.0, self.window_sec], dtype=np.float32)
                yy = np.array([0.0, 0.0], dtype=np.float32)
            self.lines[line_key].setData(xx, yy)
            y_arrays.append(yy)
        self.plot_widget.setXRange(0.0, self.window_sec, padding=0.0)
        if autoscale:
            ymin, ymax = (float(y_limits[0]), float(y_limits[1])) if y_limits is not None else self._auto_y_bounds(y_arrays)
            self._maybe_apply_y_range(ymin, ymax)

    def set_line_data(
        self,
        line_key: str,
        x,
        y,
        *,
        autoscale: bool = True,
        y_limits: tuple[float, float] | None = None,
        set_x_range: bool = True,
        reset_transform: bool = True,
    ):
        xx = np.asarray(x, dtype=np.float32)
        yy = np.asarray(y, dtype=np.float32)
        if xx.size == 0 or yy.size == 0:
            xx = np.array([0.0, self.window_sec], dtype=np.float32)
            yy = np.array([0.0, 0.0], dtype=np.float32)
        line = self.lines[line_key]
        line.setData(xx, yy)
        if reset_transform:
            line.setPos(0.0, 0.0)
        if set_x_range:
            self.plot_widget.setXRange(0.0, self.window_sec, padding=0.0)
        if autoscale:
            ymin, ymax = (float(y_limits[0]), float(y_limits[1])) if y_limits is not None else self._auto_y_bounds([yy])
            self._maybe_apply_y_range(ymin, ymax)

    def shift_line_x(self, line_key: str, dx: float):
        self.lines[line_key].setPos(float(dx), 0.0)

    def set_line_pos(
        self,
        line_key: str,
        pos: np.ndarray,
        *,
        autoscale: bool = True,
        y_limits: tuple[float, float] | None = None,
        x_limits: tuple[float, float] | None = None,
        set_x_range: bool = True,
        reset_transform: bool = True,
    ):
        arr = np.asarray(pos, dtype=np.float32)
        if arr.ndim != 2 or arr.shape[1] != 2 or arr.shape[0] == 0:
            arr = np.array([[0.0, 0.0], [self.window_sec, 0.0]], dtype=np.float32)
        line = self.lines[line_key]
        line.setData(arr[:, 0], arr[:, 1])
        if reset_transform:
            line.setPos(0.0, 0.0)
        if set_x_range:
            if x_limits is None:
                self.plot_widget.setXRange(0.0, self.window_sec, padding=0.0)
            else:
                self.plot_widget.setXRange(float(x_limits[0]), float(x_limits[1]), padding=0.0)
        if autoscale:
            if y_limits is not None:
                ymin, ymax = float(y_limits[0]), float(y_limits[1])
            else:
                ymin, ymax = self._auto_y_bounds([arr[:, 1]])
            self._maybe_apply_y_range(ymin, ymax)


class QtGraphCharts(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.order = ("HR", "RR", "ACC", "ECG")
        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(8)

        self.rows = {
            "HR": _PgChartRow(self, title="Heart Rate (bpm)", y_label="BPM", window_sec=60.0, line_specs=[("hr", "#38bdf8", 1.5)], y_default=(0.0, 200.0), downsample_auto=False, downsample_mode="subsample", show_x_grid=False, show_y_grid=True, autoscale_min_interval_s=0.0, autoscale_hysteresis_frac=0.0),
            "RR": _PgChartRow(self, title="RR Interval (ms)", y_label="ms", window_sec=60.0, line_specs=[("rr", "#f59e0b", 1.3)], y_default=(200.0, 1400.0), downsample_auto=False, downsample_mode="subsample", show_x_grid=False, show_y_grid=True, autoscale_min_interval_s=0.0, autoscale_hysteresis_frac=0.0),
            "ACC": _PgChartRow(self, title="ACC X/Y/Z (mG)", y_label="mG", window_sec=20.0, line_specs=[("x", "#22d3ee", 1.0), ("y", "#a78bfa", 1.0), ("z", "#34d399", 1.0)], y_default=(-1500.0, 1500.0), downsample_auto=True, downsample_mode="peak"),
            "ECG": _PgChartRow(self, title="ECG (uV)", y_label="uV", window_sec=20.0, line_specs=[("ecg", "#f43f5e", 1.5)], y_default=(-1500.0, 1500.0), downsample_auto=False, downsample_mode="subsample", line_antialias=False),
        }
        for key in self.order:
            self.layout.addWidget(self.rows[key], 1)

    def set_active_keys(self, keys):
        active = set(keys or [])
        if not active:
            active = {"HR"}
        for idx, key in enumerate(self.order):
            visible = key in active
            self.rows[key].setVisible(visible)
            self.layout.setStretch(idx, 1 if visible else 0)

    def set_hr(self, x, y, *, autoscale: bool = True, set_x_range: bool = True, reset_transform: bool = True):
        self.rows["HR"].set_line_data(
            "hr",
            x,
            y,
            autoscale=autoscale,
            set_x_range=set_x_range,
            reset_transform=reset_transform,
        )

    def shift_hr_x(self, dx: float):
        self.rows["HR"].shift_line_x("hr", dx)

    def set_rr(self, x, y, *, autoscale: bool = True, set_x_range: bool = True, reset_transform: bool = True):
        self.rows["RR"].set_line_data(
            "rr",
            x,
            y,
            autoscale=autoscale,
            set_x_range=set_x_range,
            reset_transform=reset_transform,
        )

    def shift_rr_x(self, dx: float):
        self.rows["RR"].shift_line_x("rr", dx)

    def set_acc(self, x, yx, yy, yz):
        self.rows["ACC"].set_data({"x": (x, yx), "y": (x, yy), "z": (x, yz)})

    def set_ecg_pos(
        self,
        pos: np.ndarray,
        *,
        autoscale: bool = True,
        y_limits: tuple[float, float] | None = None,
        x_limits: tuple[float, float] | None = None,
        set_x_range: bool = True,
        reset_transform: bool = True,
    ):
        self.rows["ECG"].set_line_pos(
            "ecg",
            pos,
            autoscale=autoscale,
            y_limits=y_limits,
            x_limits=x_limits,
            set_x_range=set_x_range,
            reset_transform=reset_transform,
        )

    def shift_ecg_x(self, dx: float):
        self.rows["ECG"].shift_line_x("ecg", dx)

    def gl_info(self):
        return None, None, "pyqtgraph active"
