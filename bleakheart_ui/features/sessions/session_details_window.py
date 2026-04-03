from datetime import datetime
from typing import Any

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from bleakheart_ui.features.sessions.models import SessionSeries
from bleakheart_ui.features.sessions.ui_utils import format_duration, format_elapsed_tick_label, scaled_font
from bleakheart_ui.infra.session_repository import RR_FILE, SessionIndexRepository
from bleakheart_ui.shared.hr_zones import ZONE_NAMES, ZONE_PCTS


class _ElapsedTimeAxis(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        step = float(spacing if spacing is not None else 1.0)
        return [format_elapsed_tick_label(v, step) for v in values]


class _ZoneBarWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._percent = [0.0] * 5
        self._colors = ["#1d4ed8", "#0284c7", "#16a34a", "#f59e0b", "#dc2626"]
        self.setMinimumHeight(24)

    def set_percent(self, values: list[float]):
        vals = list(values or [])[:5]
        while len(vals) < 5:
            vals.append(0.0)
        self._percent = vals
        self.update()

    def paintEvent(self, event: QtGui.QPaintEvent):
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
        rect = self.rect().adjusted(1, 1, -1, -1)
        painter.fillRect(rect, QtGui.QColor("#0f172a"))
        x = rect.left()
        for idx, pct in enumerate(self._percent):
            w = rect.width() * max(0.0, float(pct)) / 100.0
            if w <= 0:
                continue
            piece = QtCore.QRectF(x, rect.top(), w, rect.height())
            painter.fillRect(piece, QtGui.QColor(self._colors[idx]))
            x += w


class _MetricTile(QtWidgets.QFrame):
    def __init__(self, title: str, parent=None, accent: str = "#38bdf8"):
        super().__init__(parent)
        self.setObjectName("metric_tile")
        self.setStyleSheet(
            f"""
            QFrame#metric_tile {{
                background-color: #0f172a;
                border: 1px solid #1f2937;
                border-top: 3px solid {accent};
                border-radius: 8px;
            }}
            """
        )
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(2)

        self.title_label = QtWidgets.QLabel(title, self)
        self.title_label.setFont(scaled_font(self.font(), 0.90, weight=QtGui.QFont.Medium))
        self.title_label.setStyleSheet("color:#7f91ab;")
        self.value_label = QtWidgets.QLabel("--", self)
        self.value_label.setFont(scaled_font(self.font(), 1.50, weight=QtGui.QFont.Bold))
        self.value_label.setStyleSheet("color:#e2e8f0;")
        self.unit_label = QtWidgets.QLabel("", self)
        self.unit_label.setFont(scaled_font(self.font(), 0.86, weight=QtGui.QFont.Medium))
        self.unit_label.setStyleSheet("color:#7dd3fc;")
        self.unit_label.setVisible(False)

        self.value_label.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        layout.addWidget(self.title_label)
        layout.addWidget(self.value_label)
        layout.addWidget(self.unit_label)
        self.setMinimumHeight(max(92, int(self.fontMetrics().lineSpacing() * 5.5)))

    def set_value(self, value: str, unit: str = ""):
        self.value_label.setText(value)
        self.unit_label.setText(unit)
        self.unit_label.setVisible(bool(unit.strip()))


class SessionDetailsWindow(QtWidgets.QDialog):
    def __init__(self, service: SessionIndexRepository, session_id: str, parent=None):
        super().__init__(parent)
        self.service = service
        self.session_id = session_id
        self.setWindowTitle("Session Details")
        self.resize(1260, 840)
        self._nav_sync = False
        self._plots: dict[str, dict[str, Any]] = {}
        self._zone_colors = ["#1d4ed8", "#0284c7", "#16a34a", "#f59e0b", "#dc2626"]
        self._zone_labels = [f"Z{i + 1} {ZONE_NAMES[i]}" for i in range(5)]
        self._zone_label_widgets: list[QtWidgets.QLabel] = []
        self._plot_left_axis_width = max(58, self.fontMetrics().horizontalAdvance("-4000") + 14)
        self._series: SessionSeries | None = None
        self._ecg_hires_window_s = 20.0
        self._ecg_hires_max_points = 26000
        self._ecg_last_window: tuple[float, float] | None = None
        self._rr_hires_window_s = 240.0
        self._rr_hires_max_points = 50000
        self._rr_last_window: tuple[float, float] | None = None
        self._ecg_refresh_timer = QtCore.QTimer(self)
        self._ecg_refresh_timer.setSingleShot(True)
        self._ecg_refresh_timer.setInterval(140)
        self._ecg_refresh_timer.timeout.connect(self._refresh_ecg_hires_if_needed)

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QtWidgets.QFrame(self)
        header.setObjectName("panel")
        grid = QtWidgets.QGridLayout(header)
        grid.setContentsMargins(10, 10, 10, 10)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)

        self.meta_tiles: dict[str, _MetricTile] = {}
        fields = [
            ("Date / Time", "date"),
            ("Duration", "duration"),
            ("Activity", "activity"),
            ("Profile", "profile"),
            ("Calories", "kcal"),
            ("HR Avg", "avg_hr"),
            ("HR Max", "max_hr"),
            ("HR Min", "min_hr"),
        ]
        tile_accents = {
            "date": "#38bdf8",
            "duration": "#60a5fa",
            "activity": "#38bdf8",
            "profile": "#38bdf8",
            "kcal": "#f59e0b",
            "avg_hr": "#22d3ee",
            "max_hr": "#06b6d4",
            "min_hr": "#34d399",
        }
        for idx, (title, key) in enumerate(fields):
            tile = _MetricTile(title, header, accent=tile_accents.get(key, "#38bdf8"))
            grid.addWidget(tile, idx // 4, idx % 4)
            self.meta_tiles[key] = tile
        root.addWidget(header)

        zone_wrap = QtWidgets.QFrame(self)
        zone_wrap.setObjectName("panel")
        zl = QtWidgets.QVBoxLayout(zone_wrap)
        zl.setContentsMargins(10, 10, 10, 10)
        zone_head = QtWidgets.QHBoxLayout()
        zone_title = QtWidgets.QLabel("HR Zones")
        zone_title.setFont(scaled_font(self.font(), 1.02, weight=QtGui.QFont.DemiBold))
        zone_head.addWidget(zone_title)
        zone_head.addStretch(1)
        self.zone_details_btn = QtWidgets.QToolButton(zone_wrap)
        self.zone_details_btn.setText("Show Details")
        self.zone_details_btn.setCheckable(True)
        self.zone_details_btn.toggled.connect(self._toggle_zone_details)
        zone_head.addWidget(self.zone_details_btn)
        zl.addLayout(zone_head)
        self.zone_bar = _ZoneBarWidget(zone_wrap)
        zl.addWidget(self.zone_bar)

        legend_row = QtWidgets.QHBoxLayout()
        legend_row.setSpacing(10)
        for idx, legend in enumerate(self._zone_labels):
            color_chip = QtWidgets.QLabel()
            color_chip.setFixedSize(12, 12)
            color_chip.setStyleSheet(
                f"background-color:{self._zone_colors[idx]};border:1px solid #0b1220;border-radius:6px;"
            )
            legend_lbl = QtWidgets.QLabel(legend)
            legend_lbl.setStyleSheet("color:#cbd5e1;")
            legend_lbl.setFont(scaled_font(self.font(), 0.90))
            legend_lbl.setToolTip(
                f"{legend}: {int(ZONE_PCTS[idx][0] * 100)}-{int(ZONE_PCTS[idx][1] * 100)}% of HR reserve."
            )
            self._zone_label_widgets.append(legend_lbl)
            block = QtWidgets.QHBoxLayout()
            block.setSpacing(4)
            block.addWidget(color_chip)
            block.addWidget(legend_lbl)
            holder = QtWidgets.QWidget(zone_wrap)
            holder.setLayout(block)
            legend_row.addWidget(holder)
        legend_row.addStretch(1)
        zl.addLayout(legend_row)

        self.zone_text = QtWidgets.QLabel("", zone_wrap)
        self.zone_text.setStyleSheet("color:#94a3b8;")
        self.zone_text.setFont(scaled_font(self.font(), 0.92))
        self.zone_text.setVisible(False)
        zl.addWidget(self.zone_text)
        root.addWidget(zone_wrap)

        chart_wrap = QtWidgets.QFrame(self)
        chart_wrap.setObjectName("panel")
        chart_layout = QtWidgets.QVBoxLayout(chart_wrap)
        chart_layout.setContentsMargins(8, 8, 8, 8)
        chart_layout.setSpacing(0)
        self.tabs = QtWidgets.QTabWidget(chart_wrap)
        self.tabs.setStyleSheet(
            """
            QTabWidget::pane {
                border: 1px solid #1f2937;
                border-top: 1px solid #334155;
                border-radius: 6px;
            }
            QTabBar::tab {
                background: #172033;
                color: #cbd5e1;
                border: 1px solid #334155;
                border-bottom: none;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                padding: 8px 18px;
                margin-right: 3px;
            }
            QTabBar::tab:selected {
                background: #5ecfff;
                color: #06111f;
                border-color: #7dd3fc;
                font-weight: 700;
            }
            QTabBar::tab:hover {
                background: #22314d;
                color: #f1f5f9;
            }
            """
        )
        corner = QtWidgets.QWidget(chart_wrap)
        corner_layout = QtWidgets.QHBoxLayout(corner)
        corner_layout.setContentsMargins(0, 0, 0, 0)
        corner_layout.setSpacing(6)
        ecg_mode_lbl = QtWidgets.QLabel("ECG detail mode")
        ecg_mode_lbl.setStyleSheet("color:#94a3b8;")
        ecg_mode_lbl.setFont(scaled_font(self.font(), 0.90, weight=QtGui.QFont.Medium))
        corner_layout.addWidget(ecg_mode_lbl)
        self.ecg_detail_box = QtWidgets.QComboBox(corner)
        # data tuple: (max_window_seconds_for_hires, max_points_in_hires_slice)
        self.ecg_detail_box.addItem("Performance (10s)", (10.0, 12000))
        self.ecg_detail_box.addItem("Balanced (20s)", (20.0, 26000))
        self.ecg_detail_box.addItem("Detailed (30s)", (30.0, 42000))
        self.ecg_detail_box.addItem("Max Detail (60s)", (60.0, 70000))
        self.ecg_detail_box.setCurrentIndex(1)
        self.ecg_detail_box.setToolTip(
            "Controls when and how much high-resolution ECG is loaded.\n"
            "Higher detail modes keep true ECG morphology over wider windows,\n"
            "but use more CPU/memory."
        )
        self.ecg_detail_box.currentIndexChanged.connect(self._on_ecg_detail_mode_changed)
        corner_layout.addWidget(self.ecg_detail_box)
        self.tabs.setCornerWidget(corner, QtCore.Qt.TopRightCorner)
        self.ecg_mode_lbl = ecg_mode_lbl
        chart_layout.addWidget(self.tabs, 1)
        root.addWidget(chart_wrap, 1)

        self._create_plot_tab("bpm", "BPM", "#22d3ee")
        self._create_plot_tab("rr", "RR", "#f59e0b")
        self._create_plot_tab("ecg", "ECG", "#f43f5e")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self._on_tab_changed(self.tabs.currentIndex())
        self._load()

    def _create_plot_tab(self, key: str, title: str, color: str):
        tab = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        detail_axis = _ElapsedTimeAxis(orientation="bottom")
        detail = pg.PlotWidget(tab, axisItems={"bottom": detail_axis})
        detail.setBackground("#0b1324")
        detail.showGrid(x=True, y=True, alpha=0.14)
        detail.setMouseEnabled(x=True, y=False)
        detail.setLabel("bottom", "Elapsed time")
        detail.getAxis("left").setWidth(int(self._plot_left_axis_width))
        curve_color = QtGui.QColor(color)
        curve = detail.plot(pen=pg.mkPen(curve_color, width=1.25))
        layout.addWidget(detail, 1)

        overview_axis = _ElapsedTimeAxis(orientation="bottom")
        overview = pg.PlotWidget(tab, axisItems={"bottom": overview_axis})
        overview.setBackground("#0b1324")
        overview.setFixedHeight(max(90, int(self.fontMetrics().lineSpacing() * 5.5)))
        overview.showGrid(x=False, y=False, alpha=0.0)
        overview.setMouseEnabled(x=False, y=False)
        overview.setMenuEnabled(False)
        overview.setLabel("bottom", "Elapsed time")
        overview.getAxis("left").setWidth(int(self._plot_left_axis_width))
        over_color = QtGui.QColor(color)
        over_color.setAlpha(190)
        over_curve = overview.plot(pen=pg.mkPen(over_color, width=1.0))
        region = pg.LinearRegionItem(values=[0.0, 60.0], brush=pg.mkBrush(56, 189, 248, 35))
        overview.addItem(region)
        layout.addWidget(overview)

        region.sigRegionChanged.connect(lambda _=None, k=key: self._on_nav_region_changed(k))
        detail.sigXRangeChanged.connect(lambda *_args, k=key: self._on_nav_plot_range_changed(k))
        self.tabs.addTab(tab, title)

        self._plots[key] = {
            "plot": detail,
            "curve": curve,
            "overview_plot": overview,
            "overview_curve": over_curve,
            "region": region,
            "x": np.array([], dtype=np.float64),
            "y": np.array([], dtype=np.float64),
            "base_x": np.array([], dtype=np.float64),
            "base_y": np.array([], dtype=np.float64),
            "hires_x": np.array([], dtype=np.float64),
            "hires_y": np.array([], dtype=np.float64),
            "hires_left": 0.0,
            "hires_right": 0.0,
            "xmin": 0.0,
            "xmax": 0.0,
        }

    def _on_nav_region_changed(self, key: str):
        if self._nav_sync:
            return
        self._nav_sync = True
        try:
            reg = self._plots[key]["region"]
            plot = self._plots[key]["plot"]
            xmin = float(self._plots[key]["xmin"])
            xmax = float(self._plots[key]["xmax"])
            min_x, max_x = reg.getRegion()
            span = max(1e-6, float(max_x) - float(min_x))
            min_x = max(xmin, float(min_x))
            max_x = min(xmax, float(max_x))
            if max_x <= min_x:
                max_x = min(xmax, min_x + span)
                min_x = max(xmin, max_x - span)
            self._sync_time_window(min_x, max_x, source_key=key)
        finally:
            self._nav_sync = False

    def _on_nav_plot_range_changed(self, key: str):
        if self._nav_sync:
            return
        self._nav_sync = True
        try:
            plot = self._plots[key]["plot"]
            reg = self._plots[key]["region"]
            rng = plot.viewRange()[0]
            xmin = float(self._plots[key]["xmin"])
            xmax = float(self._plots[key]["xmax"])
            left = max(xmin, float(rng[0]))
            right = min(xmax, float(rng[1]))
            if right <= left:
                right = min(xmax, left + 1.0)
                left = max(xmin, right - 1.0)
            self._sync_time_window(left, right, source_key=key)
        finally:
            self._nav_sync = False

    def _apply_time_window_to_key(self, key: str, left: float, right: float):
        refs = self._plots.get(key)
        if not refs:
            return
        xmin = float(refs.get("xmin", left))
        xmax = float(refs.get("xmax", right))
        l = max(xmin, float(left))
        r = min(xmax, float(right))
        if r <= l:
            r = min(xmax, l + 1.0)
            l = max(xmin, r - 1.0)
        refs["region"].setRegion((l, r))
        refs["plot"].setXRange(l, r, padding=0.0)
        if key == "ecg":
            self._update_ecg_curve_for_view(l, r)
        elif key == "rr":
            self._update_rr_curve_for_view(l, r)
        self._autoscale_visible_y(key)

    def _sync_time_window(self, left: float, right: float, *, source_key: str):
        self._apply_time_window_to_key(source_key, left, right)
        for key in self._plots.keys():
            if key == source_key:
                continue
            self._apply_time_window_to_key(key, left, right)

    def _apply_zone_legend_ranges(self, series: SessionSeries):
        ranges = list(series.zone_ranges_bpm or [])
        if len(ranges) < 5:
            ranges = [(0, 0)] * 5
        for i, lbl in enumerate(self._zone_label_widgets[:5]):
            lo, hi = ranges[i]
            pct_lo = int(ZONE_PCTS[i][0] * 100)
            pct_hi = int(ZONE_PCTS[i][1] * 100)
            text = f"Z{i + 1} {ZONE_NAMES[i]} ({lo}-{hi} bpm)"
            lbl.setText(text)
            lbl.setToolTip(f"{text} | {pct_lo}-{pct_hi}% HRR")

    def _load(self):
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        try:
            series = self.service.load_session_series(self.session_id)
        finally:
            QtWidgets.QApplication.restoreOverrideCursor()
        if series is None:
            QtWidgets.QMessageBox.warning(self, "Session", "Session not found in index.")
            self.close()
            return

        s = series.session
        self.meta_tiles["date"].set_value(datetime.fromtimestamp(s.start_ts).strftime("%Y-%m-%d %H:%M:%S"))
        self.meta_tiles["profile"].set_value(s.profile_id)
        self.meta_tiles["activity"].set_value(s.activity_name.title())
        self.meta_tiles["duration"].set_value(format_duration(s.duration_s))
        self.meta_tiles["kcal"].set_value(f"{s.kcal:.1f}", "kcal")
        self.meta_tiles["avg_hr"].set_value(f"{s.avg_hr_bpm:.1f}", "bpm")
        self.meta_tiles["max_hr"].set_value(f"{s.max_hr_bpm:.1f}", "bpm")
        self.meta_tiles["min_hr"].set_value(f"{s.min_hr_bpm:.1f}", "bpm")

        self._apply_zone_legend_ranges(series)
        self.zone_bar.set_percent(series.zones_percent)
        ranges = list(series.zone_ranges_bpm or [])
        while len(ranges) < 5:
            ranges.append((0, 0))
        zone_chunks = [
            f"Z{i + 1} {ranges[i][0]}-{ranges[i][1]}: "
            f"{format_duration(series.zones_seconds[i])} ({series.zones_percent[i]:.1f}%)"
            for i in range(5)
        ]
        self.zone_text.setText(
            "  |  ".join(zone_chunks) + f"  |  HR rest/max: {series.hr_rest_est:.0f}/{series.hr_max_est:.0f} bpm"
        )
        self.zone_bar.setToolTip(
            "\n".join(
                [
                    f"Z{i + 1} {ZONE_NAMES[i]} ({ranges[i][0]}-{ranges[i][1]} bpm): "
                    f"{series.zones_percent[i]:.1f}%"
                    for i in range(5)
                ]
            )
        )

        self._set_plot_data("bpm", series.bpm_t, series.bpm_v)
        self._set_plot_data("rr", series.rr_t, series.rr_v)
        self._set_plot_data("ecg", series.ecg_t, series.ecg_v)
        self._series = series
        self._rr_last_window = None
        self._ecg_last_window = None
        self._ecg_refresh_timer.start()

    def _set_plot_data(self, key: str, x: list[float], y: list[float]):
        refs = self._plots[key]
        x_arr = np.asarray(x, dtype=np.float64)
        y_arr = np.asarray(y, dtype=np.float64)
        refs["curve"].setData(x_arr, y_arr)
        refs["overview_curve"].setData(x_arr, y_arr)
        if not x:
            return
        left = float(x_arr[0])
        right = float(x_arr[-1])
        ymin = float(np.min(y_arr))
        ymax = float(np.max(y_arr))
        yspan = max(1e-3, ymax - ymin)
        ypad = yspan * 0.12
        ylow = ymin - ypad
        yhigh = ymax + ypad
        refs["x"] = x_arr
        refs["y"] = y_arr
        refs["base_x"] = x_arr
        refs["base_y"] = y_arr
        refs["hires_x"] = np.array([], dtype=np.float64)
        refs["hires_y"] = np.array([], dtype=np.float64)
        refs["hires_left"] = left
        refs["hires_right"] = left
        refs["xmin"] = left
        refs["xmax"] = right
        refs["ymin"] = ylow
        refs["ymax"] = yhigh
        refs["plot"].setLimits(xMin=left, xMax=right)
        refs["plot"].setLimits(yMin=ylow, yMax=yhigh)
        refs["overview_plot"].setLimits(xMin=left, xMax=right)
        refs["overview_plot"].setLimits(yMin=ylow, yMax=yhigh)
        refs["overview_plot"].setYRange(ylow, yhigh, padding=0.0)
        width = max(60.0, (right - left) * 0.18)
        refs["region"].setRegion((left, min(right, left + width)))
        refs["plot"].setXRange(left, min(right, left + width), padding=0.0)
        self._autoscale_visible_y(key)

    def _autoscale_visible_y(self, key: str):
        refs = self._plots[key]
        x = refs.get("x")
        y = refs.get("y")
        if x is None or y is None or len(x) == 0 or len(y) == 0:
            return
        plot = refs["plot"]
        x_range = plot.viewRange()[0]
        left = float(x_range[0])
        right = float(x_range[1])
        if right <= left:
            return

        start = int(np.searchsorted(x, left, side="left"))
        end = int(np.searchsorted(x, right, side="right"))
        if end <= start:
            idx = min(max(0, start), len(y) - 1)
            ymin = float(y[idx])
            ymax = float(y[idx])
        else:
            y_seg = y[start:end]
            ymin = float(np.min(y_seg))
            ymax = float(np.max(y_seg))
        span = max(1e-3, ymax - ymin)
        pad = span * 0.12
        ylow = max(float(refs.get("ymin", ymin - pad)), ymin - pad)
        yhigh = min(float(refs.get("ymax", ymax + pad)), ymax + pad)
        if yhigh <= ylow:
            yhigh = ylow + 1.0
        plot.setYRange(ylow, yhigh, padding=0.0)

    def _toggle_zone_details(self, checked: bool):
        self.zone_text.setVisible(bool(checked))
        self.zone_details_btn.setText("Hide Details" if checked else "Show Details")

    def _set_ecg_base_curve(self):
        refs = self._plots.get("ecg")
        if not refs:
            return
        base_x = refs.get("base_x")
        base_y = refs.get("base_y")
        if base_x is None or base_y is None or len(base_x) == 0:
            return
        refs["curve"].setData(base_x, base_y)
        refs["x"] = base_x
        refs["y"] = base_y

    def _set_rr_base_curve(self):
        refs = self._plots.get("rr")
        if not refs:
            return
        base_x = refs.get("base_x")
        base_y = refs.get("base_y")
        if base_x is None or base_y is None or len(base_x) == 0:
            return
        refs["curve"].setData(base_x, base_y)
        refs["x"] = base_x
        refs["y"] = base_y

    def _curve_has_visible_samples(self, key: str, left: float, right: float) -> bool:
        refs = self._plots.get(key)
        if not refs:
            return False
        x_vals = refs.get("x")
        if x_vals is None or len(x_vals) == 0:
            return False
        i0 = int(np.searchsorted(x_vals, float(left), side="left"))
        i1 = int(np.searchsorted(x_vals, float(right), side="right"))
        return i1 > i0

    def _use_cached_ecg_hires(self, left: float, right: float) -> bool:
        refs = self._plots.get("ecg")
        if not refs:
            return False
        hires_x = refs.get("hires_x")
        hires_y = refs.get("hires_y")
        if hires_x is None or hires_y is None or len(hires_x) == 0:
            return False
        if float(left) < float(refs.get("hires_left", 0.0)) or float(right) > float(refs.get("hires_right", 0.0)):
            return False
        i0 = int(np.searchsorted(hires_x, float(left), side="left"))
        i1 = int(np.searchsorted(hires_x, float(right), side="right"))
        if i1 <= i0:
            return False
        view_x = hires_x[i0:i1]
        view_y = hires_y[i0:i1]
        refs["curve"].setData(view_x, view_y)
        refs["x"] = view_x
        refs["y"] = view_y
        return True

    def _on_ecg_detail_mode_changed(self, _index: int):
        data = self.ecg_detail_box.currentData()
        threshold = 20.0
        max_points = 26000
        if isinstance(data, (tuple, list)) and len(data) >= 2:
            try:
                threshold = float(data[0])
                max_points = int(data[1])
            except Exception:
                pass
        else:
            try:
                threshold = float(data)
            except Exception:
                pass
        self._ecg_hires_window_s = max(5.0, threshold)
        self._ecg_hires_max_points = max(5000, max_points)
        self._ecg_last_window = None
        self._ecg_refresh_timer.start()

    def _on_tab_changed(self, index: int):
        txt = self.tabs.tabText(index).strip().lower() if index >= 0 else ""
        show = (txt == "ecg")
        self.ecg_mode_lbl.setVisible(show)
        self.ecg_detail_box.setVisible(show)

    def _refresh_ecg_hires_if_needed(self):
        series = self._series
        if series is None:
            return
        refs = self._plots.get("ecg")
        if not refs:
            return
        plot = refs["plot"]
        x_range = plot.viewRange()[0]
        left = float(x_range[0])
        right = float(x_range[1])
        if right <= left:
            return
        self._update_ecg_curve_for_view(left, right)
        self._autoscale_visible_y("ecg")

    def _update_rr_curve_for_view(self, left: float, right: float):
        series = self._series
        if series is None:
            return
        refs = self._plots.get("rr")
        if not refs:
            return
        width = max(1e-6, float(right) - float(left))
        if width > float(self._rr_hires_window_s):
            self._set_rr_base_curve()
            self._rr_last_window = None
            return

        prev = self._rr_last_window
        if prev is not None and abs(prev[0] - left) < 0.02 and abs(prev[1] - right) < 0.02:
            return

        raw_x, raw_y = self.service.load_signal_window(
            self.session_id,
            kind="rr",
            filename=RR_FILE,
            x_key="timestamp_s",
            y_key="rr_ms",
            zero_at_ts=series.zero_at_ts,
            start_s=left,
            end_s=right,
            max_points=int(self._rr_hires_max_points),
            preserve_peaks=False,
            include_neighbors=True,
        )
        if not raw_x:
            return
        arr_x = np.asarray(raw_x, dtype=np.float64)
        arr_y = np.asarray(raw_y, dtype=np.float64)
        refs["curve"].setData(arr_x, arr_y)
        refs["x"] = arr_x
        refs["y"] = arr_y
        self._rr_last_window = (left, right)

    def _update_ecg_curve_for_view(self, left: float, right: float):
        series = self._series
        if series is None:
            return
        refs = self._plots.get("ecg")
        if not refs:
            return
        width = max(1e-6, float(right) - float(left))

        # Use base decimated ECG for wider windows.
        if width > float(self._ecg_hires_window_s):
            self._set_ecg_base_curve()
            self._ecg_last_window = None
            return

        # Reuse cached hires coverage while panning/zooming.
        if self._use_cached_ecg_hires(left, right):
            self._ecg_last_window = (left, right)
            return

        prev = self._ecg_last_window
        if prev is not None and abs(prev[0] - left) < 0.02 and abs(prev[1] - right) < 0.02:
            return

        pad = width * 1.5
        load_left = max(float(refs.get("xmin", 0.0)), float(left) - pad)
        load_right = min(float(refs.get("xmax", right)), float(right) + pad)
        raw_x, raw_y = self.service.load_ecg_window(
            self.session_id,
            zero_at_ts=series.zero_at_ts,
            start_s=load_left,
            end_s=load_right,
            max_points=int(self._ecg_hires_max_points),
        )
        if not raw_x:
            return
        hires_x = np.asarray(raw_x, dtype=np.float64)
        hires_y = np.asarray(raw_y, dtype=np.float64)
        refs["hires_x"] = hires_x
        refs["hires_y"] = hires_y
        refs["hires_left"] = float(hires_x[0])
        refs["hires_right"] = float(hires_x[-1])
        if not self._use_cached_ecg_hires(left, right):
            refs["curve"].setData(hires_x, hires_y)
            refs["x"] = hires_x
            refs["y"] = hires_y
        self._ecg_last_window = (left, right)
