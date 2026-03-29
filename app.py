import queue
import signal
import sys
import time
from collections import deque
import os
import json
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets
import pyqtgraph as pg

from engine import BleakHeartEngine, PMD_TYPES, RecordingConfig
from render import QtGraphCharts
from render_controller import RenderController


ACTIVITY_OPTIONS = [
    "Rest",
    "Elliptical",
    "Walking",
    "Running",
    "Cycling",
    "Strength Training",
    "Other",
]

ACTIVITY_FACTOR = {
    "Rest": 0.7,
    "Elliptical": 1.0,
    "Walking": 0.9,
    "Running": 1.08,
    "Cycling": 1.0,
    "Strength Training": 0.92,
    "Other": 1.0,
}

DEFAULT_PROFILE = {
    "name": "Participant 001",
    "sex": "male",
    "age_years": 30,
    "weight_kg": 75.0,
    "height_cm": 175.0,
    "hr_rest": 60,
    "hr_max": 190,
}

PMD_HELP = {
    "ECG": "Electrocardiogram. Electrical heart signal in microvolts (uV).",
    "ACC": "Accelerometer. Motion signal on X/Y/Z axes (milli-g).",
    "PPG": "Photoplethysmography. Optical pulse waveform from supported Polar sensors.",
    "PPI": "Peak-to-peak interval. Beat interval frames from supported sensors.",
    "GYRO": "Gyroscope. Angular velocity stream.",
    "MAG": "Magnetometer. Magnetic field stream.",
}

SDK_HELP = (
    "Enable SDK mode on devices that support it (for example Polar Verity) "
    "to unlock additional measurement options."
)

HR_HELP = {
    "base": "Enable live Heart Rate and RR interval data from the standard BLE Heart Rate service.",
    "instant": "When enabled, heart rate is computed from each RR interval (beat-to-beat) instead of using the device average in the frame.",
    "unpack": "When enabled, multi-beat HR frames are split into individual beats so RR/HR timestamps are per-beat.",
}

# Rendering/perf tuning (kept centralized for safe A/B changes).
ECG_RENDER_DELAY_S = 0.030


class EngineEventPump(QtCore.QObject):
    event_received = QtCore.Signal(object)
    finished = QtCore.Signal()

    def __init__(self, events_queue):
        super().__init__()
        self._events = events_queue
        self._running = False

    @QtCore.Slot()
    def run(self):
        self._running = True
        while self._running:
            try:
                event = self._events.get(timeout=0.05)
            except queue.Empty:
                continue
            self.event_received.emit(event)
        self.finished.emit()

    @QtCore.Slot()
    def stop(self):
        self._running = False


class WideClickCheckBox(QtWidgets.QCheckBox):
    def hitButton(self, pos: QtCore.QPoint) -> bool:
        return self.rect().contains(pos)


QSS_THEME = """
QWidget {
    background-color: #0b1220;
    color: #e5e7eb;
    font-size: 13px;
}
QFrame#panel {
    background-color: #111827;
    border: 1px solid #1f2937;
    border-radius: 8px;
}
QLabel#section {
    font-weight: 700;
    color: #e5e7eb;
    padding-top: 4px;
    padding-bottom: 2px;
}
QPushButton {
    background-color: #1f2937;
    color: #e5e7eb;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 6px 10px;
}
QPushButton:hover {
    background-color: #273449;
}
QPushButton:disabled {
    color: #93a4ba;
    background-color: #1a2535;
}
QCheckBox {
    spacing: 8px;
}
QCheckBox:disabled {
    color: #7b8799;
}
QCheckBox::indicator:disabled {
    border: 1px solid #334155;
    background-color: #0f172a;
}
QCheckBox::indicator:checked:disabled {
    background-color: #1e3a8a;
    border: 1px solid #334155;
}
QLineEdit, QComboBox, QListWidget, QPlainTextEdit {
    background-color: #0f172a;
    border: 1px solid #1f2937;
    border-radius: 6px;
    padding: 6px;
}
QListWidget::item:selected {
    background-color: #1d4ed8;
}
"""


class QtBleakHeartQtGraphUI(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BleakHeart Recorder (Qt + pyqtgraph)")
        app = QtWidgets.QApplication.instance()
        if app is not None and app.primaryScreen() is not None:
            avail = app.primaryScreen().availableGeometry()
            target_w = min(1580, max(980, int(avail.width() - 48)))
            target_h = min(920, max(720, int(avail.height() - 64)))
            self.resize(target_w, target_h)
        else:
            self.resize(1400, 860)

        self.events = queue.Queue()
        self.engine = BleakHeartEngine(self.events)
        self.settings_path = Path(__file__).resolve().parent / "qt_ui_settings.json"
        self._save_timer = QtCore.QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._save_settings_now)

        self.devices = []
        self.connected = False
        self.recording = False
        self.recording_paused = False
        self._is_shutting_down = False
        self.available_measurements = set()
        self.last_device_address = None
        self.last_device_name = None
        self.profiles = {"participant_001": dict(DEFAULT_PROFILE)}
        self.selected_profile_id = "participant_001"
        self.body_profile = dict(DEFAULT_PROFILE)
        self.current_session_path = None
        self.kcal_total = 0.0
        self._kcal_last_t = None
        self._kcal_timeline = []
        self._live_battery_value = None
        self._live_hr_value = None
        self._live_rr_value = None
        self._record_elapsed_s = 0.0
        self._record_last_resume_mono = None
        self.battery_poll_interval_ms = 90000
        self.battery_poll_inflight = False
        self.sidebar_collapsed = False
        self.auto_collapse_sidebar_on_record = True
        self._last_sidebar_width = 420
        self.battery_poll_timer = QtCore.QTimer(self)
        self.battery_poll_timer.setInterval(self.battery_poll_interval_ms)
        self.battery_poll_timer.timeout.connect(self._battery_poll_tick)
        self.session_timer = QtCore.QTimer(self)
        self.session_timer.setInterval(250)
        self.session_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.session_timer.timeout.connect(self._session_tick)
        self.session_timer.start()

        self.render = None
        self._pending_calls = []
        self.display_refresh_hz = self._detect_display_refresh_rate()
        self.render_fps_mode = "auto"
        self.render_fps_manual = 120
        self.render_fps = self._effective_render_fps()
        self._next_refresh_probe_due = 0.0
        self._fps_last_mono = time.monotonic()
        self._fps_frames = 0
        self._fps_value = 0.0
        self._fps_timer_ticks = 0
        self._fps_tick_value = 0.0
        self._log_buffer = deque()
        self._last_applied_live_cfg = None
        self._connect_requested_live_cfg = None

        self._build_ui()
        self.render = RenderController(self.charts, ecg_render_delay_s=ECG_RENDER_DELAY_S)
        self.setAttribute(QtCore.Qt.WA_AlwaysShowToolTips, True)
        self._apply_qss()
        self._apply_selected_profile()
        self._load_settings()
        self._last_live_state = self._current_live_state()
        self._refresh_live_labels()
        QtCore.QTimer.singleShot(0, self._log_gpu_info)
        QtCore.QTimer.singleShot(450, self._auto_connect_on_startup)

        self.event_thread = QtCore.QThread(self)
        self.event_pump = EngineEventPump(self.events)
        self.event_pump.moveToThread(self.event_thread)
        self.event_thread.started.connect(self.event_pump.run)
        self.event_pump.event_received.connect(self._handle_engine_event, QtCore.Qt.QueuedConnection)
        self.event_pump.finished.connect(self.event_thread.quit)
        self.event_thread.start()

        self.future_timer = QtCore.QTimer(self)
        self.future_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.future_timer.setInterval(50)
        self.future_timer.timeout.connect(self._resolve_futures)
        self.future_timer.start()

        self.log_flush_timer = QtCore.QTimer(self)
        self.log_flush_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.log_flush_timer.setInterval(120)
        self.log_flush_timer.timeout.connect(self._flush_log_buffer)
        self.log_flush_timer.start()

        self.event_timer = QtCore.QTimer(self)
        self.event_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.event_timer.setInterval(12)
        self.event_timer.timeout.connect(self._event_tick)
        self.event_timer.start()

        self.render_timer = QtCore.QTimer(self)
        self.render_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.render_timer.setInterval(max(1, round(1000.0 / float(self.render_fps))))
        self.render_timer.timeout.connect(self._render_tick)
        self.render_timer.start()

    def _build_ui(self):
        root = QtWidgets.QWidget(self)
        self.setCentralWidget(root)

        main_layout = QtWidgets.QVBoxLayout(root)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(12)

        controls = QtWidgets.QFrame(root)
        self.controls_panel = controls
        controls.setObjectName("panel")
        controls.setMinimumWidth(360)
        left = QtWidgets.QVBoxLayout(controls)
        left.setContentsMargins(12, 12, 12, 12)
        left.setSpacing(8)

        scan_row = QtWidgets.QHBoxLayout()
        self.scan_btn = QtWidgets.QPushButton("Scan")
        self.scan_btn.clicked.connect(self._scan)
        self.scan_btn.setToolTip("Scan nearby Polar BLE devices.")
        self.connect_btn = QtWidgets.QPushButton("Connect")
        self.connect_btn.clicked.connect(self._connect_selected)
        self.connect_btn.setToolTip("Connect to selected device or disconnect current one.")
        scan_row.addWidget(self.scan_btn)
        scan_row.addWidget(self.connect_btn)
        left.addLayout(scan_row)

        self.device_list = QtWidgets.QListWidget()
        self.device_list.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        left.addWidget(self.device_list, 1)

        self.hr_enabled = WideClickCheckBox("Heart Rate + RR")
        self.hr_enabled.setChecked(True)
        self.hr_enabled.toggled.connect(self._on_measurement_toggle)
        self.hr_enabled.setToolTip(HR_HELP["base"])
        left.addWidget(self.hr_enabled)

        self.hr_instant = WideClickCheckBox("Instant HR (RR-derived)")
        self.hr_instant.setChecked(True)
        self.hr_instant.setToolTip(HR_HELP["instant"])
        self.hr_instant.toggled.connect(lambda _v: self._save_settings())
        left.addWidget(self.hr_instant)

        self.hr_unpack = WideClickCheckBox("Unpack multi-beat HR")
        self.hr_unpack.setChecked(True)
        self.hr_unpack.setToolTip(HR_HELP["unpack"])
        self.hr_unpack.toggled.connect(lambda _v: self._save_settings())
        left.addWidget(self.hr_unpack)

        self.sdk_mode = WideClickCheckBox("SDK mode (if available)")
        self.sdk_mode.setToolTip(SDK_HELP)
        self.sdk_mode.toggled.connect(lambda _v: self._save_settings())
        left.addWidget(self.sdk_mode)

        left.addWidget(self._section_label("Live PMD Measurements"))
        self.meas_checks = {}
        for meas in PMD_TYPES:
            chk = WideClickCheckBox(meas)
            chk.toggled.connect(self._on_measurement_toggle)
            chk.setToolTip(PMD_HELP.get(meas, meas))
            chk.setAttribute(QtCore.Qt.WA_AlwaysShowToolTips, True)
            self.meas_checks[meas] = chk
            left.addWidget(chk)

        left.addWidget(self._section_label("Session"))
        profile_row = QtWidgets.QHBoxLayout()
        profile_row.addWidget(QtWidgets.QLabel("Profile"))
        self.profile_box = QtWidgets.QComboBox()
        self.profile_box.setToolTip("Select active body profile used for recording metadata and kcal estimation.")
        self.profile_box.currentTextChanged.connect(self._on_profile_selected)
        profile_row.addWidget(self.profile_box, 1)
        self.manage_profiles_btn = QtWidgets.QPushButton("Manage")
        self.manage_profiles_btn.setToolTip("Create, edit, or delete profiles.")
        self.manage_profiles_btn.clicked.connect(self._open_profile_dialog)
        profile_row.addWidget(self.manage_profiles_btn)
        left.addLayout(profile_row)

        activity_row = QtWidgets.QHBoxLayout()
        activity_row.addWidget(QtWidgets.QLabel("Activity"))
        self.activity_box = QtWidgets.QComboBox()
        self.activity_box.addItems(ACTIVITY_OPTIONS)
        self.activity_box.setCurrentText("Elliptical")
        self.activity_box.setToolTip("Activity factor used for kcal estimate.")
        self.activity_box.currentTextChanged.connect(lambda _v: self._save_settings())
        activity_row.addWidget(self.activity_box, 1)
        left.addLayout(activity_row)

        self.status_label = QtWidgets.QLabel("Ready")
        left.addWidget(self.status_label)

        left.addWidget(self._section_label("Rendering"))
        render_row = QtWidgets.QHBoxLayout()
        render_row.addWidget(QtWidgets.QLabel("FPS lock"))
        self.fps_lock_box = QtWidgets.QComboBox()
        self.fps_lock_box.addItems([
            "Auto",
            "15 FPS",
            "20 FPS",
            "24 FPS",
            "30 FPS",
            "40 FPS",
            "50 FPS",
            "60 FPS",
            "75 FPS",
            "90 FPS",
            "120 FPS",
            "144 FPS",
            "165 FPS",
            "240 FPS",
        ])
        self.fps_lock_box.setCurrentText("Auto")
        self.fps_lock_box.setToolTip("Rendering FPS cap. Auto follows current display refresh rate.")
        self.fps_lock_box.currentTextChanged.connect(self._on_fps_lock_changed)
        render_row.addWidget(self.fps_lock_box, 1)
        left.addLayout(render_row)

        left.addWidget(self._section_label("Live"))
        live_grid = QtWidgets.QGridLayout()
        live_grid.setHorizontalSpacing(12)
        live_grid.setVerticalSpacing(4)
        self.live_battery_label = QtWidgets.QLabel("Battery: --%")
        self.live_hr_label = QtWidgets.QLabel("HR: -- BPM")
        self.live_rr_label = QtWidgets.QLabel("RR: ---- ms")
        self.live_kcal_label = QtWidgets.QLabel("Est kcal: 0.00")
        self.live_duration_label = QtWidgets.QLabel("Duration: 00:00")
        live_grid.addWidget(self.live_battery_label, 0, 0)
        live_grid.addWidget(self.live_hr_label, 0, 1)
        live_grid.addWidget(self.live_rr_label, 1, 0)
        live_grid.addWidget(self.live_kcal_label, 1, 1)
        live_grid.addWidget(self.live_duration_label, 2, 0)
        left.addLayout(live_grid)

        left.addWidget(self._section_label("Logs"))
        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMaximumBlockCount(2000)
        left.addWidget(self.log_box, 2)

        charts = QtWidgets.QFrame(root)
        charts.setObjectName("panel")
        right = QtWidgets.QVBoxLayout(charts)
        right.setContentsMargins(8, 8, 8, 8)
        right.setSpacing(8)

        chart_live = QtWidgets.QHBoxLayout()
        chart_live.setSpacing(8)
        self.chart_battery_badge = QtWidgets.QLabel("BAT --%")
        self.chart_hr_badge = QtWidgets.QLabel("HR -- BPM")
        self.chart_rr_badge = QtWidgets.QLabel("RR ---- MS")
        self.chart_kcal_badge = QtWidgets.QLabel("KCAL 0.00")
        self.chart_time_badge = QtWidgets.QLabel("REC 00:00")
        self.chart_fps_badge = QtWidgets.QLabel("FPS --")
        for badge in (self.chart_battery_badge, self.chart_hr_badge, self.chart_rr_badge, self.chart_kcal_badge, self.chart_time_badge, self.chart_fps_badge):
            badge.setStyleSheet(
                "background-color:#1f2937;border:1px solid #334155;border-radius:6px;padding:5px 10px;color:#e5e7eb;font-weight:600;"
            )
            chart_live.addWidget(badge)
        self.header_record_btn = QtWidgets.QPushButton("Start Recording")
        self.header_record_btn.clicked.connect(self._toggle_recording)
        self.header_pause_btn = QtWidgets.QPushButton("Pause")
        self.header_pause_btn.clicked.connect(self._toggle_pause)
        self.header_pause_btn.setEnabled(False)
        self.header_pause_btn.setToolTip("Pause keeps live streams visible but stops file writes.")
        self.header_sidebar_btn = QtWidgets.QPushButton("☰ Hide Sidebar")
        self.header_sidebar_btn.clicked.connect(self._toggle_sidebar)
        chart_live.addWidget(self.header_pause_btn)
        chart_live.addWidget(self.header_record_btn)
        chart_live.addWidget(self.header_sidebar_btn)
        right.addLayout(chart_live)

        self.charts = QtGraphCharts(charts)
        right.addWidget(self.charts, 1)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal, root)
        self.main_splitter.addWidget(controls)
        self.main_splitter.addWidget(charts)
        self.main_splitter.setStretchFactor(0, 0)
        self.main_splitter.setStretchFactor(1, 1)
        self.main_splitter.setSizes([420, 1140])
        self.main_splitter.splitterMoved.connect(lambda *_: self._save_settings())
        main_layout.addWidget(self.main_splitter, 1)

        self.lockable_controls = [
            self.scan_btn,
            self.connect_btn,
            self.device_list,
            self.hr_enabled,
            self.hr_instant,
            self.hr_unpack,
            self.sdk_mode,
            self.profile_box,
            self.manage_profiles_btn,
            self.activity_box,
        ] + list(self.meas_checks.values())
        self.record_btn = self.header_record_btn
        self.pause_btn = self.header_pause_btn
        self.charts.set_active_keys(self._selected_chart_keys())

    def _section_label(self, text: str) -> QtWidgets.QLabel:
        label = QtWidgets.QLabel(text)
        label.setObjectName("section")
        return label

    def _apply_qss(self):
        app = QtWidgets.QApplication.instance()
        if app is not None:
            app.setStyle("Fusion")
            app.setStyleSheet(QSS_THEME)

    def _save_settings(self):
        self._save_timer.start(300)

    def _save_settings_now(self):
        data = {
            "last_device_address": self.last_device_address,
            "last_device_name": self.last_device_name,
            "sdk_mode": self.sdk_mode.isChecked(),
            "hr_enabled": self.hr_enabled.isChecked(),
            "hr_instant": self.hr_instant.isChecked(),
            "hr_unpack": self.hr_unpack.isChecked(),
            "activity_type": self.activity_box.currentText(),
            "profile_id": self.selected_profile_id,
            "profiles": self.profiles,
            "window_geometry": [int(v) for v in self.geometry().getRect()],
            "window_state": "fullscreen" if self.isFullScreen() else ("maximized" if self.isMaximized() else "normal"),
            "main_splitter_sizes": [int(v) for v in self.main_splitter.sizes()],
            "sidebar_collapsed": bool(self.sidebar_collapsed),
            "auto_collapse_sidebar_on_record": bool(self.auto_collapse_sidebar_on_record),
            "live_measurements": {m: c.isChecked() for m, c in self.meas_checks.items()},
            "render_fps_mode": str(self.render_fps_mode),
            "render_fps_manual": int(self.render_fps_manual),
        }
        try:
            self.settings_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _load_settings(self):
        if not self.settings_path.exists():
            return
        try:
            data = json.loads(self.settings_path.read_text(encoding="utf-8"))
        except Exception:
            return
        self.last_device_address = data.get("last_device_address")
        self.last_device_name = data.get("last_device_name")
        self.sdk_mode.setChecked(bool(data.get("sdk_mode", False)))
        self.hr_enabled.setChecked(bool(data.get("hr_enabled", True)))
        self.hr_instant.setChecked(bool(data.get("hr_instant", True)))
        self.hr_unpack.setChecked(bool(data.get("hr_unpack", True)))
        activity = str(data.get("activity_type") or "")
        if activity in ACTIVITY_OPTIONS:
            self.activity_box.setCurrentText(activity)
        profile_id = str(data.get("profile_id") or "").strip()
        if profile_id:
            self.selected_profile_id = profile_id
        loaded_profiles = data.get("profiles")
        if isinstance(loaded_profiles, dict) and loaded_profiles:
            normalized = {}
            for pid, prof in loaded_profiles.items():
                if not isinstance(pid, str) or not isinstance(prof, dict):
                    continue
                merged = dict(DEFAULT_PROFILE)
                merged.update(prof)
                normalized[pid] = merged
            if normalized:
                self.profiles = normalized
        self._apply_selected_profile()
        live = data.get("live_measurements") or {}
        for m, c in self.meas_checks.items():
            if m in live:
                c.setChecked(bool(live[m]))
        fps_mode = str(data.get("render_fps_mode") or "auto").strip().lower()
        fps_manual_raw = data.get("render_fps_manual", self.render_fps_manual)
        try:
            fps_manual = int(fps_manual_raw)
        except Exception:
            fps_manual = self.render_fps_manual
        self.render_fps_manual = max(1, min(240, int(fps_manual)))
        self.render_fps_mode = "manual" if fps_mode == "manual" else "auto"
        self._sync_fps_selector_text()
        self._apply_render_fps()
        geom = data.get("window_geometry")
        if isinstance(geom, list) and len(geom) == 4:
            try:
                self.setGeometry(int(geom[0]), int(geom[1]), int(geom[2]), int(geom[3]))
            except Exception:
                pass
        sizes = data.get("main_splitter_sizes")
        if isinstance(sizes, list) and len(sizes) == 2:
            try:
                self.main_splitter.setSizes([max(260, int(sizes[0])), max(420, int(sizes[1]))])
                self._last_sidebar_width = max(260, int(sizes[0]))
            except Exception:
                pass
        if "auto_collapse_sidebar_on_record" in data:
            self.auto_collapse_sidebar_on_record = bool(data.get("auto_collapse_sidebar_on_record"))
        state = str(data.get("window_state") or "normal").lower()
        if state == "fullscreen":
            self.showFullScreen()
        elif state == "maximized":
            self.showMaximized()
        if bool(data.get("sidebar_collapsed", False)):
            self._set_sidebar_collapsed(True)
        if self.last_device_address:
            self._upsert_device(self.last_device_address, self.last_device_name, None)
            self._render_device_list()
        self.charts.set_active_keys(self._selected_chart_keys())
        self._refresh_live_labels()

    def _auto_connect_on_startup(self):
        if self.connected or (not self.last_device_address):
            return
        state = self._current_live_state()
        self._connect_requested_live_cfg = self._normalize_live_cfg(state)
        self._append_log(f"Connecting (auto) to {self.last_device_address}...")
        self._set_status(f"Auto-connecting: {self.last_device_address}")
        fut = self.engine.connect(
            self.last_device_address,
            hr_live_enabled=state["hr_enabled"],
            preview_pmd_measurements=state["pmd_measurements"],
        )
        self._track_future(fut, self._on_connected, "Auto-connect failed")

    def _start_battery_polling(self):
        self.battery_poll_timer.stop()
        QtCore.QTimer.singleShot(10000, self._battery_poll_tick)
        self.battery_poll_timer.start()

    def _stop_battery_polling(self):
        self.battery_poll_timer.stop()
        self.battery_poll_inflight = False

    def _battery_poll_tick(self):
        if (not self.connected) or self.battery_poll_inflight:
            return
        self.battery_poll_inflight = True
        fut = self.engine.read_battery()
        self._track_future(fut, self._on_battery_polled, "Battery poll warning", on_fail=self._on_battery_poll_failed)

    def _on_battery_polled(self, battery):
        self.battery_poll_inflight = False
        self._live_battery_value = battery
        self._refresh_live_labels()

    def _on_battery_poll_failed(self, _exc):
        self.battery_poll_inflight = False

    def _hr_kcal_per_min(self, hr_bpm: float) -> float:
        age = float(self.body_profile.get("age_years", DEFAULT_PROFILE["age_years"]))
        wt = float(self.body_profile.get("weight_kg", DEFAULT_PROFILE["weight_kg"]))
        sex = str(self.body_profile.get("sex", DEFAULT_PROFILE["sex"])).lower()
        if sex == "female":
            kcal_min = (-20.4022 + 0.4472 * hr_bpm - 0.1263 * wt + 0.074 * age) / 4.184
        else:
            kcal_min = (-55.0969 + 0.6309 * hr_bpm + 0.1988 * wt + 0.2017 * age) / 4.184
        factor = ACTIVITY_FACTOR.get(self.activity_box.currentText(), 1.0)
        return max(0.0, kcal_min * factor)

    def _update_kcal_estimate(self, timestamp_s: float, hr_bpm: float):
        if self._kcal_last_t is None:
            self._kcal_last_t = timestamp_s
            return
        dt_s = max(0.0, float(timestamp_s) - float(self._kcal_last_t))
        self._kcal_last_t = timestamp_s
        if dt_s <= 0.0:
            return
        kcal_per_min = self._hr_kcal_per_min(float(hr_bpm))
        self.kcal_total += kcal_per_min * (dt_s / 60.0)
        self._refresh_live_labels()
        self._kcal_timeline.append(
            {
                "timestamp_s": float(timestamp_s),
                "heart_rate_bpm": float(hr_bpm),
                "kcal_total": float(self.kcal_total),
                "kcal_per_min": float(kcal_per_min),
                "activity_type": self.activity_box.currentText(),
            }
        )

    def _save_energy_outputs(self):
        if not self.current_session_path:
            return
        try:
            summary = {
                "estimated_kcal_total": round(float(self.kcal_total), 3),
                "activity_type": self.activity_box.currentText(),
                "profile_id": self.selected_profile_id,
                "profile": self.body_profile,
                "points": len(self._kcal_timeline),
                "method": "HR-based estimate (Keytel equation + activity factor)",
            }
            (self.current_session_path / "energy_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            if self._kcal_timeline:
                lines = ["timestamp_s,heart_rate_bpm,kcal_total,kcal_per_min,activity_type"]
                for p in self._kcal_timeline:
                    lines.append(
                        f"{p['timestamp_s']:.6f},{p['heart_rate_bpm']:.2f},{p['kcal_total']:.6f},{p['kcal_per_min']:.6f},{p['activity_type']}"
                    )
                (self.current_session_path / "kcal_timeline.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        except Exception as exc:
            self._append_log(f"Warning: could not save kcal files: {exc}")

    def _log_gpu_info(self):
        vendor, renderer, version = self.charts.gl_info()
        if vendor is None and renderer is None:
            self._append_log(f"GPU: {version}")
            return
        self._append_log(f"GPU: OpenGL active | vendor={vendor} | renderer={renderer} | version={version}")

    def _sanitize_id(self, value: str) -> str:
        raw = (value or "").strip().lower()
        safe = []
        for ch in raw:
            if ch.isalnum() or ch in "._-":
                safe.append(ch)
            else:
                safe.append("_")
        out = "".join(safe).strip("._-")
        return out or "participant_001"

    def _sanitize_profile_id(self, value: str) -> str:
        return self._sanitize_id(value)

    def _refresh_profile_box(self):
        if not hasattr(self, "profile_box"):
            return
        ids = sorted(self.profiles.keys())
        self.profile_box.blockSignals(True)
        self.profile_box.clear()
        self.profile_box.addItems(ids)
        if self.selected_profile_id in ids:
            self.profile_box.setCurrentText(self.selected_profile_id)
        elif ids:
            self.profile_box.setCurrentIndex(0)
        self.profile_box.blockSignals(False)

    def _apply_selected_profile(self):
        pid = str(self.selected_profile_id or "").strip()
        if pid not in self.profiles:
            if self.profiles:
                pid = sorted(self.profiles.keys())[0]
            else:
                pid = "participant_001"
                self.profiles[pid] = dict(DEFAULT_PROFILE)
        self.selected_profile_id = pid
        merged = dict(DEFAULT_PROFILE)
        merged.update(self.profiles.get(pid, {}))
        self.body_profile = merged
        self._refresh_profile_box()

    def _on_profile_selected(self, _value=None):
        if hasattr(self, "profile_box"):
            txt = self.profile_box.currentText().strip()
            if txt:
                self.selected_profile_id = txt
        self._apply_selected_profile()
        self._save_settings()

    def _open_profile_dialog(self):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Profiles")
        dialog.resize(720, 460)
        layout = QtWidgets.QHBoxLayout(dialog)

        left = QtWidgets.QVBoxLayout()
        right = QtWidgets.QFormLayout()

        profile_list = QtWidgets.QListWidget(dialog)
        left.addWidget(profile_list, 1)

        left_btns = QtWidgets.QHBoxLayout()
        btn_new = QtWidgets.QPushButton("New", dialog)
        btn_delete = QtWidgets.QPushButton("Delete", dialog)
        left_btns.addWidget(btn_new)
        left_btns.addWidget(btn_delete)
        left.addLayout(left_btns)

        fields = {
            "profile_id": QtWidgets.QLineEdit(dialog),
            "name": QtWidgets.QLineEdit(dialog),
            "sex": QtWidgets.QComboBox(dialog),
            "age_years": QtWidgets.QSpinBox(dialog),
            "weight_kg": QtWidgets.QDoubleSpinBox(dialog),
            "height_cm": QtWidgets.QDoubleSpinBox(dialog),
            "hr_rest": QtWidgets.QSpinBox(dialog),
            "hr_max": QtWidgets.QSpinBox(dialog),
        }
        fields["sex"].addItems(["male", "female"])
        fields["age_years"].setRange(1, 120)
        fields["weight_kg"].setRange(1.0, 400.0)
        fields["weight_kg"].setDecimals(1)
        fields["height_cm"].setRange(50.0, 260.0)
        fields["height_cm"].setDecimals(1)
        fields["hr_rest"].setRange(20, 240)
        fields["hr_max"].setRange(40, 260)

        right.addRow("Profile ID", fields["profile_id"])
        right.addRow("Name", fields["name"])
        right.addRow("Sex", fields["sex"])
        right.addRow("Age (years)", fields["age_years"])
        right.addRow("Weight (kg)", fields["weight_kg"])
        right.addRow("Height (cm)", fields["height_cm"])
        right.addRow("Resting Pulse (bpm)", fields["hr_rest"])
        right.addRow("Max Pulse (bpm)", fields["hr_max"])

        right_btns = QtWidgets.QHBoxLayout()
        btn_save = QtWidgets.QPushButton("Save Profile", dialog)
        btn_close = QtWidgets.QPushButton("Close", dialog)
        right_btns.addStretch(1)
        right_btns.addWidget(btn_save)
        right_btns.addWidget(btn_close)

        right_wrap = QtWidgets.QVBoxLayout()
        right_wrap.addLayout(right)
        right_wrap.addStretch(1)
        right_wrap.addLayout(right_btns)

        layout.addLayout(left, 1)
        layout.addLayout(right_wrap, 2)

        def refresh_list(select_id=None):
            ids = sorted(self.profiles.keys())
            profile_list.clear()
            for pid in ids:
                profile_list.addItem(pid)
            if not ids:
                return
            target = select_id if (select_id in ids) else (self.selected_profile_id if self.selected_profile_id in ids else ids[0])
            for i in range(profile_list.count()):
                if profile_list.item(i).text() == target:
                    profile_list.setCurrentRow(i)
                    break

        def load_selected():
            item = profile_list.currentItem()
            if item is None:
                return
            pid = item.text()
            prof = dict(DEFAULT_PROFILE)
            prof.update(self.profiles.get(pid, {}))
            fields["profile_id"].setText(pid)
            fields["name"].setText(str(prof.get("name", "")))
            fields["sex"].setCurrentText(str(prof.get("sex", "male")))
            fields["age_years"].setValue(int(prof.get("age_years", 30)))
            fields["weight_kg"].setValue(float(prof.get("weight_kg", 75.0)))
            fields["height_cm"].setValue(float(prof.get("height_cm", 175.0)))
            fields["hr_rest"].setValue(int(prof.get("hr_rest", 60)))
            fields["hr_max"].setValue(int(prof.get("hr_max", 190)))

        def save_current():
            old_item = profile_list.currentItem()
            old_pid = old_item.text() if old_item is not None else ""
            pid = self._sanitize_profile_id(fields["profile_id"].text())
            if not pid:
                QtWidgets.QMessageBox.warning(dialog, "Invalid profile", "Profile ID is required.")
                return
            profile = {
                "name": fields["name"].text().strip() or pid,
                "sex": fields["sex"].currentText().strip().lower(),
                "age_years": int(fields["age_years"].value()),
                "weight_kg": float(fields["weight_kg"].value()),
                "height_cm": float(fields["height_cm"].value()),
                "hr_rest": int(fields["hr_rest"].value()),
                "hr_max": int(fields["hr_max"].value()),
            }
            if profile["sex"] not in ("male", "female"):
                QtWidgets.QMessageBox.warning(dialog, "Invalid profile", "Sex must be male or female.")
                return
            if profile["hr_rest"] >= profile["hr_max"]:
                QtWidgets.QMessageBox.warning(dialog, "Invalid profile", "Resting pulse must be lower than max pulse.")
                return
            if old_pid and old_pid != pid and old_pid in self.profiles:
                self.profiles.pop(old_pid, None)
            self.profiles[pid] = profile
            self.selected_profile_id = pid
            self._apply_selected_profile()
            self._save_settings()
            refresh_list(select_id=pid)
            self._set_status(f"Profile saved: {pid}")
            dialog.accept()

        def new_profile():
            base = "participant"
            idx = 1
            candidate = f"{base}_{idx:02d}"
            while candidate in self.profiles:
                idx += 1
                candidate = f"{base}_{idx:02d}"
            fields["profile_id"].setText(candidate)
            fields["name"].setText(f"Participant {idx:02d}")
            fields["sex"].setCurrentText("male")
            fields["age_years"].setValue(30)
            fields["weight_kg"].setValue(75.0)
            fields["height_cm"].setValue(175.0)
            fields["hr_rest"].setValue(60)
            fields["hr_max"].setValue(190)

        def delete_profile():
            item = profile_list.currentItem()
            if item is None:
                return
            pid = item.text()
            if len(self.profiles) <= 1:
                QtWidgets.QMessageBox.warning(dialog, "Cannot delete", "At least one profile must remain.")
                return
            ans = QtWidgets.QMessageBox.question(dialog, "Delete profile", f"Delete profile '{pid}'?")
            if ans != QtWidgets.QMessageBox.Yes:
                return
            self.profiles.pop(pid, None)
            if self.selected_profile_id == pid:
                self.selected_profile_id = sorted(self.profiles.keys())[0]
            self._apply_selected_profile()
            self._save_settings()
            refresh_list(select_id=self.selected_profile_id)

        profile_list.currentItemChanged.connect(lambda *_: load_selected())
        btn_new.clicked.connect(new_profile)
        btn_delete.clicked.connect(delete_profile)
        btn_save.clicked.connect(save_current)
        btn_close.clicked.connect(dialog.accept)

        refresh_list(select_id=self.selected_profile_id)
        if profile_list.currentItem() is not None:
            load_selected()
        dialog.exec()

    def _append_log(self, text: str):
        self._log_buffer.append(str(text))

    def _flush_log_buffer(self):
        if not self._log_buffer:
            return
        batch = []
        while self._log_buffer and len(batch) < 120:
            batch.append(self._log_buffer.popleft())
        self.log_box.appendPlainText("\n".join(batch))

    def _set_status(self, text: str):
        self.status_label.setText(text)

    def _reset_ecg_stream_state(self):
        if self.render is not None:
            self.render.reset_ecg_stream_state()

    def _reset_playback_stream_state(self):
        if self.render is not None:
            self.render.reset_playback_stream_state()

    def _current_live_state(self):
        pmd_selected = [m for m, c in self.meas_checks.items() if c.isChecked()]
        if self.connected:
            pmd_selected = [m for m in pmd_selected if m in self.available_measurements]
        hr_enabled = bool(self.hr_enabled.isChecked())
        chart_keys = []
        if hr_enabled:
            chart_keys.extend(["HR", "RR"])
        if "ACC" in pmd_selected:
            chart_keys.append("ACC")
        if "ECG" in pmd_selected:
            chart_keys.append("ECG")
        return {
            "hr_enabled": hr_enabled,
            "pmd_measurements": tuple(pmd_selected),
            "chart_keys": tuple(chart_keys),
        }

    def _selected_preview_measurements(self):
        return self._current_live_state()["pmd_measurements"]

    def _selected_chart_keys(self):
        return self._current_live_state()["chart_keys"]

    def _normalize_live_cfg(self, state: dict) -> tuple[bool, tuple[str, ...]]:
        return bool(state["hr_enabled"]), tuple(sorted(state["pmd_measurements"]))

    def _apply_live_config_if_needed(self, state: dict, *, force: bool = False):
        if not self.connected:
            return
        cfg = self._normalize_live_cfg(state)
        if (not force) and (cfg == self._last_applied_live_cfg):
            return
        fut = self.engine.set_live_config(
            hr_live_enabled=state["hr_enabled"],
            preview_measurements=state["pmd_measurements"],
        )

        def _ok(_):
            self._last_applied_live_cfg = cfg

        self._track_future(fut, _ok, "Live config warning")

    def _current_record_elapsed_s(self) -> float:
        elapsed = float(self._record_elapsed_s)
        if self.recording and (not self.recording_paused) and self._record_last_resume_mono is not None:
            elapsed += max(0.0, time.monotonic() - float(self._record_last_resume_mono))
        return elapsed

    def _format_duration(self, total_s: float) -> str:
        total = max(0, int(total_s))
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def _session_tick(self):
        if self.recording:
            self._refresh_live_labels()

    def _refresh_live_labels(self):
        bat_txt = "--%" if self._live_battery_value is None else f"{int(self._live_battery_value)}%"
        hr_txt = "-- BPM" if self._live_hr_value is None else f"{int(round(self._live_hr_value))} BPM"
        rr_txt = "---- ms" if self._live_rr_value is None else f"{int(round(self._live_rr_value))} ms"
        kcal_txt = f"{self.kcal_total:.2f}"
        dur_txt = self._format_duration(self._current_record_elapsed_s())
        self.live_battery_label.setText(f"Battery: {bat_txt}")
        self.live_hr_label.setText(f"HR: {hr_txt}")
        self.live_rr_label.setText(f"RR: {rr_txt}")
        self.live_kcal_label.setText(f"Est kcal: {kcal_txt}")
        self.live_duration_label.setText(f"Duration: {dur_txt}")
        self.chart_battery_badge.setText(f"BAT {bat_txt}")
        self.chart_hr_badge.setText(f"HR {hr_txt.upper()}")
        self.chart_rr_badge.setText(f"RR {rr_txt.upper()}")
        self.chart_kcal_badge.setText(f"KCAL {kcal_txt}")
        self.chart_time_badge.setText(f"REC {dur_txt}")
        if self._fps_value <= 0.0:
            self.chart_fps_badge.setText(f"FPS --/{int(self.render_fps)}")
        else:
            self.chart_fps_badge.setText(f"FPS {self._fps_value:0.1f}/{int(self.render_fps)}")

    def _detect_display_refresh_rate(self) -> int:
        app = QtWidgets.QApplication.instance()
        if app is None:
            return 60
        screen = None
        try:
            wh = self.windowHandle()
            if wh is not None:
                screen = wh.screen()
        except Exception:
            screen = None
        if screen is None:
            screen = app.primaryScreen()
        if screen is None:
            return 60
        try:
            hz = int(round(float(screen.refreshRate())))
        except Exception:
            hz = 60
        if hz <= 1:
            hz = 60
        return max(30, min(360, hz))

    def _effective_render_fps(self) -> int:
        if self.render_fps_mode == "auto":
            return max(1, min(240, int(self.display_refresh_hz)))
        return max(1, min(240, int(self.render_fps_manual)))

    def _apply_render_fps(self):
        self.render_fps = self._effective_render_fps()
        interval_ms = max(1, round(1000.0 / float(self.render_fps)))
        if hasattr(self, "render_timer") and self.render_timer is not None:
            self.render_timer.setInterval(interval_ms)
        self._refresh_live_labels()

    def _sync_fps_selector_text(self):
        if not hasattr(self, "fps_lock_box"):
            return
        if self.render_fps_mode == "auto":
            self.fps_lock_box.blockSignals(True)
            self.fps_lock_box.setCurrentText("Auto")
            self.fps_lock_box.blockSignals(False)
            self.fps_lock_box.setToolTip(f"Rendering FPS cap. Auto follows current display refresh rate (~{int(self.display_refresh_hz)} Hz).")
        else:
            target = f"{int(self.render_fps_manual)} FPS"
            self.fps_lock_box.blockSignals(True)
            idx = self.fps_lock_box.findText(target)
            if idx >= 0:
                self.fps_lock_box.setCurrentIndex(idx)
            else:
                self.fps_lock_box.setCurrentText(target)
            self.fps_lock_box.blockSignals(False)
            self.fps_lock_box.setToolTip("Rendering FPS cap. Auto follows current display refresh rate.")

    def _on_fps_lock_changed(self, value: str):
        txt = str(value or "").strip().lower()
        if txt.startswith("auto"):
            self.render_fps_mode = "auto"
        else:
            try:
                fps = int(txt.replace("fps", "").strip())
            except Exception:
                return
            self.render_fps_mode = "manual"
            self.render_fps_manual = max(1, min(240, int(fps)))
        self._apply_render_fps()
        self._sync_fps_selector_text()
        self._save_settings()

    def _refresh_display_rate_if_needed(self, now_monotonic: float):
        if now_monotonic < self._next_refresh_probe_due:
            return
        self._next_refresh_probe_due = now_monotonic + 2.0
        hz = self._detect_display_refresh_rate()
        if hz != self.display_refresh_hz:
            self.display_refresh_hz = hz
            if self.render_fps_mode == "auto":
                self._apply_render_fps()
            self._sync_fps_selector_text()

    def _render_device_list(self):
        self.device_list.clear()
        selected_row = None
        for d in self.devices:
            name = d.get("name") or "Unknown"
            addr = d.get("address") or "?"
            rssi = d.get("rssi")
            rssi_txt = f"RSSI={rssi}" if rssi is not None else "RSSI=?"
            row = self.device_list.count()
            self.device_list.addItem(f"{name} | {addr} | {rssi_txt}")
            if self.last_device_address and str(addr).upper() == self.last_device_address.upper():
                selected_row = row
        if selected_row is not None:
            self.device_list.setCurrentRow(selected_row)

    def _upsert_device(self, address: str, name: str | None = None, rssi=None):
        if not address:
            return
        addr_up = str(address).upper()
        for d in self.devices:
            if str(d.get("address") or "").upper() == addr_up:
                if name:
                    d["name"] = str(name)
                if rssi is not None:
                    d["rssi"] = rssi
                return
        self.devices.append(
            {
                "address": str(address),
                "name": str(name) if name else "Remembered device",
                "rssi": rssi,
            }
        )

    def _selected_address(self):
        row = self.device_list.currentRow()
        if row < 0 or row >= len(self.devices):
            return None
        return self.devices[row].get("address")

    def _set_controls_locked(self, locked: bool):
        for ctrl in self.lockable_controls:
            ctrl.setEnabled(not locked)
        if not locked:
            self._refresh_measurement_availability()

    def _refresh_measurement_availability(self):
        for meas, chk in self.meas_checks.items():
            enabled = (not self.connected) or (meas in self.available_measurements)
            chk.setEnabled(enabled)
            if enabled:
                chk.setToolTip(PMD_HELP.get(meas, meas))
            else:
                chk.setToolTip(f"{PMD_HELP.get(meas, meas)}\nNot available on this connected device.")
        self.charts.set_active_keys(self._selected_chart_keys())

    def _set_connect_button_state(self):
        self.connect_btn.setText("Disconnect" if self.connected else "Connect")

    def _set_record_button_state(self):
        rec_btn = self.header_record_btn
        pause_btn = self.header_pause_btn
        if self.recording:
            rec_btn.setText("Stop Recording")
            pause_btn.setEnabled(True)
            pause_btn.setText("Resume" if self.recording_paused else "Pause")
        else:
            rec_btn.setText("Start Recording")
            pause_btn.setEnabled(False)
            pause_btn.setText("Pause")

    def _toggle_sidebar(self):
        self._set_sidebar_collapsed(not self.sidebar_collapsed)

    def _set_sidebar_collapsed(self, collapsed: bool):
        collapsed = bool(collapsed)
        total = max(1, self.main_splitter.width())
        sizes = self.main_splitter.sizes()
        left_now = sizes[0] if sizes else self._last_sidebar_width
        if (not collapsed) and left_now > 40:
            self._last_sidebar_width = int(left_now)
        if collapsed:
            self.main_splitter.setSizes([0, total])
        else:
            left = max(320, min(int(self._last_sidebar_width or 420), int(total * 0.7)))
            right = max(420, total - left)
            self.main_splitter.setSizes([left, right])
        self.sidebar_collapsed = collapsed
        self.header_sidebar_btn.setText("☰ Show Sidebar" if collapsed else "☰ Hide Sidebar")
        self._save_settings()

    def _track_future(self, fut, on_ok, on_err, on_fail=None):
        self._pending_calls.append((fut, on_ok, on_err, on_fail))

    def _scan(self):
        self._set_status("Scanning Polar devices...")
        fut = self.engine.scan(timeout=8.0)

        def on_ok(result):
            self.devices = result
            self._render_device_list()
            self._set_status(f"Found {len(self.devices)} Polar device(s)")

        self._track_future(fut, on_ok, "Scan failed")

    def _connect_selected(self):
        if self.connected:
            self._disconnect()
            return
        address = self._selected_address()
        if not address:
            QtWidgets.QMessageBox.warning(self, "No selection", "Select a device first.")
            return
        state = self._current_live_state()
        self._connect_requested_live_cfg = self._normalize_live_cfg(state)
        self._append_log(f"Connecting to {address}...")
        self._set_status(f"Connecting to {address}...")
        fut = self.engine.connect(
            address,
            hr_live_enabled=state["hr_enabled"],
            preview_pmd_measurements=state["pmd_measurements"],
        )
        self._track_future(fut, self._on_connected, "Connect failed")

    def _on_connected(self, info):
        self.connected = True
        self.available_measurements = set(info.get("available_measurements") or [])
        self._set_connect_button_state()
        self._refresh_measurement_availability()
        self._set_status(f"Connected: {info.get('address')}")
        self.last_device_address = info.get("address")
        self.last_device_name = info.get("name")
        self._upsert_device(self.last_device_address, self.last_device_name, None)
        self._render_device_list()
        battery = info.get("battery")
        if battery is not None:
            self._live_battery_value = battery
            self._refresh_live_labels()
        self._start_battery_polling()
        self._reset_ecg_stream_state()
        self._reset_playback_stream_state()
        state = self._current_live_state()
        cfg_now = self._normalize_live_cfg(state)
        if self._connect_requested_live_cfg == cfg_now:
            self._last_applied_live_cfg = cfg_now
        else:
            self._apply_live_config_if_needed(state, force=True)
        self._connect_requested_live_cfg = None
        self._save_settings()

    def _disconnect(self):
        fut = self.engine.disconnect()

        def on_ok(_):
            self.connected = False
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self.available_measurements = set()
            self._stop_battery_polling()
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._set_connect_button_state()
            self._refresh_measurement_availability()
            self._set_status("Disconnected")
            self._live_battery_value = None
            self._live_hr_value = None
            self._live_rr_value = None
            self._reset_ecg_stream_state()
            self._reset_playback_stream_state()
            self._last_applied_live_cfg = None
            self._connect_requested_live_cfg = None
            self._refresh_live_labels()
            self._save_settings()

        self._track_future(fut, on_ok, "Disconnect warning")

    def _on_measurement_toggle(self):
        prev_state = getattr(self, "_last_live_state", self._current_live_state())
        state = self._current_live_state()
        prev_pmd = set(prev_state.get("pmd_measurements", ()))
        new_pmd = set(state.get("pmd_measurements", ()))
        if ("ECG" in new_pmd and "ECG" not in prev_pmd) or ("ECG" in prev_pmd and "ECG" not in new_pmd):
            self._reset_ecg_stream_state()
        self.charts.set_active_keys(state["chart_keys"])
        self._last_live_state = state
        self._save_settings()
        if self.recording or (not self.connected):
            return
        self._apply_live_config_if_needed(state)

    def _toggle_recording(self):
        if self.recording:
            self._stop_recording()
            return
        self._start_recording()

    def _start_recording(self):
        if not self.connected:
            QtWidgets.QMessageBox.warning(self, "Not connected", "Connect to a device first.")
            return
        profile_id = self._sanitize_profile_id(self.selected_profile_id)
        if profile_id not in self.profiles:
            QtWidgets.QMessageBox.warning(self, "Profile missing", "Select a valid profile before recording.")
            return
        activity = self._sanitize_id(self.activity_box.currentText())
        config = RecordingConfig(
            session_id=f"{profile_id}_{activity}",
            record_hr=self._current_live_state()["hr_enabled"],
            instant_rate=self.hr_instant.isChecked(),
            unpack_hr=self.hr_unpack.isChecked(),
            enable_sdk_mode=self.sdk_mode.isChecked(),
            pmd_measurements=self._current_live_state()["pmd_measurements"],
        )
        fut = self.engine.start_recording(config)

        def on_ok(path):
            self.recording = True
            self.recording_paused = False
            self.current_session_path = Path(path)
            self.kcal_total = 0.0
            self._kcal_last_t = None
            self._kcal_timeline = []
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = time.monotonic()
            self._refresh_live_labels()
            self.selected_profile_id = profile_id
            self._apply_selected_profile()
            self._set_controls_locked(True)
            self._set_record_button_state()
            if self.auto_collapse_sidebar_on_record:
                self._set_sidebar_collapsed(True)
            self._set_status(f"Recording: {path}")

        self._track_future(fut, on_ok, "Start failed")

    def _stop_recording(self):
        fut = self.engine.stop_recording()

        def on_ok(_):
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self._save_energy_outputs()
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._set_status("Recording stopped")
            self._refresh_live_labels()

        self._track_future(fut, on_ok, "Stop warning")

    def _toggle_pause(self):
        if (not self.connected) or (not self.recording):
            return
        if self.recording_paused:
            fut = self.engine.resume_recording()

            def on_ok(_):
                self.recording_paused = False
                self._record_last_resume_mono = time.monotonic()
                self._set_record_button_state()
                self._set_status("Recording resumed")
                self._refresh_live_labels()

            self._track_future(fut, on_ok, "Resume failed")
            return

        fut = self.engine.pause_recording()

        def on_ok(_):
            self.recording_paused = True
            if self._record_last_resume_mono is not None:
                self._record_elapsed_s += max(0.0, time.monotonic() - float(self._record_last_resume_mono))
            self._record_last_resume_mono = None
            self._set_record_button_state()
            self._set_status("Recording paused")
            self._refresh_live_labels()

        self._track_future(fut, on_ok, "Pause failed")

    def _enqueue_hr_sample(self, ts: float, hr_value: float, now_monotonic: float):
        if self.render is not None:
            self.render.enqueue_hr_sample(ts, hr_value, now_monotonic)

    def _enqueue_rr_samples(self, packet_ts: float, rr_values, now_monotonic: float):
        if self.render is not None:
            self.render.enqueue_rr_samples(packet_ts, rr_values, now_monotonic)

    def _enqueue_ecg_chunk(self, end_ts: float, samples, sample_rate: float, now_monotonic: float):
        if self.render is not None:
            self.render.enqueue_ecg_chunk(end_ts, samples, sample_rate, now_monotonic)

    def _enqueue_acc_chunk(self, end_ts: float, samples, sample_rate: float):
        if self.render is not None:
            self.render.enqueue_acc_chunk(end_ts, samples, sample_rate, time.monotonic())

    def _is_stream_recent(self, stream_key: str, now_monotonic: float, horizon_s: float = 0.9) -> bool:
        if self.render is None:
            return False
        return self.render._is_stream_recent(stream_key, now_monotonic, horizon_s=horizon_s)

    def _should_motion_redraw(self, now_monotonic: float) -> bool:
        return bool(self.render is not None and self.render.should_motion_redraw(now_monotonic))

    def _resolve_futures(self):
        if not self._pending_calls:
            return
        keep = []
        for fut, on_ok, on_err, on_fail in self._pending_calls:
            if not fut.done():
                keep.append((fut, on_ok, on_err, on_fail))
                continue
            try:
                on_ok(fut.result())
            except Exception as exc:
                if on_fail is not None:
                    try:
                        on_fail(exc)
                    except Exception:
                        pass
                QtWidgets.QMessageBox.warning(self, "Operation", f"{on_err}: {exc}")
                self._append_log(f"{on_err}: {exc}")
                self._set_status(on_err)
        self._pending_calls = keep

    @QtCore.Slot(object)
    def _handle_engine_event(self, event):
        now_monotonic = time.monotonic()
        etype = event.get("type")
        if etype == "log":
            msg = str(event.get("message", ""))
            low = msg.lower()
            if "already in state" in low and "preview failed" in low:
                msg = msg.replace(" failed: ALREADY IN STATE", " already active")
                msg = msg.replace(" failed: already in state", " already active")
            self._append_log(msg)
        elif etype == "battery":
            battery = event.get("value")
            self._live_battery_value = battery
            self._refresh_live_labels()
        elif etype == "recording_paused":
            self.recording_paused = True
            if self._record_last_resume_mono is not None:
                self._record_elapsed_s += max(0.0, time.monotonic() - float(self._record_last_resume_mono))
            self._record_last_resume_mono = None
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "recording_resumed":
            self.recording_paused = False
            self._record_last_resume_mono = time.monotonic()
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "recording_stopped":
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "disconnected":
            self.connected = False
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self.available_measurements = set()
            self._reset_ecg_stream_state()
            self._reset_playback_stream_state()
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._set_connect_button_state()
            self._refresh_measurement_availability()
            self._set_status("Disconnected")
        elif etype == "hr_sample":
            t = event.get("timestamp_s")
            hr = event.get("heart_rate")
            rr_vals = event.get("rr_values") or []
            if hr is not None and t is not None:
                self._enqueue_hr_sample(t, hr, now_monotonic)
                self._live_hr_value = hr
                self._refresh_live_labels()
                if self.recording and (not self.recording_paused):
                    self._update_kcal_estimate(float(t), float(hr))
            if rr_vals and t is not None:
                self._enqueue_rr_samples(t, rr_vals, now_monotonic)
                self._live_rr_value = rr_vals[-1]
                self._refresh_live_labels()
        elif etype == "ecg_samples":
            end_ts = event.get("end_timestamp_s")
            samples = event.get("samples") or []
            sr = float(event.get("sample_rate") or 130.0)
            if samples and end_ts is not None:
                self._enqueue_ecg_chunk(end_ts, samples, sr, now_monotonic)
        elif etype == "acc_samples":
            end_ts = event.get("end_timestamp_s")
            samples = event.get("samples") or []
            sr = float(event.get("sample_rate") or 200.0)
            if samples and end_ts is not None:
                self._enqueue_acc_chunk(end_ts, samples, sr)

    def _redraw_plot(self, now_monotonic: float, dirty: set[str] | None = None, motion_only: bool = False):
        if self.render is not None:
            self.render.redraw(now_monotonic, dirty=dirty, motion_only=motion_only)

    def _event_tick(self):
        now = time.monotonic()
        if self.render is not None:
            self.render.drain_playback_streams(now)

    def _render_tick(self):
        now = time.monotonic()
        self._refresh_display_rate_if_needed(now)
        self._fps_timer_ticks += 1
        should_motion = self._should_motion_redraw(now)
        dirty = self.render.consume_dirty() if self.render is not None else set()
        if (not dirty) and (not should_motion):
            elapsed = now - float(self._fps_last_mono)
            if elapsed >= 0.5:
                self._fps_tick_value = float(self._fps_timer_ticks) / elapsed
                self._fps_timer_ticks = 0
                self._fps_last_mono = now
                self.chart_fps_badge.setText(f"FPS {self._fps_value:0.1f}/{self._fps_tick_value:0.1f}")
            return
        motion_only = (not dirty) and should_motion
        self._redraw_plot(now_monotonic=now, dirty=dirty, motion_only=motion_only)
        self._fps_frames += 1
        elapsed = now - float(self._fps_last_mono)
        if elapsed >= 0.5:
            self._fps_value = float(self._fps_frames) / elapsed
            self._fps_tick_value = float(self._fps_timer_ticks) / elapsed
            self._fps_frames = 0
            self._fps_timer_ticks = 0
            self._fps_last_mono = now
            self.chart_fps_badge.setText(f"FPS {self._fps_value:0.1f}/{self._fps_tick_value:0.1f}")

    def _shutdown_gracefully(self):
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        self._set_status("Shutting down...")
        if hasattr(self, "event_timer"):
            self.event_timer.stop()
        if hasattr(self, "render_timer"):
            self.render_timer.stop()
        if hasattr(self, "future_timer"):
            self.future_timer.stop()
        if hasattr(self, "log_flush_timer"):
            self.log_flush_timer.stop()
        self._flush_log_buffer()
        try:
            if hasattr(self, "event_pump"):
                self.event_pump.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "event_thread"):
                self.event_thread.quit()
                self.event_thread.wait(1000)
        except Exception:
            pass
        self._stop_battery_polling()
        self._save_settings_now()
        try:
            if self.recording:
                self._append_log("Shutdown: stopping recording...")
                self.engine.stop_recording().result(timeout=20)
                self._save_energy_outputs()
            if self.connected:
                self._append_log("Shutdown: disconnecting device...")
                self.engine.disconnect().result(timeout=15)
        except Exception as exc:
            self._append_log(f"Shutdown warning: {exc}")
        try:
            self.engine.shutdown()
        except Exception as exc:
            self._append_log(f"Shutdown warning: {exc}")

    def closeEvent(self, event: QtGui.QCloseEvent):
        self._shutdown_gracefully()
        super().closeEvent(event)


def main():
    os.environ.setdefault("QT_OPENGL", "desktop")
    os.environ.setdefault("QSG_RHI_BACKEND", "opengl")
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.ApplicationAttribute.AA_UseDesktopOpenGL)

    app = QtWidgets.QApplication(sys.argv)
    pg.setConfigOptions(antialias=False, useOpenGL=True, enableExperimental=True)
    window = QtBleakHeartQtGraphUI()
    window.show()

    # Allow Ctrl+C in console to close Qt app via the same graceful shutdown path as window close.
    signal_pump = QtCore.QTimer()
    signal_pump.setInterval(200)
    signal_pump.timeout.connect(lambda: None)
    signal_pump.start()

    def _handle_sigint(_sig, _frame):
        QtCore.QTimer.singleShot(0, window.close)

    signal.signal(signal.SIGINT, _handle_sigint)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
