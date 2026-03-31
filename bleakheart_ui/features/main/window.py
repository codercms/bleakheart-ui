import queue
import signal
import sys
import time
import ctypes
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import os
import json
import shutil
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets
import pyqtgraph as pg

from bleakheart_ui.core.engine import BleakHeartEngine, PMD_TYPES, RecordingConfig
from bleakheart_ui.core.connection_manager import ConnectionManager
from bleakheart_ui.shared.render import QtGraphCharts
from bleakheart_ui.shared.render_controller import RenderController
from bleakheart_ui.features.sessions.recent_sessions_widget import RecentSessionCard
from bleakheart_ui.features.sessions.session_details_window import SessionDetailsWindow
from bleakheart_ui.features.sessions.session_manager_window import SessionHistoryWindow
from bleakheart_ui.infra.session_repository import SessionIndexRepository
from bleakheart_ui.features.main.settings_window import SettingsWindow
from bleakheart_ui.features.main.constants import (
    ACTIVITY_FACTOR,
    ACTIVITY_OPTIONS,
    DEFAULT_PROFILE,
    ECG_RENDER_DELAY_S,
    HR_HELP,
    PMD_HELP,
    SDK_HELP,
)
from bleakheart_ui.features.main.widgets import (
    ContainedScrollArea,
    EngineEventPump,
    GuardedComboBox,
    TelemetryTile,
    WideClickCheckBox,
)


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
QPushButton:focus {
    border: 1px solid #93c5fd;
}
QPushButton:disabled {
    color: #93a4ba;
    background-color: #1a2535;
}
QFrame#control_dock {
    background-color: #0f172a;
    border: 1px solid #24344e;
    border-radius: 12px;
}
QPushButton#dock_primary {
    background-color: #0f766e;
    border: 1px solid #2dd4bf;
    color: #ecfeff;
    font-weight: 700;
    border-radius: 10px;
    padding: 7px 14px;
}
QPushButton#dock_primary:hover {
    background-color: #0d9488;
    border: 1px solid #5eead4;
}
QPushButton#dock_primary:disabled {
    background-color: #17393a;
    border: 1px solid #2f4f52;
    color: #8aa2a4;
}
QPushButton#dock_secondary {
    background-color: #1f2937;
    border: 1px solid #475569;
    color: #dbe5f4;
    font-weight: 600;
    border-radius: 10px;
    padding: 7px 12px;
}
QPushButton#dock_secondary:hover {
    background-color: #26354c;
    border: 1px solid #64748b;
}
QPushButton#dock_secondary:disabled {
    background-color: #1a2535;
    border: 1px solid #334155;
    color: #7f91ab;
}
QToolButton#sidebar_handle {
    background-color: rgba(30, 41, 59, 0.94);
    border: 1px solid #60a5fa;
    border-radius: 10px;
    color: #e2e8f0;
    font-weight: 700;
    font-size: 13px;
    padding: 4px 6px;
}
QToolButton#sidebar_handle:hover {
    background-color: rgba(37, 99, 235, 0.28);
    border: 1px solid #93c5fd;
}
QToolButton#sidebar_handle:pressed {
    background-color: rgba(37, 99, 235, 0.4);
    border: 1px solid #bfdbfe;
}
QCheckBox {
    spacing: 8px;
    color: #e5e7eb;
    padding: 2px 0px;
}
QCheckBox::indicator {
    width: 18px;
    height: 18px;
}
QCheckBox:disabled {
    color: #7b8799;
}
QLineEdit, QListWidget, QPlainTextEdit {
    background-color: #0f172a;
    border: 1px solid #1f2937;
    border-radius: 6px;
    padding: 6px;
}
QComboBox {
    combobox-popup: 0;
    background-color: #0f172a;
    color: #e5e7eb;
    border: 1px solid #64748b;
    border-radius: 0px;
    padding: 5px 8px;
    padding-right: 24px;
}
QComboBox:hover {
    border: 1px solid #93c5fd;
    background-color: #12213b;
}
QComboBox:focus {
    border: 1px solid #60a5fa;
    background-color: #132642;
}
QComboBox QAbstractItemView {
    background-color: #0f172a;
    color: #e5e7eb;
    border: 1px solid #334155;
    show-decoration-selected: 1;
    selection-background-color: #1d4ed8;
    selection-color: #e5e7eb;
}
QComboBox QAbstractItemView::item {
    padding: 6px 8px;
}
QComboBox QAbstractItemView::item:hover {
    background-color: #1e293b;
    color: #e5e7eb;
}
QListWidget::item:selected {
    background-color: #1d4ed8;
}
"""


class QtBleakHeartQtGraphUI(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BleakHeart UI")
        icon_path = Path(__file__).resolve().parents[2] / "assets" / "app_icon.png"
        if icon_path.exists():
            self.setWindowIcon(QtGui.QIcon(str(icon_path)))
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
        self.app_dir = Path(__file__).resolve().parents[3]
        self.settings_path = self.app_dir / "qt_ui_settings.json"
        self.session_repo = SessionIndexRepository(self.app_dir)
        self._bg_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="history")
        self.history_window = None
        self._details_windows = {}
        self._history_refresh_inflight = False
        self._sidebar_handle_bootstrap_done = False
        self._save_timer = QtCore.QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._save_settings_now)

        self.devices = []
        self.connected = False
        self.recording = False
        self.recording_paused = False
        self._is_shutting_down = False
        self.auto_connect_on_startup = True
        self.recording_disconnect_mode = "pause_then_stop"
        self.startup_window_mode = "remember_last"
        self.connection_mgr = ConnectionManager(auto_reconnect_enabled=True, auto_reconnect_interval_ms=5000)
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
        self.signal_poll_interval_ms = 15000
        self.signal_poll_inflight = False
        self.sidebar_collapsed = False
        self.auto_collapse_sidebar_on_record = True
        self._last_sidebar_width = 420
        self.battery_poll_timer = QtCore.QTimer(self)
        self.battery_poll_timer.setInterval(self.battery_poll_interval_ms)
        self.battery_poll_timer.timeout.connect(self._battery_poll_tick)
        self.signal_poll_timer = QtCore.QTimer(self)
        self.signal_poll_timer.setInterval(self.signal_poll_interval_ms)
        self.signal_poll_timer.timeout.connect(self._signal_poll_tick)
        self.auto_reconnect_timer = QtCore.QTimer(self)
        self.auto_reconnect_timer.setInterval(self.connection_mgr.auto_reconnect_interval_ms)
        self.auto_reconnect_timer.timeout.connect(self._auto_reconnect_tick)
        self.recording_disconnect_grace_ms = 5 * 60 * 1000
        self.recording_disconnect_timer = QtCore.QTimer(self)
        self.recording_disconnect_timer.setSingleShot(True)
        self.recording_disconnect_timer.timeout.connect(self._on_recording_disconnect_timeout)
        self.session_timer = QtCore.QTimer(self)
        self.session_timer.setInterval(250)
        self.session_timer.setTimerType(QtCore.Qt.TimerType.PreciseTimer)
        self.session_timer.timeout.connect(self._session_tick)
        self.session_timer.start()

        self.render = None
        self._pending_calls = []
        self.display_refresh_hz = self._detect_display_refresh_rate()
        self.render_fps_mode = "manual"
        self.render_fps_manual = 30
        self.render_fps = self._effective_render_fps()
        self._next_refresh_probe_due = 0.0
        self._fps_last_mono = time.monotonic()
        self._fps_frames = 0
        self._fps_value = 0.0
        self._fps_timer_ticks = 0
        self._fps_tick_value = 0.0
        self.show_fps_overlay = False
        self.combine_hr_rr_chart = True
        self._last_signal_event_mono = 0.0
        self._log_buffer = deque()
        self._last_applied_live_cfg = None
        self._connect_requested_live_cfg = None
        self._engine_reset_required = False
        self._engine_reset_reason = None
        self._service_unavailable_retries = 0
        self._service_unavailable_reset_threshold = 2
        self._missing_service_failure_streak = 0
        self._missing_service_abort_threshold = 6
        self._adapter_recover_not_before_mono = 0.0
        self._adapter_recovery_delay_s = 7.0
        self._require_reconnect_scan = False
        self._reconnect_scan_inflight = False
        self._last_engine_recreate_mono = 0.0
        self._engine_recreate_min_interval_s = 8.0

        self._build_ui()
        self.render = RenderController(self.charts, ecg_render_delay_s=ECG_RENDER_DELAY_S)
        self.setAttribute(QtCore.Qt.WA_AlwaysShowToolTips, True)
        self._apply_qss()
        self._apply_selected_profile()
        self._load_settings()
        self._sync_fps_selector_text()
        self._apply_render_fps()
        self._schedule_sidebar_handle_reposition()
        self._last_live_state = self._current_live_state()
        self._refresh_live_labels()
        self._refresh_recent_sessions_ui()
        self._refresh_history_index_async(show_dialog=False)
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
        self._sync_render_timer_with_window_state()

    def _build_ui(self):
        root = QtWidgets.QWidget(self)
        self.setCentralWidget(root)
        self._root_container = root

        main_layout = QtWidgets.QVBoxLayout(root)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(12)
        line_h = max(14, QtGui.QFontMetrics(self.font()).lineSpacing())
        char_w = max(7, QtGui.QFontMetrics(self.font()).horizontalAdvance("M"))

        controls = QtWidgets.QFrame(root)
        self.controls_panel = controls
        controls.setObjectName("panel")
        controls.setMinimumWidth(char_w * 28)
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
        self.profile_box = GuardedComboBox()
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
        self.activity_box = GuardedComboBox()
        self.activity_box.addItems(ACTIVITY_OPTIONS)
        self.activity_box.setCurrentText("Elliptical")
        self.activity_box.setToolTip("Activity factor used for kcal estimate.")
        self.activity_box.currentTextChanged.connect(lambda _v: self._save_settings())
        activity_row.addWidget(self.activity_box, 1)
        left.addLayout(activity_row)

        self.status_label = QtWidgets.QLabel("Ready")
        left.addWidget(self.status_label)

        # Left "Live" section removed to reduce sidebar density.
        self.live_battery_label = QtWidgets.QLabel("Battery: --%")
        self.live_hr_label = QtWidgets.QLabel("HR: -- BPM")
        self.live_rr_label = QtWidgets.QLabel("RR: ---- ms")
        self.live_kcal_label = QtWidgets.QLabel("Est kcal: 0.00")
        self.live_duration_label = QtWidgets.QLabel("Duration: 00:00")
        self.live_battery_label.hide()
        self.live_hr_label.hide()
        self.live_rr_label.hide()
        self.live_kcal_label.hide()
        self.live_duration_label.hide()

        left.addWidget(self._section_label("Recent Sessions"))
        recent_header_row = QtWidgets.QHBoxLayout()
        self.open_history_btn = QtWidgets.QPushButton("Open Session History")
        self.open_history_btn.setToolTip("Browse all recorded sessions and open detailed viewer.")
        self.open_history_btn.clicked.connect(self._open_history_window)
        recent_header_row.addWidget(self.open_history_btn)
        self.open_settings_btn = QtWidgets.QPushButton("Settings")
        self.open_settings_btn.setToolTip("Open app defaults and behavior settings.")
        self.open_settings_btn.clicked.connect(self._open_settings_window)
        recent_header_row.addWidget(self.open_settings_btn)
        recent_header_row.addStretch(1)
        left.addLayout(recent_header_row)
        self.recent_sessions_scroll = ContainedScrollArea(controls)
        self.recent_sessions_scroll.setWidgetResizable(True)
        self.recent_sessions_scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.recent_sessions_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.recent_sessions_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.recent_sessions_wrap = QtWidgets.QWidget(self.recent_sessions_scroll)
        self.recent_sessions_layout = QtWidgets.QVBoxLayout(self.recent_sessions_wrap)
        self.recent_sessions_layout.setContentsMargins(0, 0, 0, 0)
        self.recent_sessions_layout.setSpacing(6)
        self.recent_sessions_scroll.setWidget(self.recent_sessions_wrap)
        self.recent_sessions_scroll.setMinimumHeight(line_h * 11)
        left.addWidget(self.recent_sessions_scroll, 3)

        left.addWidget(self._section_label("Logs"))
        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMaximumBlockCount(2000)
        self.log_box.setMinimumHeight(line_h * 6)
        left.addWidget(self.log_box, 1)

        charts = QtWidgets.QFrame(root)
        charts.setObjectName("panel")
        right = QtWidgets.QVBoxLayout(charts)
        right.setContentsMargins(8, 8, 8, 8)
        right.setSpacing(8)

        tiles_wrap = QtWidgets.QFrame(charts)
        tiles_wrap.setObjectName("panel")
        tiles_layout = QtWidgets.QGridLayout(tiles_wrap)
        tiles_layout.setContentsMargins(8, 8, 8, 8)
        tiles_layout.setHorizontalSpacing(8)
        tiles_layout.setVerticalSpacing(8)

        self.tile_battery = TelemetryTile("Battery", accent="#60a5fa", parent=tiles_wrap)
        self.tile_hr = TelemetryTile("Heart Rate", accent="#22d3ee", parent=tiles_wrap)
        self.tile_rr = TelemetryTile("RR Interval", accent="#f59e0b", parent=tiles_wrap)
        self.tile_kcal = TelemetryTile("Calories", accent="#fb7185", parent=tiles_wrap)
        self.tile_rec = TelemetryTile("Recording", accent="#34d399", parent=tiles_wrap)
        self.tile_signal = TelemetryTile("Signal Quality", accent="#64748b", parent=tiles_wrap)

        tiles_layout.addWidget(self.tile_battery, 0, 0)
        tiles_layout.addWidget(self.tile_hr, 0, 1)
        tiles_layout.addWidget(self.tile_rr, 0, 2)
        tiles_layout.addWidget(self.tile_kcal, 1, 0)
        tiles_layout.addWidget(self.tile_rec, 1, 1)
        tiles_layout.addWidget(self.tile_signal, 1, 2)
        tiles_layout.setColumnStretch(0, 1)
        tiles_layout.setColumnStretch(1, 1)
        tiles_layout.setColumnStretch(2, 1)
        right.addWidget(tiles_wrap, 0)

        self.charts = QtGraphCharts(charts)
        self.charts.set_combine_hr_rr(bool(self.combine_hr_rr_chart))
        right.addWidget(self.charts, 1)

        self.control_dock = QtWidgets.QFrame(tiles_wrap)
        self.control_dock.setObjectName("control_dock")
        control_dock_layout = QtWidgets.QHBoxLayout(self.control_dock)
        control_dock_layout.setContentsMargins(8, 6, 8, 6)
        control_dock_layout.setSpacing(8)

        self.header_record_btn = QtWidgets.QPushButton("⏺ Start")
        self.header_record_btn.setObjectName("dock_primary")
        self.header_record_btn.clicked.connect(self._toggle_recording)
        self.header_pause_btn = QtWidgets.QPushButton("⏸ Pause")
        self.header_pause_btn.setObjectName("dock_secondary")
        self.header_pause_btn.clicked.connect(self._toggle_pause)
        self.header_pause_btn.setEnabled(False)
        self.header_pause_btn.setVisible(False)
        self.header_pause_btn.setToolTip("Pause keeps live streams visible but stops file writes.")
        btn_fm = self.fontMetrics()
        rec_w = max(btn_fm.horizontalAdvance("⏺ Start"), btn_fm.horizontalAdvance("⏹ Stop")) + 38
        pause_w = max(btn_fm.horizontalAdvance("⏸ Pause"), btn_fm.horizontalAdvance("▶ Resume")) + 34
        self.header_record_btn.setMinimumWidth(rec_w)
        self.header_pause_btn.setMinimumWidth(pause_w)
        control_dock_layout.addWidget(self.header_pause_btn)
        control_dock_layout.addWidget(self.header_record_btn)
        self.control_dock.raise_()

        self.fps_overlay_label = QtWidgets.QLabel(root)
        self.fps_overlay_label.setObjectName("fps_overlay")
        self.fps_overlay_label.setStyleSheet(
            "QLabel#fps_overlay{background-color:rgba(10,17,30,195);"
            "border:1px solid #334155;border-radius:8px;color:#dbe5f4;padding:4px 8px;font-weight:700;}"
        )
        self.fps_overlay_label.setVisible(False)
        self.fps_overlay_label.raise_()

        self.controls_scroll = QtWidgets.QScrollArea(root)
        self.controls_scroll.setWidgetResizable(True)
        self.controls_scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.controls_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.controls_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.controls_scroll.setWidget(controls)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal, root)
        self.main_splitter.addWidget(self.controls_scroll)
        self.main_splitter.addWidget(charts)
        self.main_splitter.setStretchFactor(0, 0)
        self.main_splitter.setStretchFactor(1, 1)
        self.main_splitter.setSizes([420, 1140])
        self.main_splitter.splitterMoved.connect(
            lambda *_: (self._save_settings(), self._reposition_sidebar_handle(), self._reposition_control_dock())
        )
        main_layout.addWidget(self.main_splitter, 1)

        self.sidebar_handle_btn = QtWidgets.QToolButton(root)
        self.sidebar_handle_btn.setObjectName("sidebar_handle")
        self.sidebar_handle_btn.setCursor(QtCore.Qt.PointingHandCursor)
        self.sidebar_handle_btn.setToolTip("Show or hide sidebar.")
        self.sidebar_handle_btn.clicked.connect(self._toggle_sidebar)
        self.sidebar_handle_btn.raise_()
        self._reposition_sidebar_handle()

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
        self._set_record_button_state()

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
            "auto_connect_on_startup": bool(self.auto_connect_on_startup),
            "auto_reconnect_enabled": bool(self.connection_mgr.auto_reconnect_enabled),
            "auto_reconnect_interval_ms": int(self.connection_mgr.auto_reconnect_interval_ms),
            "recording_disconnect_mode": str(self.recording_disconnect_mode),
            "recording_disconnect_grace_ms": int(self.recording_disconnect_grace_ms),
            "startup_window_mode": str(self.startup_window_mode),
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
            "show_fps_overlay": bool(self.show_fps_overlay),
            "combine_hr_rr_chart": bool(self.combine_hr_rr_chart),
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
        self.auto_connect_on_startup = bool(data.get("auto_connect_on_startup", True))
        self.connection_mgr.auto_reconnect_enabled = bool(data.get("auto_reconnect_enabled", True))
        try:
            reconnect_ms = int(data.get("auto_reconnect_interval_ms", self.connection_mgr.auto_reconnect_interval_ms))
        except Exception:
            reconnect_ms = self.connection_mgr.auto_reconnect_interval_ms
        self.connection_mgr.auto_reconnect_interval_ms = max(1000, min(30000, int(reconnect_ms)))
        self.auto_reconnect_timer.setInterval(self.connection_mgr.auto_reconnect_interval_ms)
        mode = str(data.get("recording_disconnect_mode") or "pause_then_stop").strip().lower()
        if mode not in ("pause_then_stop", "stop_immediately", "pause_indefinitely"):
            mode = "pause_then_stop"
        self.recording_disconnect_mode = mode
        try:
            grace_ms = int(data.get("recording_disconnect_grace_ms", self.recording_disconnect_grace_ms))
        except Exception:
            grace_ms = self.recording_disconnect_grace_ms
        self.recording_disconnect_grace_ms = max(10000, min(3600000, int(grace_ms)))
        startup_mode = str(data.get("startup_window_mode") or "remember_last").strip().lower()
        if startup_mode not in ("remember_last", "normal", "maximized", "fullscreen"):
            startup_mode = "remember_last"
        self.startup_window_mode = startup_mode
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
        fps_mode = str(data.get("render_fps_mode") or "manual").strip().lower()
        fps_manual_raw = data.get("render_fps_manual", self.render_fps_manual)
        try:
            fps_manual = int(fps_manual_raw)
        except Exception:
            fps_manual = self.render_fps_manual
        self.render_fps_manual = max(1, min(240, int(fps_manual)))
        self.render_fps_mode = "manual" if fps_mode == "manual" else "auto"
        self.show_fps_overlay = bool(data.get("show_fps_overlay", False))
        self.combine_hr_rr_chart = bool(data.get("combine_hr_rr_chart", True))
        self.charts.set_combine_hr_rr(bool(self.combine_hr_rr_chart))
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
        if self.startup_window_mode == "remember_last":
            if state == "fullscreen":
                self.showFullScreen()
            elif state == "maximized":
                self.showMaximized()
        elif self.startup_window_mode == "fullscreen":
            self.showFullScreen()
        elif self.startup_window_mode == "maximized":
            self.showMaximized()
        if bool(data.get("sidebar_collapsed", False)):
            self._set_sidebar_collapsed(True)
        if self.last_device_address:
            self._upsert_device(self.last_device_address, self.last_device_name, None)
            self._render_device_list()
        self.charts.set_active_keys(self._selected_chart_keys())
        self._refresh_live_labels()

    def _current_settings_payload(self) -> dict:
        return {
            "auto_connect_on_startup": bool(self.auto_connect_on_startup),
            "auto_reconnect_enabled": bool(self.connection_mgr.auto_reconnect_enabled),
            "auto_reconnect_interval_ms": int(self.connection_mgr.auto_reconnect_interval_ms),
            "recording_disconnect_mode": str(self.recording_disconnect_mode),
            "recording_disconnect_grace_ms": int(self.recording_disconnect_grace_ms),
            "render_fps_mode": str(self.render_fps_mode),
            "render_fps_manual": int(self.render_fps_manual),
            "show_fps_overlay": bool(self.show_fps_overlay),
            "combine_hr_rr_chart": bool(self.combine_hr_rr_chart),
            "auto_collapse_sidebar_on_record": bool(self.auto_collapse_sidebar_on_record),
            "startup_window_mode": str(self.startup_window_mode),
        }

    def _apply_settings_payload(self, payload: dict):
        self.auto_connect_on_startup = bool(payload.get("auto_connect_on_startup", True))
        self.connection_mgr.auto_reconnect_enabled = bool(payload.get("auto_reconnect_enabled", True))
        try:
            reconnect_ms = int(payload.get("auto_reconnect_interval_ms", 5000))
        except Exception:
            reconnect_ms = 5000
        self.connection_mgr.auto_reconnect_interval_ms = max(1000, min(30000, reconnect_ms))
        self.auto_reconnect_timer.setInterval(self.connection_mgr.auto_reconnect_interval_ms)
        if not self.connection_mgr.auto_reconnect_enabled:
            self._stop_auto_reconnect()

        mode = str(payload.get("recording_disconnect_mode") or "pause_then_stop").strip().lower()
        if mode not in ("pause_then_stop", "stop_immediately", "pause_indefinitely"):
            mode = "pause_then_stop"
        self.recording_disconnect_mode = mode
        try:
            grace_ms = int(payload.get("recording_disconnect_grace_ms", 300000))
        except Exception:
            grace_ms = 300000
        self.recording_disconnect_grace_ms = max(10000, min(3600000, grace_ms))

        fps_mode = str(payload.get("render_fps_mode") or "auto").strip().lower()
        self.render_fps_mode = "manual" if fps_mode == "manual" else "auto"
        try:
            fps_manual = int(payload.get("render_fps_manual", self.render_fps_manual))
        except Exception:
            fps_manual = self.render_fps_manual
        self.render_fps_manual = max(1, min(240, fps_manual))
        self.show_fps_overlay = bool(payload.get("show_fps_overlay", False))
        self.combine_hr_rr_chart = bool(payload.get("combine_hr_rr_chart", True))
        self.charts.set_combine_hr_rr(bool(self.combine_hr_rr_chart))
        self._sync_fps_selector_text()
        self._apply_render_fps()
        self.charts.set_active_keys(self._selected_chart_keys())

        self.auto_collapse_sidebar_on_record = bool(payload.get("auto_collapse_sidebar_on_record", True))
        startup_mode = str(payload.get("startup_window_mode") or "remember_last").strip().lower()
        if startup_mode not in ("remember_last", "normal", "maximized", "fullscreen"):
            startup_mode = "remember_last"
        self.startup_window_mode = startup_mode

    def _open_settings_window(self):
        dialog = SettingsWindow(self._current_settings_payload(), self)
        if dialog.exec() != QtWidgets.QDialog.Accepted:
            return
        self._apply_settings_payload(dialog.values())
        self._save_settings()

    def _auto_connect_on_startup(self):
        if not self.auto_connect_on_startup:
            return
        if (not self.connection_mgr.begin_connect_attempt(connected=self.connected)) or (not self.last_device_address):
            return
        self._recreate_engine_if_needed()
        state = self._current_live_state()
        self._connect_requested_live_cfg = self._normalize_live_cfg(state)
        self._append_log(f"Connecting (auto) to {self.last_device_address}...")
        self._set_status(f"Auto-connecting: {self.last_device_address}")
        self._set_connect_button_state()
        fut = self.engine.connect(
            self.last_device_address,
            hr_live_enabled=state["hr_enabled"],
            preview_pmd_measurements=state["pmd_measurements"],
        )

        def on_ok(info):
            self.connection_mgr.finish_connect_success()
            self._on_connected(info)

        def on_fail(_exc):
            self.connection_mgr.finish_connect_failure()
            self._set_connect_button_state()

        self._track_future(fut, on_ok, "Auto-connect failed", on_fail=on_fail)

    def _stop_auto_reconnect(self):
        self.auto_reconnect_timer.stop()
        self.connection_mgr.stop_auto_reconnect()

    def _cancel_recording_disconnect_grace(self):
        self.recording_disconnect_timer.stop()

    def _start_recording_disconnect_grace(self):
        if not self.recording:
            self._cancel_recording_disconnect_grace()
            return
        self.recording_disconnect_timer.start(int(self.recording_disconnect_grace_ms))
        secs = int(self.recording_disconnect_grace_ms // 1000)
        self._append_log(
            f"Recording paused due to disconnect. Waiting up to {secs}s for reconnect before auto-stop."
        )

    def _on_recording_disconnect_timeout(self):
        if self.connected:
            return
        if (not self.recording) or (not self.recording_paused):
            return
        secs = int(self.recording_disconnect_grace_ms // 1000)
        self._append_log(f"Recording auto-stopped after {secs}s offline.")
        self._set_status("Recording stopped after offline timeout")
        self._stop_auto_reconnect()
        self._stop_recording()

    def _schedule_auto_reconnect(self, address: str | None):
        if self._is_shutting_down:
            return
        if not self.connection_mgr.schedule_auto_reconnect(address):
            return
        if not self.auto_reconnect_timer.isActive():
            self.auto_reconnect_timer.start()
        QtCore.QTimer.singleShot(0, self._auto_reconnect_tick)

    def _auto_reconnect_tick(self):
        address = self.connection_mgr.next_auto_reconnect_address(
            connected=self.connected,
            is_shutting_down=self._is_shutting_down,
            fallback_address=self.last_device_address,
        )
        if not address:
            return
        now = time.monotonic()
        if now < float(self._adapter_recover_not_before_mono):
            self.connection_mgr.finish_auto_reconnect_failure()
            return
        if self._require_reconnect_scan:
            if self._reconnect_scan_inflight:
                self.connection_mgr.finish_auto_reconnect_failure()
                return
            self._reconnect_scan_inflight = True
            self._append_log("Auto-reconnect: scanning for device before reconnect...")
            self._set_status("Auto-reconnecting: scanning device...")
            scan_fut = self.engine.scan(timeout=4.0)

            def on_scan_ok(result):
                self._reconnect_scan_inflight = False
                found = any(str(d.get("address", "")).lower() == str(address).lower() for d in (result or []))
                if not found:
                    self.connection_mgr.finish_auto_reconnect_failure()
                    self._append_log("Auto-reconnect scan did not find target device; retrying.")
                    return
                self._require_reconnect_scan = False
                self._start_auto_reconnect_connect(address)

            def on_scan_fail(_exc):
                self._reconnect_scan_inflight = False
                self.connection_mgr.finish_auto_reconnect_failure()

            self._track_future(
                scan_fut,
                on_scan_ok,
                "Auto-reconnect scan failed",
                on_fail=on_scan_fail,
                show_dialog=False,
            )
            return
        self._start_auto_reconnect_connect(address)

    def _start_auto_reconnect_connect(self, address: str):
        self._recreate_engine_if_needed()
        state = self._current_live_state()
        self._connect_requested_live_cfg = self._normalize_live_cfg(state)
        self._set_connect_button_state()
        self._append_log(f"Auto-reconnect: connecting to {address}...")
        self._set_status(f"Auto-reconnecting: {address}")
        fut = self.engine.connect(
            address,
            hr_live_enabled=state["hr_enabled"],
            preview_pmd_measurements=state["pmd_measurements"],
        )

        def on_ok(info):
            self.connection_mgr.finish_connect_success()
            self._stop_auto_reconnect()
            self._missing_service_failure_streak = 0
            self._on_connected(info)

        def on_fail(exc):
            self.connection_mgr.finish_auto_reconnect_failure()
            self._handle_connect_failure(exc)
            self._set_connect_button_state()

        self._track_future(fut, on_ok, "Auto-reconnect failed", on_fail=on_fail, show_dialog=False)

    def _start_battery_polling(self):
        self.battery_poll_timer.stop()
        QtCore.QTimer.singleShot(10000, self._battery_poll_tick)
        self.battery_poll_timer.start()

    def _stop_battery_polling(self):
        self.battery_poll_timer.stop()
        self.battery_poll_inflight = False

    def _start_signal_polling(self):
        self.signal_poll_timer.stop()
        self.signal_poll_inflight = False
        QtCore.QTimer.singleShot(1500, self._signal_poll_tick)
        self.signal_poll_timer.start()

    def _stop_signal_polling(self):
        self.signal_poll_timer.stop()
        self.signal_poll_inflight = False

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

    def _signal_poll_tick(self):
        if (not self.connected) or self.signal_poll_inflight or (not self.last_device_address):
            return
        self.signal_poll_inflight = True
        fut = self.engine.scan_address_rssi(self.last_device_address, timeout=2.0)

        def on_ok(result):
            self.signal_poll_inflight = False
            if not result:
                return
            self._upsert_device(result.get("address"), result.get("name"), result.get("rssi"))
            self._refresh_live_labels()

        def on_fail(_exc):
            self.signal_poll_inflight = False

        self._track_future(fut, on_ok, "Signal quality scan warning", on_fail=on_fail, show_dialog=False)

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

    def _clear_layout(self, layout: QtWidgets.QLayout):
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            child_layout = item.layout()
            if widget is not None:
                widget.deleteLater()
            elif child_layout is not None:
                self._clear_layout(child_layout)

    def _refresh_recent_sessions_ui(self):
        if not hasattr(self, "recent_sessions_layout"):
            return
        self._clear_layout(self.recent_sessions_layout)
        try:
            sessions = self.session_repo.get_recent_sessions(limit=3)
        except Exception:
            sessions = []
        if not sessions:
            empty = QtWidgets.QLabel("No sessions yet. Record your first session to see history cards here.")
            empty.setStyleSheet("color:#94a3b8;")
            empty.setWordWrap(True)
            self.recent_sessions_layout.addWidget(empty)
            return
        for session in sessions:
            card = RecentSessionCard(session, self.recent_sessions_wrap)
            card.clicked_session.connect(self._open_session_details)
            self.recent_sessions_layout.addWidget(card)
        self.recent_sessions_layout.addStretch(1)

    def _reload_history_views(self):
        self._refresh_recent_sessions_ui()
        if self.history_window is None:
            return
        try:
            rows = self.session_repo.list_sessions(sort_by="start_ts", descending=True)
            profiles = self.session_repo.list_profiles()
        except Exception as exc:
            self._append_log(f"History read warning: {exc}")
            rows = []
            profiles = []
        self.history_window.set_profiles(profiles)
        self.history_window.set_rows(rows)

    def _refresh_history_index_async(self, *, show_dialog: bool = False):
        if self._history_refresh_inflight:
            return
        self._history_refresh_inflight = True
        fut = self._bg_executor.submit(self.session_repo.refresh_index)

        def on_ok(stats):
            self._history_refresh_inflight = False
            self._append_log(
                f"Session index refreshed: scanned={stats.get('scanned', 0)}, "
                f"updated={stats.get('updated', 0)}, removed={stats.get('removed', 0)}"
            )
            self._reload_history_views()

        def on_fail(_exc):
            self._history_refresh_inflight = False

        self._track_future(fut, on_ok, "Session history refresh failed", on_fail=on_fail, show_dialog=show_dialog)

    def _open_history_window(self):
        if self.history_window is None:
            self.history_window = SessionHistoryWindow(self)
            self.history_window.refresh_requested.connect(lambda: self._refresh_history_index_async(show_dialog=True))
            self.history_window.session_open_requested.connect(self._open_session_details)
            self.history_window.session_delete_requested.connect(self._delete_session_from_history)
            self.history_window.finished.connect(lambda _res: setattr(self, "history_window", None))
        self._refresh_history_index_async(show_dialog=False)
        self._reload_history_views()
        self.history_window.show()
        self.history_window.raise_()
        self.history_window.activateWindow()

    def _open_session_details(self, session_id: str):
        existing = self._details_windows.get(session_id)
        if existing is not None and existing.isVisible():
            existing.raise_()
            existing.activateWindow()
            return
        win = SessionDetailsWindow(self.session_repo, session_id, self)
        win.setAttribute(QtCore.Qt.WA_DeleteOnClose, True)

        def _cleanup(*_):
            self._details_windows.pop(session_id, None)

        win.destroyed.connect(_cleanup)
        self._details_windows[session_id] = win
        win.show()

    def _delete_session_from_history(self, session_id: str):
        session = self.session_repo.get_session(session_id)
        if session is None:
            QtWidgets.QMessageBox.warning(self, "Delete Session", "Session not found in index.")
            self._refresh_history_index_async(show_dialog=False)
            return

        sessions_root = (self.app_dir / "sessions").resolve()
        target = Path(session.session_path).resolve()
        try:
            target.relative_to(sessions_root)
        except Exception:
            QtWidgets.QMessageBox.warning(
                self,
                "Delete Session",
                "Refusing to delete path outside sessions directory.",
            )
            return
        if target == sessions_root:
            QtWidgets.QMessageBox.warning(self, "Delete Session", "Refusing to delete sessions root.")
            return
        if not target.exists():
            self._append_log(f"Delete session warning: path not found ({target}).")
            self._refresh_history_index_async(show_dialog=False)
            return

        try:
            shutil.rmtree(target)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Delete Session", f"Could not delete session: {exc}")
            return

        existing = self._details_windows.pop(session_id, None)
        if existing is not None:
            try:
                existing.close()
            except Exception:
                pass
        self._append_log(f"Session deleted: {session_id}")
        self._refresh_history_index_async(show_dialog=False)

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
        if self._live_battery_value is None:
            self.tile_battery.set_value("--", "%", "Device battery")
            self.tile_battery.set_accent("#64748b")
        else:
            b = int(self._live_battery_value)
            self.tile_battery.set_value(f"{b}", "%", "Device battery")
            if b <= 15:
                self.tile_battery.set_accent("#ef4444")
            elif b <= 30:
                self.tile_battery.set_accent("#f59e0b")
            else:
                self.tile_battery.set_accent("#22c55e")

        if self._live_hr_value is None:
            self.tile_hr.set_value("--", "BPM", "Live heart rate")
            self.tile_hr.set_accent("#64748b")
        else:
            hr = int(round(self._live_hr_value))
            self.tile_hr.set_value(f"{hr}", "BPM", "Live heart rate")
            if hr >= 170:
                self.tile_hr.set_accent("#ef4444")
            elif hr >= 140:
                self.tile_hr.set_accent("#f59e0b")
            elif hr >= 110:
                self.tile_hr.set_accent("#22d3ee")
            else:
                self.tile_hr.set_accent("#38bdf8")

        if self._live_rr_value is None:
            self.tile_rr.set_value("----", "ms", "Beat interval")
            self.tile_rr.set_accent("#64748b")
        else:
            rr = int(round(self._live_rr_value))
            self.tile_rr.set_value(f"{rr}", "ms", "Beat interval")
            if rr < 450 or rr > 1400:
                self.tile_rr.set_accent("#f59e0b")
            else:
                self.tile_rr.set_accent("#06b6d4")

        self.tile_kcal.set_value(kcal_txt, "kcal", "Estimated total")
        self.tile_kcal.set_accent("#fb7185")
        self.tile_rec.set_value(dur_txt, "", "Recording duration")
        if self.recording and self.recording_paused and (not self.connected):
            self.tile_rec.set_accent("#ef4444")
            self.tile_rec.set_value(dur_txt, "", "Recording paused (device disconnected)")
        elif self.recording and not self.recording_paused:
            self.tile_rec.set_accent("#22c55e")
        elif self.recording and self.recording_paused:
            self.tile_rec.set_accent("#f59e0b")
        else:
            self.tile_rec.set_accent("#64748b")
        self._update_signal_quality_tile()
        self._update_fps_overlay()

    def _connected_device_rssi(self):
        if not self.last_device_address:
            return None
        target = str(self.last_device_address).upper()
        for d in self.devices:
            if str(d.get("address") or "").upper() == target:
                rssi = d.get("rssi")
                if rssi is None:
                    return None
                try:
                    return float(rssi)
                except Exception:
                    return None
        return None

    def _update_signal_quality_tile(self):
        if not self.connected:
            self.tile_signal.set_value("Offline", "", "Device disconnected")
            self.tile_signal.set_accent("#ef4444")
            return
        rssi = self._connected_device_rssi()
        if rssi is None:
            age = 9999.0
            if self._last_signal_event_mono > 0.0:
                age = max(0.0, time.monotonic() - float(self._last_signal_event_mono))
            if age <= 2.0:
                self.tile_signal.set_value("Stable", "", "Live stream healthy")
                self.tile_signal.set_accent("#22c55e")
            elif age <= 5.0:
                self.tile_signal.set_value("Fair", "", "Intermittent stream")
                self.tile_signal.set_accent("#f59e0b")
            else:
                self.tile_signal.set_value("Weak", "", "No recent stream data")
                self.tile_signal.set_accent("#ef4444")
            return
        if rssi >= -65:
            quality, accent = "Excellent", "#22c55e"
        elif rssi >= -75:
            quality, accent = "Good", "#84cc16"
        elif rssi >= -85:
            quality, accent = "Fair", "#f59e0b"
        else:
            quality, accent = "Poor", "#ef4444"
        self.tile_signal.set_value(quality, "", f"Strong link ({int(round(rssi))} dBm)" if quality in ("Excellent", "Good") else f"Link ({int(round(rssi))} dBm)")
        self.tile_signal.set_accent(accent)

    def _update_fps_overlay(self):
        if not hasattr(self, "fps_overlay_label"):
            return
        visible = bool(self.show_fps_overlay) and (not self.isMinimized())
        self.fps_overlay_label.setVisible(visible)
        if not visible:
            return
        fps_txt = "--" if self._fps_value <= 0.0 else f"{self._fps_value:0.1f}"
        self.fps_overlay_label.setText(f"FPS {fps_txt}/{int(self.render_fps)}")
        self.fps_overlay_label.adjustSize()
        self.fps_overlay_label.raise_()
        self._reposition_fps_overlay()

    def _reposition_fps_overlay(self):
        if not hasattr(self, "fps_overlay_label"):
            return
        if not self.fps_overlay_label.isVisible():
            return
        root = getattr(self, "_root_container", None)
        if root is None:
            return
        margin = 12
        self.fps_overlay_label.adjustSize()
        hint = self.fps_overlay_label.sizeHint()
        x = max(margin, root.width() - hint.width() - margin)
        y = margin
        self.fps_overlay_label.move(x, y)
        self.fps_overlay_label.raise_()

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
            self._sync_render_timer_with_window_state()
        self._refresh_live_labels()

    def _sync_render_timer_with_window_state(self):
        if not hasattr(self, "render_timer") or self.render_timer is None:
            return
        if self._is_shutting_down:
            self.render_timer.stop()
            return
        if self.isMinimized():
            self.render_timer.stop()
            self._update_fps_overlay()
            return
        if not self.render_timer.isActive():
            self.render_timer.start()
        self._update_fps_overlay()

    def changeEvent(self, event: QtCore.QEvent):
        super().changeEvent(event)
        if event.type() == QtCore.QEvent.Type.WindowStateChange:
            self._sync_render_timer_with_window_state()
            self._reposition_sidebar_handle()
            self._reposition_control_dock()
            self._reposition_fps_overlay()

    def showEvent(self, event: QtGui.QShowEvent):
        super().showEvent(event)
        if not self._sidebar_handle_bootstrap_done:
            self._sidebar_handle_bootstrap_done = True
            self._schedule_sidebar_handle_reposition()

    def resizeEvent(self, event: QtGui.QResizeEvent):
        super().resizeEvent(event)
        self._reposition_sidebar_handle()
        self._reposition_control_dock()
        self._reposition_fps_overlay()

    def _schedule_sidebar_handle_reposition(self):
        QtCore.QTimer.singleShot(0, self._reposition_sidebar_handle)
        QtCore.QTimer.singleShot(60, self._reposition_sidebar_handle)
        QtCore.QTimer.singleShot(0, self._reposition_control_dock)
        QtCore.QTimer.singleShot(60, self._reposition_control_dock)
        QtCore.QTimer.singleShot(0, self._reposition_fps_overlay)
        QtCore.QTimer.singleShot(60, self._reposition_fps_overlay)

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
        self.connect_btn.setEnabled(self.connection_mgr.should_enable_connect_button(self.connected))

    def _set_record_button_state(self):
        rec_btn = self.header_record_btn
        pause_btn = self.header_pause_btn
        if self.recording:
            rec_btn.setText("⏹ Stop")
            rec_btn.setEnabled(True)
            pause_btn.setVisible(True)
            pause_btn.setEnabled(True)
            pause_btn.setText("▶ Resume" if self.recording_paused else "⏸ Pause")
        else:
            rec_btn.setText("⏺ Start")
            rec_btn.setEnabled(bool(self.connected))
            pause_btn.setVisible(False)
            pause_btn.setEnabled(False)
            pause_btn.setText("⏸ Pause")
        self._reposition_control_dock()

    def _toggle_sidebar(self):
        self._set_sidebar_collapsed(not self.sidebar_collapsed)

    def _reposition_sidebar_handle(self):
        if not hasattr(self, "sidebar_handle_btn") or self.sidebar_handle_btn is None:
            return
        if not hasattr(self, "_root_container") or self._root_container is None:
            return
        splitter_geom = self.main_splitter.geometry()
        total = max(1, self.main_splitter.width())
        sizes = self.main_splitter.sizes()
        left_now = int(sizes[0]) if sizes else 0
        handle_w = 24
        handle_h = 38
        x_local = max(2, min(total - (handle_w + 2), left_now - (handle_w // 2)))
        y_local = max(8, int((self.main_splitter.height() * 0.5) - (handle_h // 2)))
        x = int(splitter_geom.x()) + int(x_local)
        y = int(splitter_geom.y()) + int(y_local)
        x = max(2, min(self._root_container.width() - (handle_w + 2), x))
        y = max(2, min(self._root_container.height() - (handle_h + 2), y))
        self.sidebar_handle_btn.setGeometry(x, y, handle_w, handle_h)
        self.sidebar_handle_btn.setText("▶" if self.sidebar_collapsed else "◀")
        self.sidebar_handle_btn.raise_()

    def _reposition_control_dock(self):
        if not hasattr(self, "control_dock") or self.control_dock is None:
            return
        if not hasattr(self, "tile_battery") or self.tile_battery is None:
            return
        parent = self.control_dock.parentWidget()
        if parent is None:
            return
        self.control_dock.adjustSize()
        w = int(self.control_dock.sizeHint().width())
        h = int(self.control_dock.sizeHint().height())
        x = max(8, parent.width() - w - 10)
        y = max(8, parent.height() - h - 10)
        self.control_dock.setGeometry(x, y, w, h)
        self.control_dock.raise_()

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
        self._reposition_sidebar_handle()
        self._reposition_control_dock()
        self._reposition_fps_overlay()
        QtCore.QTimer.singleShot(0, self._reposition_control_dock)
        QtCore.QTimer.singleShot(60, self._reposition_control_dock)
        QtCore.QTimer.singleShot(0, self._reposition_fps_overlay)
        QtCore.QTimer.singleShot(60, self._reposition_fps_overlay)
        self._save_settings()

    def _track_future(self, fut, on_ok, on_err, on_fail=None, show_dialog: bool = True):
        self._pending_calls.append((fut, on_ok, on_err, on_fail, bool(show_dialog)))

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
        if not self.connection_mgr.begin_connect_attempt(connected=self.connected):
            return
        self._stop_auto_reconnect()
        self._recreate_engine_if_needed()
        address = self._selected_address()
        if not address:
            self.connection_mgr.finish_connect_failure()
            self._set_connect_button_state()
            QtWidgets.QMessageBox.warning(self, "No selection", "Select a device first.")
            return
        state = self._current_live_state()
        self._connect_requested_live_cfg = self._normalize_live_cfg(state)
        self._set_connect_button_state()
        self._append_log(f"Connecting to {address}...")
        self._set_status(f"Connecting to {address}...")
        fut = self.engine.connect(
            address,
            hr_live_enabled=state["hr_enabled"],
            preview_pmd_measurements=state["pmd_measurements"],
        )

        def on_ok(info):
            self.connection_mgr.finish_connect_success()
            self._on_connected(info)

        def on_fail(exc):
            self.connection_mgr.finish_connect_failure()
            self._handle_connect_failure(exc)
            self._set_connect_button_state()

        self._track_future(fut, on_ok, "Connect failed", on_fail=on_fail)

    def _on_connected(self, info):
        self.connection_mgr.finish_connect_success()
        self._stop_auto_reconnect()
        self._cancel_recording_disconnect_grace()
        self._service_unavailable_retries = 0
        self._missing_service_failure_streak = 0
        self._engine_reset_required = False
        self._engine_reset_reason = None
        self._require_reconnect_scan = False
        self._adapter_recover_not_before_mono = 0.0
        self.connected = True
        self.available_measurements = set(info.get("available_measurements") or [])
        self._set_record_button_state()
        self._set_connect_button_state()
        self._refresh_measurement_availability()
        self._set_status(f"Connected: {info.get('address')}")
        self.last_device_address = info.get("address")
        self.last_device_name = info.get("name")
        self._upsert_device(self.last_device_address, self.last_device_name, info.get("rssi"))
        self._render_device_list()
        self._last_signal_event_mono = time.monotonic()
        self._refresh_live_labels()
        battery = info.get("battery")
        if battery is not None:
            self._live_battery_value = battery
            self._refresh_live_labels()
        self._start_battery_polling()
        self._start_signal_polling()
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
        self.connection_mgr.request_user_disconnect()
        self._stop_auto_reconnect()
        self._cancel_recording_disconnect_grace()
        fut = self.engine.disconnect()

        def on_ok(_):
            self.connected = False
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self.available_measurements = set()
            self._stop_battery_polling()
            self._stop_signal_polling()
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._set_connect_button_state()
            self._refresh_measurement_availability()
            self._set_status("Disconnected")
            self._live_battery_value = None
            self._live_hr_value = None
            self._live_rr_value = None
            self._last_signal_event_mono = 0.0
            self._reset_ecg_stream_state()
            self._reset_playback_stream_state()
            self._last_applied_live_cfg = None
            self._connect_requested_live_cfg = None
            self._refresh_live_labels()
            self._save_settings()

        self._track_future(fut, on_ok, "Disconnect warning")

    def _mark_engine_reset_required(self, reason: str):
        self._engine_reset_required = True
        self._engine_reset_reason = str(reason)

    def _looks_like_powered_off(self, exc: Exception) -> bool:
        low = str(exc).lower()
        return ("powered_off" in low) or ("bluetooth radio is not powered on" in low)

    def _looks_like_missing_required_services(self, exc: Exception) -> bool:
        low = str(exc).lower()
        if "hr service unavailable" in low:
            return True
        if "pmd service unavailable" in low:
            return True
        return False

    def _handle_connect_failure(self, exc: Exception):
        if self._looks_like_powered_off(exc):
            self._missing_service_failure_streak = 0
            self._require_reconnect_scan = True
            self._adapter_recover_not_before_mono = time.monotonic() + float(self._adapter_recovery_delay_s)
            self._mark_engine_reset_required("bluetooth adapter was powered off")
            return
        if self._looks_like_missing_required_services(exc):
            self._service_unavailable_retries += 1
            self._missing_service_failure_streak += 1
            if self._service_unavailable_retries >= self._service_unavailable_reset_threshold:
                self._service_unavailable_retries = 0
                self._mark_engine_reset_required("required GATT services unavailable after reconnect")
            if self._missing_service_failure_streak >= self._missing_service_abort_threshold:
                self._stop_auto_reconnect()
                self._append_log(
                    "Auto-reconnect paused after repeated missing HR/PMD services. "
                    "Restart Bluetooth adapter or restart app, then connect again."
                )
                self._set_status("Reconnect paused (GATT incomplete). Restart Bluetooth/app.")
            return
        self._service_unavailable_retries = 0
        self._missing_service_failure_streak = 0

    def _recreate_engine_if_needed(self):
        if (not self._engine_reset_required) or self._is_shutting_down:
            return
        now = time.monotonic()
        if (now - float(self._last_engine_recreate_mono)) < self._engine_recreate_min_interval_s:
            return
        reason = self._engine_reset_reason or "runtime reset"
        self._last_engine_recreate_mono = now

        # Drop unresolved futures bound to the old engine instance before shutdown.
        keep = []
        for item in self._pending_calls:
            if len(item) == 4:
                fut, on_ok, on_err, on_fail = item
                show_dialog = True
            else:
                fut, on_ok, on_err, on_fail, show_dialog = item
            if fut.done():
                keep.append((fut, on_ok, on_err, on_fail, show_dialog))
                continue
            try:
                fut.cancel()
            except Exception:
                pass
        self._pending_calls = keep

        try:
            self.engine.shutdown()
        except Exception as exc:
            self._append_log(f"BLE runtime shutdown warning: {exc}")
        self.engine = BleakHeartEngine(self.events)
        self._engine_reset_required = False
        self._engine_reset_reason = None
        self._append_log(f"BLE runtime recreated ({reason}).")

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
        self._cancel_recording_disconnect_grace()
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
        for item in self._pending_calls:
            if len(item) == 4:
                fut, on_ok, on_err, on_fail = item
                show_dialog = True
            else:
                fut, on_ok, on_err, on_fail, show_dialog = item
            if not fut.done():
                keep.append((fut, on_ok, on_err, on_fail, show_dialog))
                continue
            try:
                on_ok(fut.result())
            except Exception as exc:
                if on_fail is not None:
                    try:
                        on_fail(exc)
                    except Exception:
                        pass
                if show_dialog:
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
            self.recording = True
            self.recording_paused = True
            if self._record_last_resume_mono is not None:
                self._record_elapsed_s += max(0.0, time.monotonic() - float(self._record_last_resume_mono))
            self._record_last_resume_mono = None
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "recording_resumed":
            self.recording = True
            self.recording_paused = False
            self._record_last_resume_mono = time.monotonic()
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "recording_stopped":
            self._cancel_recording_disconnect_grace()
            self.recording = False
            self.recording_paused = False
            self._record_elapsed_s = 0.0
            self._record_last_resume_mono = None
            self._refresh_history_index_async(show_dialog=False)
            self._set_controls_locked(False)
            self._set_record_button_state()
            self._refresh_live_labels()
        elif etype == "disconnected":
            was_connected = bool(self.connected)
            reconnect_address = self.connection_mgr.handle_disconnect_event(
                was_connected=was_connected,
                is_shutting_down=self._is_shutting_down,
                last_address=self.last_device_address,
            )
            had_recording = bool(self.recording)
            preserve_recording_state = had_recording and (self.recording_disconnect_mode != "stop_immediately")
            self.connected = False
            if not preserve_recording_state:
                self._cancel_recording_disconnect_grace()
                self.recording = False
                self.recording_paused = False
                self._record_elapsed_s = 0.0
                self._record_last_resume_mono = None
            else:
                self.recording = True
                self.recording_paused = True
                if self.recording_disconnect_mode == "pause_then_stop":
                    self._start_recording_disconnect_grace()
                else:
                    self._cancel_recording_disconnect_grace()
            self.available_measurements = set()
            self._last_signal_event_mono = 0.0
            self._reset_ecg_stream_state()
            self._reset_playback_stream_state()
            self._set_controls_locked(False if not preserve_recording_state else True)
            self._set_record_button_state()
            self._set_connect_button_state()
            self._refresh_measurement_availability()
            self._stop_signal_polling()
            if preserve_recording_state:
                if self.recording_disconnect_mode == "pause_indefinitely":
                    self._set_status("Disconnected (recording paused, waiting reconnect)")
                else:
                    self._set_status("Disconnected (recording paused)")
            else:
                self._set_status("Disconnected")
            if reconnect_address:
                self._append_log("Connection lost unexpectedly; auto-reconnect enabled.")
                self._schedule_auto_reconnect(reconnect_address)
            if had_recording and self.recording_disconnect_mode == "stop_immediately":
                self._append_log("Recording stopped immediately due to disconnect policy.")
                self._stop_recording()
        elif etype == "hr_sample":
            t = event.get("timestamp_s")
            hr = event.get("heart_rate")
            rr_vals = event.get("rr_values") or []
            if hr is not None and t is not None:
                self._last_signal_event_mono = now_monotonic
                self._enqueue_hr_sample(t, hr, now_monotonic)
                self._live_hr_value = hr
                self._refresh_live_labels()
                if self.recording and (not self.recording_paused):
                    self._update_kcal_estimate(float(t), float(hr))
            if rr_vals and t is not None:
                self._last_signal_event_mono = now_monotonic
                self._enqueue_rr_samples(t, rr_vals, now_monotonic)
                self._live_rr_value = rr_vals[-1]
                self._refresh_live_labels()
        elif etype == "ecg_samples":
            end_ts = event.get("end_timestamp_s")
            samples = event.get("samples") or []
            sr = float(event.get("sample_rate") or 130.0)
            if samples and end_ts is not None:
                self._last_signal_event_mono = now_monotonic
                self._enqueue_ecg_chunk(end_ts, samples, sr, now_monotonic)
        elif etype == "acc_samples":
            end_ts = event.get("end_timestamp_s")
            samples = event.get("samples") or []
            sr = float(event.get("sample_rate") or 200.0)
            if samples and end_ts is not None:
                self._last_signal_event_mono = now_monotonic
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
                self._update_fps_overlay()
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
            self._update_fps_overlay()

    def _shutdown_gracefully(self):
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        self.connection_mgr.begin_shutdown()
        self._stop_auto_reconnect()
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
        self._stop_signal_polling()
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
        try:
            self._bg_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    def closeEvent(self, event: QtGui.QCloseEvent):
        self._shutdown_gracefully()
        super().closeEvent(event)


def main():
    if sys.platform.startswith("win"):
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("bleakheart.ui")
        except Exception:
            pass

    os.environ.setdefault("QT_OPENGL", "desktop")
    os.environ.setdefault("QSG_RHI_BACKEND", "opengl")
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.ApplicationAttribute.AA_UseDesktopOpenGL)

    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("BleakHeart UI")
    icon_path = Path(__file__).resolve().parents[2] / "assets" / "app_icon.png"
    if icon_path.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))
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
