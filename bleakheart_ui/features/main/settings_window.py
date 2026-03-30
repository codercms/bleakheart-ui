from PySide6 import QtCore, QtWidgets


class SettingsWindow(QtWidgets.QDialog):
    def __init__(self, current: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.resize(700, 560)

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        title = QtWidgets.QLabel("Application Settings", self)
        title.setStyleSheet("color:#e2e8f0;font-weight:700;font-size:15px;")
        root.addWidget(title)

        # Connection defaults
        conn_group = QtWidgets.QGroupBox("Connection", self)
        conn_layout = QtWidgets.QFormLayout(conn_group)
        conn_layout.setLabelAlignment(QtCore.Qt.AlignLeft)
        conn_layout.setFormAlignment(QtCore.Qt.AlignTop)
        conn_layout.setHorizontalSpacing(12)
        conn_layout.setVerticalSpacing(8)

        self.auto_connect_on_startup = QtWidgets.QCheckBox("Auto-connect to last device on app startup", conn_group)
        self.auto_connect_on_startup.setToolTip("When enabled, app will try connecting to the last successful device after launch.")
        conn_layout.addRow(self.auto_connect_on_startup)

        self.auto_reconnect_enabled = QtWidgets.QCheckBox("Auto-reconnect after unexpected disconnect", conn_group)
        self.auto_reconnect_enabled.setToolTip("When enabled, app retries connection after unplanned disconnects.")
        conn_layout.addRow(self.auto_reconnect_enabled)

        self.auto_reconnect_interval_s = QtWidgets.QSpinBox(conn_group)
        self.auto_reconnect_interval_s.setRange(1, 30)
        self.auto_reconnect_interval_s.setSuffix(" s")
        self.auto_reconnect_interval_s.setToolTip("Delay between reconnect retries.")
        conn_layout.addRow("Reconnect retry interval", self.auto_reconnect_interval_s)
        root.addWidget(conn_group)

        # Recording disconnect behavior
        rec_group = QtWidgets.QGroupBox("Recording Disconnect", self)
        rec_layout = QtWidgets.QFormLayout(rec_group)
        rec_layout.setHorizontalSpacing(12)
        rec_layout.setVerticalSpacing(8)

        self.recording_disconnect_mode = QtWidgets.QComboBox(rec_group)
        self.recording_disconnect_mode.addItem("Pause and auto-stop after timeout", "pause_then_stop")
        self.recording_disconnect_mode.addItem("Stop recording immediately", "stop_immediately")
        self.recording_disconnect_mode.addItem("Pause and wait indefinitely", "pause_indefinitely")
        self.recording_disconnect_mode.setToolTip("What to do when connection drops while recording is active.")
        rec_layout.addRow("On disconnect during recording", self.recording_disconnect_mode)

        self.recording_disconnect_grace_s = QtWidgets.QSpinBox(rec_group)
        self.recording_disconnect_grace_s.setRange(10, 3600)
        self.recording_disconnect_grace_s.setSuffix(" s")
        self.recording_disconnect_grace_s.setToolTip("Used only for pause+timeout mode.")
        rec_layout.addRow("Auto-stop timeout", self.recording_disconnect_grace_s)
        root.addWidget(rec_group)

        # UI/performance defaults
        ui_group = QtWidgets.QGroupBox("UI & Performance", self)
        ui_layout = QtWidgets.QFormLayout(ui_group)
        ui_layout.setHorizontalSpacing(12)
        ui_layout.setVerticalSpacing(8)

        self.render_fps_mode = QtWidgets.QComboBox(ui_group)
        self.render_fps_mode.addItem("Auto", "auto")
        self.render_fps_mode.addItem("Manual", "manual")
        ui_layout.addRow("Render FPS mode", self.render_fps_mode)

        self.render_fps_manual = QtWidgets.QSpinBox(ui_group)
        self.render_fps_manual.setRange(1, 240)
        self.render_fps_manual.setSuffix(" FPS")
        self.render_fps_manual.setToolTip("Used only in manual FPS mode.")
        ui_layout.addRow("Manual FPS cap", self.render_fps_manual)
        self.show_fps_overlay = QtWidgets.QCheckBox("Show FPS overlay (advanced)", ui_group)
        self.show_fps_overlay.setToolTip("Displays live FPS over content. Useful for tuning only.")
        ui_layout.addRow(self.show_fps_overlay)
        self.combine_hr_rr_chart = QtWidgets.QCheckBox("Combine HR + RR chart (dual-axis)", ui_group)
        self.combine_hr_rr_chart.setToolTip("Shows Heart Rate and RR Interval on one chart with separate Y axes.")
        ui_layout.addRow(self.combine_hr_rr_chart)

        self.auto_collapse_sidebar_on_record = QtWidgets.QCheckBox("Auto-collapse sidebar when recording starts", ui_group)
        ui_layout.addRow(self.auto_collapse_sidebar_on_record)

        self.startup_window_mode = QtWidgets.QComboBox(ui_group)
        self.startup_window_mode.addItem("Remember last", "remember_last")
        self.startup_window_mode.addItem("Normal", "normal")
        self.startup_window_mode.addItem("Maximized", "maximized")
        self.startup_window_mode.addItem("Fullscreen", "fullscreen")
        ui_layout.addRow("Startup window mode", self.startup_window_mode)
        root.addWidget(ui_group)

        root.addStretch(1)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel, parent=self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

        self.recording_disconnect_mode.currentIndexChanged.connect(self._sync_dependent_controls)
        self.render_fps_mode.currentIndexChanged.connect(self._sync_dependent_controls)
        self.auto_reconnect_enabled.toggled.connect(self._sync_dependent_controls)

        self._set_from(current or {})
        self._sync_dependent_controls()

    def _set_from(self, current: dict):
        self.auto_connect_on_startup.setChecked(bool(current.get("auto_connect_on_startup", True)))
        self.auto_reconnect_enabled.setChecked(bool(current.get("auto_reconnect_enabled", True)))
        interval_ms = int(current.get("auto_reconnect_interval_ms", 5000) or 5000)
        self.auto_reconnect_interval_s.setValue(max(1, min(30, int(round(interval_ms / 1000.0)))))

        mode = str(current.get("recording_disconnect_mode") or "pause_then_stop")
        idx = self.recording_disconnect_mode.findData(mode)
        self.recording_disconnect_mode.setCurrentIndex(idx if idx >= 0 else 0)
        grace_ms = int(current.get("recording_disconnect_grace_ms", 300000) or 300000)
        self.recording_disconnect_grace_s.setValue(max(10, min(3600, int(round(grace_ms / 1000.0)))))

        fps_mode = str(current.get("render_fps_mode") or "manual")
        idx = self.render_fps_mode.findData(fps_mode)
        manual_idx = self.render_fps_mode.findData("manual")
        self.render_fps_mode.setCurrentIndex(idx if idx >= 0 else max(0, manual_idx))
        fps_manual = int(current.get("render_fps_manual", 30) or 30)
        self.render_fps_manual.setValue(max(1, min(240, fps_manual)))
        self.show_fps_overlay.setChecked(bool(current.get("show_fps_overlay", False)))
        self.combine_hr_rr_chart.setChecked(bool(current.get("combine_hr_rr_chart", True)))

        self.auto_collapse_sidebar_on_record.setChecked(bool(current.get("auto_collapse_sidebar_on_record", True)))
        startup_mode = str(current.get("startup_window_mode") or "remember_last")
        idx = self.startup_window_mode.findData(startup_mode)
        self.startup_window_mode.setCurrentIndex(idx if idx >= 0 else 0)

    def _sync_dependent_controls(self):
        reconnect_enabled = bool(self.auto_reconnect_enabled.isChecked())
        self.auto_reconnect_interval_s.setEnabled(reconnect_enabled)

        rec_mode = str(self.recording_disconnect_mode.currentData() or "pause_then_stop")
        self.recording_disconnect_grace_s.setEnabled(rec_mode == "pause_then_stop")

        fps_mode = str(self.render_fps_mode.currentData() or "auto")
        self.render_fps_manual.setEnabled(fps_mode == "manual")

    def values(self) -> dict:
        return {
            "auto_connect_on_startup": bool(self.auto_connect_on_startup.isChecked()),
            "auto_reconnect_enabled": bool(self.auto_reconnect_enabled.isChecked()),
            "auto_reconnect_interval_ms": int(self.auto_reconnect_interval_s.value()) * 1000,
            "recording_disconnect_mode": str(self.recording_disconnect_mode.currentData() or "pause_then_stop"),
            "recording_disconnect_grace_ms": int(self.recording_disconnect_grace_s.value()) * 1000,
            "render_fps_mode": str(self.render_fps_mode.currentData() or "auto"),
            "render_fps_manual": int(self.render_fps_manual.value()),
            "show_fps_overlay": bool(self.show_fps_overlay.isChecked()),
            "combine_hr_rr_chart": bool(self.combine_hr_rr_chart.isChecked()),
            "auto_collapse_sidebar_on_record": bool(self.auto_collapse_sidebar_on_record.isChecked()),
            "startup_window_mode": str(self.startup_window_mode.currentData() or "remember_last"),
        }
