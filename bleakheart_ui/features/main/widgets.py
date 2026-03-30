import queue

from PySide6 import QtCore, QtGui, QtWidgets


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


class GuardedComboBox(QtWidgets.QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFocusPolicy(QtCore.Qt.StrongFocus)

    # Prevent accidental value changes while wheel-scrolling the sidebar.
    def wheelEvent(self, event: QtGui.QWheelEvent):
        # Accept wheel only when user intentionally opened the dropdown.
        if self.view().isVisible():
            super().wheelEvent(event)
            return
        event.ignore()


class ContainedScrollArea(QtWidgets.QScrollArea):
    # Wheel events are fully handled here to avoid scroll-chain to parent containers.
    def wheelEvent(self, event: QtGui.QWheelEvent):
        bar = self.verticalScrollBar()
        if bar is None:
            event.accept()
            return

        pixel_delta = event.pixelDelta().y()
        angle_delta = event.angleDelta().y()

        if pixel_delta != 0:
            new_value = bar.value() - int(pixel_delta)
        elif angle_delta != 0:
            line_steps = float(angle_delta) / 120.0
            step_px = max(1, int(bar.singleStep())) * 3
            new_value = bar.value() - int(round(line_steps * step_px))
        else:
            event.accept()
            return

        new_value = max(bar.minimum(), min(bar.maximum(), int(new_value)))
        bar.setValue(new_value)
        event.accept()


class TelemetryTile(QtWidgets.QFrame):
    def __init__(self, title: str, *, accent: str = "#38bdf8", parent=None):
        super().__init__(parent)
        self.setObjectName("telemetry_tile")
        self._accent = str(accent)
        self._apply_style()
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(3)

        self.title_label = QtWidgets.QLabel(title, self)
        self.title_label.setStyleSheet("color:#9fb2cd;font-weight:650;letter-spacing:0.5px;font-size:11pt;")
        title_font = QtGui.QFont(self.title_label.font())
        title_font.setPointSizeF(11.0)
        self.title_label.setFont(title_font)

        value_row = QtWidgets.QHBoxLayout()
        value_row.setContentsMargins(0, 0, 0, 0)
        value_row.setSpacing(6)
        self.value_label = QtWidgets.QLabel("--", self)
        self.value_label.setStyleSheet("color:#f3f8ff;font-weight:700;font-size:22pt;")
        value_font = QtGui.QFont(self.value_label.font())
        value_font.setPointSizeF(22.0)
        self.value_label.setFont(value_font)
        self.unit_label = QtWidgets.QLabel("", self)
        self.unit_label.setStyleSheet("color:#aab8cc;font-weight:650;font-size:12pt;")
        unit_font = QtGui.QFont(self.unit_label.font())
        unit_font.setPointSizeF(12.0)
        self.unit_label.setFont(unit_font)
        self.unit_label.setVisible(False)
        value_row.addWidget(self.value_label, 0, QtCore.Qt.AlignBottom)
        value_row.addWidget(self.unit_label, 0, QtCore.Qt.AlignBottom)
        value_row.addStretch(1)

        self.subtle_label = QtWidgets.QLabel("", self)
        self.subtle_label.setStyleSheet("color:#7388a7;font-weight:500;font-size:10pt;")
        subtle_font = QtGui.QFont(self.subtle_label.font())
        subtle_font.setPointSizeF(10.0)
        self.subtle_label.setFont(subtle_font)
        self.subtle_label.setVisible(False)

        layout.addWidget(self.title_label)
        layout.addLayout(value_row)
        layout.addWidget(self.subtle_label)
        self.setMinimumHeight(124)

    def _apply_style(self):
        self.setStyleSheet(
            f"""
            QFrame#telemetry_tile {{
                background-color: #0f172a;
                border: 1px solid #24344e;
                border-top: 3px solid {self._accent};
                border-radius: 8px;
            }}
            """
        )

    def set_accent(self, accent: str):
        accent_txt = str(accent or "").strip()
        if not accent_txt:
            return
        if accent_txt == self._accent:
            return
        self._accent = accent_txt
        self._apply_style()

    def set_value(self, value: str, unit: str = "", subtle: str = ""):
        self.value_label.setText(str(value))
        self.unit_label.setText(str(unit))
        self.unit_label.setVisible(bool(str(unit).strip()))
        self.subtle_label.setText(str(subtle))
        self.subtle_label.setVisible(bool(str(subtle).strip()))
