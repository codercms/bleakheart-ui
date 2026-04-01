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
    def __init__(self, title: str, *, accent: str = "#38bdf8", icon_kind: str | None = None, parent=None):
        super().__init__(parent)
        self.setObjectName("telemetry_tile")
        self._accent = str(accent)
        self._icon_kind = str(icon_kind or "").strip().lower()
        self._scale = 1.0
        self._font_boost = 1.25
        self._base_rem = max(1.0, QtGui.QFontMetricsF(self.font()).lineSpacing())
        self._apply_style()
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.title_label = QtWidgets.QLabel(title, self)
        title_font = QtGui.QFont(self.title_label.font())
        title_font.setPointSizeF(11.0 * self._font_boost)
        self.title_label.setFont(title_font)
        self._base_title_pt = 11.0 * self._font_boost

        value_row = QtWidgets.QHBoxLayout()
        value_row.setContentsMargins(0, 0, 0, 0)
        value_row.setSpacing(0)
        self.value_label = QtWidgets.QLabel("--", self)
        value_font = QtGui.QFont(self.value_label.font())
        value_font.setPointSizeF(22.0 * self._font_boost)
        self.value_label.setFont(value_font)
        self._base_value_pt = 22.0 * self._font_boost
        self.unit_label = QtWidgets.QLabel("", self)
        unit_font = QtGui.QFont(self.unit_label.font())
        unit_font.setPointSizeF(12.0 * self._font_boost)
        self.unit_label.setFont(unit_font)
        self._base_unit_pt = 12.0 * self._font_boost
        self.unit_label.setVisible(False)
        value_row.addWidget(self.value_label, 0, QtCore.Qt.AlignVCenter)
        value_row.addWidget(self.unit_label, 0, QtCore.Qt.AlignVCenter)
        value_row.addStretch(1)

        self.subtle_label = QtWidgets.QLabel("", self)
        subtle_font = QtGui.QFont(self.subtle_label.font())
        subtle_font.setPointSizeF(10.0 * self._font_boost)
        self.subtle_label.setFont(subtle_font)
        self._base_subtle_pt = 10.0 * self._font_boost
        self.subtle_label.setVisible(False)

        self.icon_label = QtWidgets.QLabel(self)
        self.icon_label.setStyleSheet("background:transparent;")
        self.icon_label.setFixedSize(1, 1)

        title_row = QtWidgets.QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(0)
        title_row.addWidget(self.title_label, 0, QtCore.Qt.AlignVCenter)
        title_row.addStretch(1)
        layout.addLayout(title_row)

        value_row.insertWidget(0, self.icon_label, 0, QtCore.Qt.AlignVCenter)
        self._icon_gap_item = QtWidgets.QSpacerItem(0, 0, QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Minimum)
        value_row.insertSpacerItem(1, self._icon_gap_item)

        layout.addStretch(1)
        layout.addLayout(value_row)
        layout.addWidget(self.subtle_label)
        layout.addStretch(1)
        self._layout = layout
        self._title_row = title_row
        self._value_row = value_row
        self._base_min_height = int(round(self._base_rem * 6.8))
        self.setMinimumHeight(self._base_min_height)

        self._apply_text_style()
        self._apply_layout_tokens()
        self._refresh_icon()

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
        self._refresh_icon()

    def set_value(self, value: str, unit: str = "", subtle: str = ""):
        self.value_label.setText(str(value))
        self.unit_label.setText(str(unit))
        self.unit_label.setVisible(bool(str(unit).strip()))
        self.subtle_label.setText(str(subtle))
        self.subtle_label.setVisible(bool(str(subtle).strip()))

    def _apply_text_style(self):
        s = float(self._scale)
        self.title_label.setStyleSheet(
            f"color:#9fb2cd;background:transparent;font-weight:650;letter-spacing:0.5px;font-size:{self._base_title_pt * s:.2f}pt;"
        )
        self.value_label.setStyleSheet(
            f"color:#f3f8ff;background:transparent;font-weight:700;font-size:{self._base_value_pt * s:.2f}pt;"
        )
        self.unit_label.setStyleSheet(
            f"color:#aab8cc;background:transparent;font-weight:650;font-size:{self._base_unit_pt * s:.2f}pt;"
        )
        self.subtle_label.setStyleSheet(
            f"color:#7388a7;background:transparent;font-weight:500;font-size:{self._base_subtle_pt * s:.2f}pt;"
        )

    def set_scale(self, scale: float):
        s = max(0.85, min(1.8, float(scale)))
        self._scale = s
        self._apply_text_style()
        self._apply_layout_tokens()
        self._refresh_icon()
        self._refresh_icon_gap()

    def _apply_layout_tokens(self):
        rem = float(self._base_rem) * float(self._scale)
        pad_x = max(1, int(round(rem * 0.72)))
        pad_y = max(1, int(round(rem * 0.46)))
        block_gap = max(1, int(round(rem * 0.18)))
        title_gap = max(1, int(round(rem * 0.18)))
        value_gap = max(1, int(round(rem * 0.26)))
        min_h = max(int(round(self._base_min_height)), int(round(float(self._base_min_height) * float(self._scale))))

        self.setMinimumHeight(min_h)
        self._layout.setContentsMargins(pad_x, pad_y, pad_x, pad_y)
        self._layout.setSpacing(block_gap)
        self._title_row.setSpacing(title_gap)
        self._value_row.setSpacing(value_gap)

    def _refresh_icon(self):
        if not hasattr(self, "icon_label") or self.icon_label is None:
            return
        if not self._icon_kind:
            self.icon_label.clear()
            self.icon_label.setVisible(False)
            return
        self.icon_label.setVisible(True)
        value_h = QtGui.QFontMetrics(self.value_label.font()).height()
        rem = float(self._base_rem) * float(self._scale)
        size = max(1, int(round(max(float(value_h) * 0.78, rem * 0.92))))
        self.icon_label.setFixedSize(size, size)
        pix = self._render_icon(self._icon_kind, QtGui.QColor(self._accent), size)
        self.icon_label.setPixmap(pix)
        self._refresh_icon_gap()

    def _refresh_icon_gap(self):
        if not hasattr(self, "_icon_gap_item"):
            return
        value_h = QtGui.QFontMetrics(self.value_label.font()).height()
        gap = max(1, int(round(float(value_h) * 0.12 * 0.75)))
        self._icon_gap_item.changeSize(gap, 0, QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Minimum)
        if hasattr(self, "_value_row") and self._value_row is not None:
            self._value_row.invalidate()

    def _render_icon(self, kind: str, color: QtGui.QColor, size: int) -> QtGui.QPixmap:
        dpr = 1.0
        try:
            dpr = max(1.0, float(self.devicePixelRatioF()))
        except Exception:
            dpr = 1.0
        px = max(1, int(round(float(size) * dpr)))
        pix = QtGui.QPixmap(px, px)
        pix.setDevicePixelRatio(dpr)
        pix.fill(QtCore.Qt.GlobalColor.transparent)
        p = QtGui.QPainter(pix)
        p.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        p.setRenderHint(QtGui.QPainter.RenderHint.SmoothPixmapTransform, True)
        pen = QtGui.QPen(color, max(1.0, size * 0.09))
        pen.setCosmetic(True)
        pen.setCapStyle(QtCore.Qt.PenCapStyle.RoundCap)
        pen.setJoinStyle(QtCore.Qt.PenJoinStyle.RoundJoin)
        p.setPen(pen)
        p.setBrush(QtCore.Qt.BrushStyle.NoBrush)

        s = float(size)
        if kind == "heart":
            path = QtGui.QPainterPath()
            path.moveTo(0.5 * s, 0.83 * s)
            path.cubicTo(0.15 * s, 0.58 * s, 0.18 * s, 0.28 * s, 0.38 * s, 0.28 * s)
            path.cubicTo(0.47 * s, 0.28 * s, 0.5 * s, 0.33 * s, 0.5 * s, 0.37 * s)
            path.cubicTo(0.5 * s, 0.33 * s, 0.53 * s, 0.28 * s, 0.62 * s, 0.28 * s)
            path.cubicTo(0.82 * s, 0.28 * s, 0.85 * s, 0.58 * s, 0.5 * s, 0.83 * s)
            p.drawPath(path)
        elif kind == "stopwatch":
            p.drawEllipse(QtCore.QRectF(0.2 * s, 0.25 * s, 0.6 * s, 0.6 * s))
            p.drawLine(QtCore.QPointF(0.5 * s, 0.18 * s), QtCore.QPointF(0.5 * s, 0.25 * s))
            p.drawLine(QtCore.QPointF(0.5 * s, 0.55 * s), QtCore.QPointF(0.65 * s, 0.45 * s))
        elif kind == "battery":
            p.drawRect(QtCore.QRectF(0.10 * s, 0.30 * s, 0.74 * s, 0.40 * s))
            p.drawRect(QtCore.QRectF(0.86 * s, 0.42 * s, 0.08 * s, 0.16 * s))
            p.fillRect(QtCore.QRectF(0.16 * s, 0.36 * s, 0.54 * s, 0.28 * s), color)
        elif kind == "rr":
            path = QtGui.QPainterPath()
            path.moveTo(0.12 * s, 0.58 * s)
            path.lineTo(0.32 * s, 0.58 * s)
            path.lineTo(0.42 * s, 0.38 * s)
            path.lineTo(0.53 * s, 0.72 * s)
            path.lineTo(0.63 * s, 0.47 * s)
            path.lineTo(0.88 * s, 0.47 * s)
            p.drawPath(path)
        elif kind == "flame":
            path = QtGui.QPainterPath()
            path.moveTo(0.5 * s, 0.14 * s)
            path.cubicTo(0.62 * s, 0.32 * s, 0.72 * s, 0.36 * s, 0.73 * s, 0.56 * s)
            path.cubicTo(0.73 * s, 0.74 * s, 0.62 * s, 0.86 * s, 0.5 * s, 0.86 * s)
            path.cubicTo(0.34 * s, 0.86 * s, 0.25 * s, 0.72 * s, 0.25 * s, 0.57 * s)
            path.cubicTo(0.25 * s, 0.44 * s, 0.31 * s, 0.35 * s, 0.44 * s, 0.27 * s)
            path.cubicTo(0.48 * s, 0.24 * s, 0.49 * s, 0.20 * s, 0.5 * s, 0.14 * s)
            p.drawPath(path)
        elif kind == "signal":
            p.drawEllipse(QtCore.QRectF(0.42 * s, 0.66 * s, 0.16 * s, 0.16 * s))
            p.drawArc(QtCore.QRectF(0.26 * s, 0.50 * s, 0.48 * s, 0.34 * s), 0, 180 * 16)
            p.drawArc(QtCore.QRectF(0.12 * s, 0.34 * s, 0.76 * s, 0.52 * s), 0, 180 * 16)
        else:
            p.drawEllipse(QtCore.QRectF(0.25 * s, 0.25 * s, 0.5 * s, 0.5 * s))
        p.end()
        return pix
