from PySide6 import QtCore, QtGui, QtWidgets


class SparklineWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._values: list[float] = []
        line_h = max(14, self.fontMetrics().lineSpacing())
        self.setMinimumHeight(line_h * 2)

    def set_values(self, values: list[float]):
        self._values = list(values or [])
        self.update()

    def paintEvent(self, event: QtGui.QPaintEvent):
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
        rect = self.rect().adjusted(3, 3, -3, -3)
        painter.fillRect(rect, QtGui.QColor("#0f172a"))
        if len(self._values) < 2:
            painter.setPen(QtGui.QColor("#64748b"))
            painter.drawText(rect, QtCore.Qt.AlignCenter, "--")
            return
        vmin = min(self._values)
        vmax = max(self._values)
        span = max(1e-6, vmax - vmin)
        path = QtGui.QPainterPath()
        for i, val in enumerate(self._values):
            x = rect.left() + (i / max(1, len(self._values) - 1)) * rect.width()
            y = rect.bottom() - ((val - vmin) / span) * rect.height()
            if i == 0:
                path.moveTo(x, y)
            else:
                path.lineTo(x, y)
        painter.setPen(QtGui.QPen(QtGui.QColor("#22d3ee"), 1.6))
        painter.drawPath(path)
