from datetime import datetime

from PySide6 import QtCore, QtGui, QtWidgets

from bleakheart_ui.features.sessions.models import SessionSummary
from bleakheart_ui.features.sessions.sparkline_widget import SparklineWidget
from bleakheart_ui.features.sessions.ui_utils import format_duration, scaled_font
class RecentSessionCard(QtWidgets.QPushButton):
    clicked_session = QtCore.Signal(str)

    def __init__(self, session: SessionSummary, parent=None):
        super().__init__(parent)
        self.session = session
        self.setCursor(QtCore.Qt.PointingHandCursor)
        self.setStyleSheet(
            "QPushButton { text-align: left; border: 1px solid #334155; border-radius: 8px; background-color: #0f172a; padding: 8px; }"
            "QPushButton:hover { border-color: #38bdf8; background-color: #12223a; }"
        )
        line_h = max(14, self.fontMetrics().lineSpacing())
        self.setMinimumHeight(line_h * 6)
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(max(4, line_h // 3))
        title_font = scaled_font(self.font(), 1.05, weight=QtGui.QFont.DemiBold)
        meta_font = scaled_font(self.font(), 0.94, weight=QtGui.QFont.Normal)

        dt = datetime.fromtimestamp(session.start_ts).strftime("%Y-%m-%d %H:%M")
        top = QtWidgets.QLabel(f"{dt}  |  {format_duration(session.duration_s)}")
        top.setFont(title_font)
        top.setStyleSheet("color: #e2e8f0;")
        layout.addWidget(top)

        mid = QtWidgets.QLabel(
            f"{session.activity_name.title()}  ·  {session.kcal:.0f} kcal  ·  avg {session.avg_hr_bpm:.0f} / max {session.max_hr_bpm:.0f}"
        )
        mid.setFont(meta_font)
        mid.setStyleSheet("color: #94a3b8;")
        mid.setWordWrap(True)
        layout.addWidget(mid)

        self.spark = SparklineWidget(self)
        self.spark.set_values(session.preview)
        layout.addWidget(self.spark)

        self.clicked.connect(lambda: self.clicked_session.emit(session.session_id))

