from datetime import datetime

from PySide6 import QtCore, QtGui, QtWidgets

from bleakheart_ui.features.sessions.models import SessionSummary
from bleakheart_ui.features.sessions.ui_utils import format_duration, scaled_font
class _HistoryTableModel(QtCore.QAbstractTableModel):
    ROLE_SESSION = QtCore.Qt.UserRole + 1
    ROLE_PREVIEW = QtCore.Qt.UserRole + 2
    ROLE_SORT = QtCore.Qt.UserRole + 3

    HEADERS = ["Date/Time", "Profile", "Activity", "Duration", "Kcal", "Avg HR", "Max HR", "BPM Preview"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows: list[SessionSummary] = []

    def set_rows(self, rows: list[SessionSummary]):
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def rowCount(self, parent=QtCore.QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent=QtCore.QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.HEADERS)

    def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        if orientation == QtCore.Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return None

    def data(self, index: QtCore.QModelIndex, role=QtCore.Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._rows):
            return None
        row = self._rows[index.row()]
        col = index.column()

        if role == self.ROLE_SESSION:
            return row
        if role == self.ROLE_PREVIEW and col == 7:
            return row.preview
        if role == self.ROLE_SORT:
            if col == 0:
                return row.start_ts
            if col == 1:
                return row.profile_id
            if col == 2:
                return row.activity_name
            if col == 3:
                return row.duration_s
            if col == 4:
                return row.kcal
            if col == 5:
                return row.avg_hr_bpm
            if col == 6:
                return row.max_hr_bpm
            return 0

        if role == QtCore.Qt.DisplayRole:
            if col == 0:
                return datetime.fromtimestamp(row.start_ts).strftime("%Y-%m-%d %H:%M")
            if col == 1:
                return row.profile_id
            if col == 2:
                return row.activity_name.title()
            if col == 3:
                return format_duration(row.duration_s)
            if col == 4:
                return f"{row.kcal:.0f}"
            if col == 5:
                return f"{row.avg_hr_bpm:.0f}"
            if col == 6:
                return f"{row.max_hr_bpm:.0f}"
            if col == 7:
                return ""

        if role == QtCore.Qt.TextAlignmentRole and col in (3, 4, 5, 6):
            return int(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

        return None


class _HistoryFilterProxy(QtCore.QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.profile_id = ""
        self.start_ts = None
        self.end_ts = None
        self.min_duration_s = None
        self.max_duration_s = None
        self.setSortRole(_HistoryTableModel.ROLE_SORT)

    def filterAcceptsRow(self, source_row: int, source_parent: QtCore.QModelIndex) -> bool:
        idx = self.sourceModel().index(source_row, 0, source_parent)
        session = self.sourceModel().data(idx, _HistoryTableModel.ROLE_SESSION)
        if session is None:
            return False
        if self.profile_id and session.profile_id != self.profile_id:
            return False
        if self.start_ts is not None and session.start_ts < self.start_ts:
            return False
        if self.end_ts is not None and session.start_ts > self.end_ts:
            return False
        if self.min_duration_s is not None and session.duration_s < self.min_duration_s:
            return False
        if self.max_duration_s is not None and session.duration_s > self.max_duration_s:
            return False
        return True


class _SparklineDelegate(QtWidgets.QStyledItemDelegate):
    def paint(self, painter: QtGui.QPainter, option: QtWidgets.QStyleOptionViewItem, index: QtCore.QModelIndex):
        values = index.data(_HistoryTableModel.ROLE_PREVIEW) or []
        rect = option.rect.adjusted(4, 5, -4, -5)
        painter.save()
        painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
        painter.fillRect(rect, QtGui.QColor("#0f172a"))
        if len(values) < 2:
            painter.setPen(QtGui.QColor("#64748b"))
            painter.drawText(rect, QtCore.Qt.AlignCenter, "--")
        else:
            vmin = min(values)
            vmax = max(values)
            span = max(1e-6, vmax - vmin)
            path = QtGui.QPainterPath()
            for i, val in enumerate(values):
                x = rect.left() + (i / max(1, len(values) - 1)) * rect.width()
                y = rect.bottom() - ((float(val) - vmin) / span) * rect.height()
                if i == 0:
                    path.moveTo(x, y)
                else:
                    path.lineTo(x, y)
            painter.setPen(QtGui.QPen(QtGui.QColor("#22d3ee"), 1.4))
            painter.drawPath(path)
        painter.restore()


class SessionHistoryWindow(QtWidgets.QDialog):
    refresh_requested = QtCore.Signal()
    session_open_requested = QtCore.Signal(str)
    session_delete_requested = QtCore.Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Session History")
        self.resize(1200, 760)

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)
        heading = QtWidgets.QLabel("Session History", self)
        heading.setFont(scaled_font(self.font(), 1.16, weight=QtGui.QFont.DemiBold))
        heading.setStyleSheet("color:#e2e8f0;")
        root.addWidget(heading)

        filters = QtWidgets.QFrame(self)
        filters.setObjectName("panel")
        f = QtWidgets.QGridLayout(filters)
        f.setContentsMargins(10, 10, 10, 10)
        f.setHorizontalSpacing(8)
        f.setVerticalSpacing(8)
        filter_label_font = scaled_font(self.font(), 0.95, weight=QtGui.QFont.Medium)

        self.profile_filter = QtWidgets.QComboBox(filters)
        self.profile_filter.addItem("All profiles", "")
        self.date_from = QtWidgets.QDateEdit(filters)
        self.date_to = QtWidgets.QDateEdit(filters)
        self.date_from.setCalendarPopup(True)
        self.date_to.setCalendarPopup(True)
        self.date_from.setDisplayFormat("yyyy-MM-dd")
        self.date_to.setDisplayFormat("yyyy-MM-dd")
        self.date_from.setDate(QtCore.QDate(2000, 1, 1))
        self.date_to.setDate(QtCore.QDate.currentDate())
        self.date_from.setMinimumDate(QtCore.QDate(2000, 1, 1))
        self.date_to.setMinimumDate(QtCore.QDate(2000, 1, 1))

        self.min_dur = QtWidgets.QSpinBox(filters)
        self.max_dur = QtWidgets.QSpinBox(filters)
        for spin in (self.min_dur, self.max_dur):
            spin.setRange(0, 24 * 60 * 60)
            spin.setSuffix(" s")
        self.max_dur.setValue(24 * 60 * 60)

        self.refresh_btn = QtWidgets.QPushButton("Refresh Index", filters)
        self.open_btn = QtWidgets.QPushButton("Open Session", filters)
        self.delete_btn = QtWidgets.QPushButton("Delete Session", filters)

        lbl_profile = QtWidgets.QLabel("Profile")
        lbl_profile.setFont(filter_label_font)
        f.addWidget(lbl_profile, 0, 0)
        f.addWidget(self.profile_filter, 0, 1)
        lbl_date_from = QtWidgets.QLabel("Date from")
        lbl_date_from.setFont(filter_label_font)
        f.addWidget(lbl_date_from, 0, 2)
        f.addWidget(self.date_from, 0, 3)
        lbl_date_to = QtWidgets.QLabel("Date to")
        lbl_date_to.setFont(filter_label_font)
        f.addWidget(lbl_date_to, 0, 4)
        f.addWidget(self.date_to, 0, 5)
        lbl_min = QtWidgets.QLabel("Min duration")
        lbl_min.setFont(filter_label_font)
        f.addWidget(lbl_min, 1, 0)
        f.addWidget(self.min_dur, 1, 1)
        lbl_max = QtWidgets.QLabel("Max duration")
        lbl_max.setFont(filter_label_font)
        f.addWidget(lbl_max, 1, 2)
        f.addWidget(self.max_dur, 1, 3)
        f.addWidget(self.refresh_btn, 1, 4)
        f.addWidget(self.open_btn, 1, 5)
        f.addWidget(self.delete_btn, 1, 6)

        root.addWidget(filters)

        self.table = QtWidgets.QTableView(self)
        self.model = _HistoryTableModel(self.table)
        self.proxy = _HistoryFilterProxy(self.table)
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(False)
        self.table.setSortingEnabled(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setFont(scaled_font(self.font(), 0.98))
        header_font = scaled_font(self.font(), 1.0, weight=QtGui.QFont.DemiBold)
        self.table.horizontalHeader().setFont(header_font)
        self.table.verticalHeader().setDefaultSectionSize(max(24, int(self.fontMetrics().lineSpacing() * 2.0)))
        self.table.setItemDelegateForColumn(7, _SparklineDelegate(self.table))
        self.table.setColumnWidth(0, 165)
        self.table.setColumnWidth(1, 130)
        self.table.setColumnWidth(2, 140)
        self.table.setColumnWidth(3, 95)
        self.table.setColumnWidth(4, 80)
        self.table.setColumnWidth(5, 85)
        self.table.setColumnWidth(6, 85)
        self.table.setColumnWidth(7, 270)
        self.table.sortByColumn(0, QtCore.Qt.DescendingOrder)
        root.addWidget(self.table, 1)

        self.status = QtWidgets.QLabel("", self)
        self.status.setStyleSheet("color:#94a3b8;")
        root.addWidget(self.status)

        self.profile_filter.currentIndexChanged.connect(self._apply_filters)
        self.date_from.dateChanged.connect(self._apply_filters)
        self.date_to.dateChanged.connect(self._apply_filters)
        self.min_dur.valueChanged.connect(self._apply_filters)
        self.max_dur.valueChanged.connect(self._apply_filters)
        self.refresh_btn.clicked.connect(self.refresh_requested.emit)
        self.open_btn.clicked.connect(self._open_selected)
        self.delete_btn.clicked.connect(self._delete_selected_with_confirm)
        self.table.doubleClicked.connect(lambda _idx: self._open_selected())

    def set_profiles(self, profile_ids: list[str]):
        current = self.profile_filter.currentData()
        self.profile_filter.blockSignals(True)
        self.profile_filter.clear()
        self.profile_filter.addItem("All profiles", "")
        for pid in profile_ids:
            self.profile_filter.addItem(pid, pid)
        if current:
            i = self.profile_filter.findData(current)
            if i >= 0:
                self.profile_filter.setCurrentIndex(i)
        self.profile_filter.blockSignals(False)

    def set_rows(self, rows: list[SessionSummary]):
        self.model.set_rows(rows)
        self._apply_filters()
        self.status.setText(f"{len(rows)} session(s)")

    def _apply_filters(self):
        self.proxy.profile_id = str(self.profile_filter.currentData() or "")
        start_q = self.date_from.date()
        end_q = self.date_to.date()
        start_dt = datetime(start_q.year(), start_q.month(), start_q.day(), 0, 0, 0)
        end_dt = datetime(end_q.year(), end_q.month(), end_q.day(), 23, 59, 59)
        self.proxy.start_ts = start_dt.timestamp()
        self.proxy.end_ts = end_dt.timestamp()
        self.proxy.min_duration_s = float(self.min_dur.value()) if self.min_dur.value() > 0 else None
        max_val = self.max_dur.value()
        self.proxy.max_duration_s = float(max_val) if max_val < (24 * 60 * 60) else None
        self.proxy.invalidateFilter()
        self.status.setText(f"{self.proxy.rowCount()} / {self.model.rowCount()} session(s)")

    def _open_selected(self):
        session = self._selected_session()
        if session is None:
            return
        self.session_open_requested.emit(session.session_id)

    def _selected_session(self) -> SessionSummary | None:
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return None
        src = self.proxy.mapToSource(sel[0])
        return self.model.data(self.model.index(src.row(), 0), _HistoryTableModel.ROLE_SESSION)

    def _delete_selected_with_confirm(self):
        session = self._selected_session()
        if session is None:
            return
        dt = datetime.fromtimestamp(session.start_ts).strftime("%Y-%m-%d %H:%M")
        message = (
            f"Delete session '{session.session_id}'?\n\n"
            f"Date: {dt}\n"
            f"Duration: {format_duration(session.duration_s)}\n\n"
            "This will remove the session files from disk."
        )
        box = QtWidgets.QMessageBox(self)
        box.setWindowTitle("Delete Session")
        box.setIcon(QtWidgets.QMessageBox.Warning)
        box.setText(message)
        box.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        box.setDefaultButton(QtWidgets.QMessageBox.No)
        box.setEscapeButton(QtWidgets.QMessageBox.No)
        no_btn = box.button(QtWidgets.QMessageBox.No)
        yes_btn = box.button(QtWidgets.QMessageBox.Yes)
        if no_btn is not None:
            no_btn.setAutoDefault(True)
            no_btn.setDefault(True)
            no_btn.setFocus()
        if yes_btn is not None:
            yes_btn.setAutoDefault(False)
            yes_btn.setDefault(False)
        ans = box.exec()
        if ans != QtWidgets.QMessageBox.Yes:
            return
        self.session_delete_requested.emit(session.session_id)

    def keyPressEvent(self, event: QtGui.QKeyEvent):
        key = event.key()
        if key in (QtCore.Qt.Key_Return, QtCore.Qt.Key_Enter):
            self._open_selected()
            event.accept()
            return
        if key in (QtCore.Qt.Key_Delete, QtCore.Qt.Key_Backspace):
            self._delete_selected_with_confirm()
            event.accept()
            return
        super().keyPressEvent(event)


