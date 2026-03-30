
import csv
import bisect
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets


HR_FILE = "HeartRate_recording.csv"
RR_FILE = "RRinterval_recording.csv"
ECG_FILE = "RawECG_recording.csv"
ENERGY_FILE = "energy_summary.json"


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _scaled_font(
    base_font: QtGui.QFont,
    factor: float = 1.0,
    *,
    weight: QtGui.QFont.Weight | None = None,
) -> QtGui.QFont:
    out = QtGui.QFont(base_font)
    size = out.pointSizeF()
    if size <= 0:
        app_font = QtWidgets.QApplication.font()
        size = app_font.pointSizeF() if app_font.pointSizeF() > 0 else 10.0
    out.setPointSizeF(max(8.0, float(size) * float(factor)))
    if weight is not None:
        out.setWeight(weight)
    return out


def format_duration(total_s: float) -> str:
    total = max(0, int(round(float(total_s))))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _bucket_downsample(values: list[float], max_points: int) -> list[float]:
    if max_points <= 0 or len(values) <= max_points:
        return list(values)
    out: list[float] = []
    n = len(values)
    for i in range(max_points):
        idx = int(round(i * (n - 1) / max(1, max_points - 1)))
        out.append(values[idx])
    return out


def _decimate_xy(x: list[float], y: list[float], max_points: int) -> tuple[list[float], list[float]]:
    if max_points <= 0 or len(x) <= max_points:
        return list(x), list(y)
    out_x: list[float] = []
    out_y: list[float] = []
    n = len(x)
    for i in range(max_points):
        idx = int(round(i * (n - 1) / max(1, max_points - 1)))
        out_x.append(x[idx])
        out_y.append(y[idx])
    return out_x, out_y


def _parse_session_name(path: Path) -> tuple[str, str, float | None]:
    parts = path.name.split("_")
    if len(parts) < 3:
        return "unknown", "other", None
    ts_raw = f"{parts[-2]}_{parts[-1]}"
    start_ts = None
    try:
        start_ts = datetime.strptime(ts_raw, "%Y%m%d_%H%M%S").timestamp()
    except Exception:
        start_ts = None
    profile = "_".join(parts[:-2]) if len(parts) > 2 else "unknown"
    activity = "other"
    if "_" in profile:
        profile_parts = profile.split("_")
        if len(profile_parts) >= 2:
            activity = profile_parts[-1]
            profile = "_".join(profile_parts[:-1]) or profile
    return profile, activity, start_ts


@dataclass(slots=True)
class SessionSummary:
    session_id: str
    session_path: str
    start_ts: float
    end_ts: float
    duration_s: float
    profile_id: str
    profile_name: str
    activity_name: str
    kcal: float
    avg_hr_bpm: float
    max_hr_bpm: float
    min_hr_bpm: float
    has_rr: bool
    has_ecg: bool
    preview: list[float]
    indexed_at: float


@dataclass(slots=True)
class SessionSeries:
    session: SessionSummary
    bpm_t: list[float]
    bpm_v: list[float]
    rr_t: list[float]
    rr_v: list[float]
    ecg_t: list[float]
    ecg_v: list[float]
    hr_max_est: float
    zones_seconds: list[float]
    zones_percent: list[float]


class SessionIndexRepository:
    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.sessions_dir = self.root_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.sessions_dir / "session_index.sqlite3"
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_schema(self):
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    session_path TEXT UNIQUE NOT NULL,
                    start_ts REAL NOT NULL,
                    end_ts REAL NOT NULL,
                    duration_s REAL NOT NULL,
                    profile_id TEXT,
                    profile_name TEXT,
                    activity_name TEXT,
                    kcal REAL,
                    avg_hr_bpm REAL,
                    max_hr_bpm REAL,
                    min_hr_bpm REAL,
                    has_rr INTEGER NOT NULL,
                    has_ecg INTEGER NOT NULL,
                    preview_json TEXT,
                    preview_cache_key TEXT,
                    indexed_at REAL NOT NULL,
                    file_fingerprint TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_start_ts ON sessions(start_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_sessions_profile ON sessions(profile_id);
                CREATE INDEX IF NOT EXISTS idx_sessions_activity ON sessions(activity_name);
                CREATE INDEX IF NOT EXISTS idx_sessions_duration ON sessions(duration_s);
                """
            )

    def _is_session_dir(self, path: Path) -> bool:
        if not path.is_dir():
            return False
        if path.name.startswith("archive"):
            return False
        return (path / HR_FILE).exists() or (path / ENERGY_FILE).exists() or (path / RR_FILE).exists() or (path / ECG_FILE).exists()

    def _dir_fingerprint(self, session_dir: Path) -> str:
        mtimes: list[float] = []
        sizes: list[int] = []
        for name in (HR_FILE, RR_FILE, ECG_FILE, ENERGY_FILE, "kcal_timeline.csv"):
            p = session_dir / name
            if p.exists():
                try:
                    st = p.stat()
                    mtimes.append(float(st.st_mtime))
                    sizes.append(int(st.st_size))
                except Exception:
                    continue
        if not mtimes:
            st = session_dir.stat()
            return f"{int(st.st_mtime)}:{int(st.st_size)}"
        return f"{int(max(mtimes))}:{sum(sizes)}"

    def _extract_hr_summary(self, hr_path: Path, preview_points: int = 120) -> dict[str, Any]:
        count = 0
        sum_hr = 0.0
        max_hr = 0.0
        min_hr = 1e9
        start_ts = None
        end_ts = None
        preview: list[float] = []

        with hr_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                t = _safe_float(row.get("timestamp_s"))
                hr = _safe_float(row.get("heart_rate_bpm"))
                if t is None or hr is None:
                    continue
                if start_ts is None:
                    start_ts = t
                end_ts = t
                count += 1
                sum_hr += hr
                max_hr = max(max_hr, hr)
                min_hr = min(min_hr, hr)
                preview.append(hr)
                if len(preview) > preview_points * 8:
                    preview = preview[::2]

        if count == 0:
            return {
                "count": 0,
                "start_ts": None,
                "end_ts": None,
                "duration_s": 0.0,
                "avg_hr": 0.0,
                "max_hr": 0.0,
                "min_hr": 0.0,
                "preview": [],
            }

        return {
            "count": count,
            "start_ts": float(start_ts),
            "end_ts": float(end_ts),
            "duration_s": max(0.0, float(end_ts) - float(start_ts)),
            "avg_hr": float(sum_hr / count),
            "max_hr": float(max_hr),
            "min_hr": float(min_hr),
            "preview": _bucket_downsample(preview, preview_points),
        }

    def _extract_session_summary(self, session_dir: Path, fingerprint: str) -> SessionSummary:
        profile_id, activity_name, parsed_start = _parse_session_name(session_dir)
        profile_name = profile_id
        kcal = 0.0
        energy = session_dir / ENERGY_FILE
        if energy.exists():
            try:
                payload = json.loads(energy.read_text(encoding="utf-8"))
                kcal = float(payload.get("estimated_kcal_total") or 0.0)
                profile_id = str(payload.get("profile_id") or profile_id)
                activity_name = str(payload.get("activity_type") or activity_name).strip().lower() or activity_name
                profile_obj = payload.get("profile") or {}
                if isinstance(profile_obj, dict):
                    profile_name = str(profile_obj.get("name") or profile_name)
            except Exception:
                pass

        has_rr = (session_dir / RR_FILE).exists()
        has_ecg = (session_dir / ECG_FILE).exists()

        hr_summary = {
            "start_ts": parsed_start,
            "end_ts": parsed_start,
            "duration_s": 0.0,
            "avg_hr": 0.0,
            "max_hr": 0.0,
            "min_hr": 0.0,
            "preview": [],
        }
        hr_file = session_dir / HR_FILE
        if hr_file.exists():
            try:
                hr_summary = self._extract_hr_summary(hr_file)
            except Exception:
                pass

        start_ts = hr_summary.get("start_ts")
        if start_ts is None:
            start_ts = parsed_start if parsed_start is not None else float(session_dir.stat().st_mtime)
        end_ts = hr_summary.get("end_ts")
        if end_ts is None:
            end_ts = start_ts

        preview = hr_summary.get("preview") or []
        session_id = session_dir.name
        indexed_at = time.time()

        return SessionSummary(
            session_id=session_id,
            session_path=str(session_dir),
            start_ts=float(start_ts),
            end_ts=float(end_ts),
            duration_s=float(hr_summary.get("duration_s") or 0.0),
            profile_id=str(profile_id or "unknown"),
            profile_name=str(profile_name or profile_id or "unknown"),
            activity_name=str(activity_name or "other"),
            kcal=float(kcal),
            avg_hr_bpm=float(hr_summary.get("avg_hr") or 0.0),
            max_hr_bpm=float(hr_summary.get("max_hr") or 0.0),
            min_hr_bpm=float(hr_summary.get("min_hr") or 0.0),
            has_rr=bool(has_rr),
            has_ecg=bool(has_ecg),
            preview=list(preview),
            indexed_at=float(indexed_at),
        )
    def refresh_index(self) -> dict[str, int]:
        scanned = 0
        updated = 0
        removed = 0

        session_dirs = [p for p in self.sessions_dir.iterdir() if self._is_session_dir(p)]
        seen_paths = set()

        with self._connect() as conn:
            existing = {
                row["session_path"]: row
                for row in conn.execute("SELECT session_path, file_fingerprint FROM sessions")
            }

            for session_dir in session_dirs:
                scanned += 1
                session_path = str(session_dir)
                seen_paths.add(session_path)
                fingerprint = self._dir_fingerprint(session_dir)
                current = existing.get(session_path)
                if current is not None and str(current["file_fingerprint"]) == fingerprint:
                    continue
                try:
                    summary = self._extract_session_summary(session_dir, fingerprint)
                except Exception:
                    continue
                conn.execute(
                    """
                    INSERT INTO sessions (
                        session_id, session_path, start_ts, end_ts, duration_s, profile_id, profile_name,
                        activity_name, kcal, avg_hr_bpm, max_hr_bpm, min_hr_bpm, has_rr, has_ecg,
                        preview_json, preview_cache_key, indexed_at, file_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        session_path=excluded.session_path,
                        start_ts=excluded.start_ts,
                        end_ts=excluded.end_ts,
                        duration_s=excluded.duration_s,
                        profile_id=excluded.profile_id,
                        profile_name=excluded.profile_name,
                        activity_name=excluded.activity_name,
                        kcal=excluded.kcal,
                        avg_hr_bpm=excluded.avg_hr_bpm,
                        max_hr_bpm=excluded.max_hr_bpm,
                        min_hr_bpm=excluded.min_hr_bpm,
                        has_rr=excluded.has_rr,
                        has_ecg=excluded.has_ecg,
                        preview_json=excluded.preview_json,
                        preview_cache_key=excluded.preview_cache_key,
                        indexed_at=excluded.indexed_at,
                        file_fingerprint=excluded.file_fingerprint
                    """,
                    (
                        summary.session_id,
                        summary.session_path,
                        summary.start_ts,
                        summary.end_ts,
                        summary.duration_s,
                        summary.profile_id,
                        summary.profile_name,
                        summary.activity_name,
                        summary.kcal,
                        summary.avg_hr_bpm,
                        summary.max_hr_bpm,
                        summary.min_hr_bpm,
                        1 if summary.has_rr else 0,
                        1 if summary.has_ecg else 0,
                        json.dumps(summary.preview),
                        f"{summary.session_id}:{fingerprint}",
                        summary.indexed_at,
                        fingerprint,
                    ),
                )
                updated += 1

            stale = [path for path in existing.keys() if path not in seen_paths]
            for path in stale:
                conn.execute("DELETE FROM sessions WHERE session_path = ?", (path,))
                removed += 1
            conn.commit()

        return {"scanned": scanned, "updated": updated, "removed": removed}

    def _row_to_summary(self, row: sqlite3.Row) -> SessionSummary:
        preview_raw = row["preview_json"] or "[]"
        try:
            preview = [float(v) for v in json.loads(preview_raw)]
        except Exception:
            preview = []
        return SessionSummary(
            session_id=str(row["session_id"]),
            session_path=str(row["session_path"]),
            start_ts=float(row["start_ts"]),
            end_ts=float(row["end_ts"]),
            duration_s=float(row["duration_s"]),
            profile_id=str(row["profile_id"] or "unknown"),
            profile_name=str(row["profile_name"] or row["profile_id"] or "unknown"),
            activity_name=str(row["activity_name"] or "other"),
            kcal=float(row["kcal"] or 0.0),
            avg_hr_bpm=float(row["avg_hr_bpm"] or 0.0),
            max_hr_bpm=float(row["max_hr_bpm"] or 0.0),
            min_hr_bpm=float(row["min_hr_bpm"] or 0.0),
            has_rr=bool(int(row["has_rr"])),
            has_ecg=bool(int(row["has_ecg"])),
            preview=preview,
            indexed_at=float(row["indexed_at"]),
        )

    def list_profiles(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT profile_id FROM sessions ORDER BY profile_id ASC").fetchall()
        return [str(r[0]) for r in rows if r[0]]

    def get_recent_sessions(self, limit: int = 3) -> list[SessionSummary]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sessions ORDER BY start_ts DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [self._row_to_summary(row) for row in rows]

    def list_sessions(
        self,
        *,
        profile_id: str = "",
        start_ts: float | None = None,
        end_ts: float | None = None,
        min_duration_s: float | None = None,
        max_duration_s: float | None = None,
        sort_by: str = "start_ts",
        descending: bool = True,
    ) -> list[SessionSummary]:
        clauses: list[str] = []
        params: list[Any] = []
        if profile_id:
            clauses.append("profile_id = ?")
            params.append(profile_id)
        if start_ts is not None:
            clauses.append("start_ts >= ?")
            params.append(float(start_ts))
        if end_ts is not None:
            clauses.append("start_ts <= ?")
            params.append(float(end_ts))
        if min_duration_s is not None:
            clauses.append("duration_s >= ?")
            params.append(float(min_duration_s))
        if max_duration_s is not None:
            clauses.append("duration_s <= ?")
            params.append(float(max_duration_s))

        sort_map = {
            "start_ts": "start_ts",
            "duration_s": "duration_s",
            "avg_hr_bpm": "avg_hr_bpm",
            "max_hr_bpm": "max_hr_bpm",
            "kcal": "kcal",
            "activity_name": "activity_name",
            "profile_id": "profile_id",
        }
        order_col = sort_map.get(sort_by, "start_ts")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        direction = "DESC" if descending else "ASC"
        sql = f"SELECT * FROM sessions {where} ORDER BY {order_col} {direction}, start_ts DESC"

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_summary(r) for r in rows]

    def get_session(self, session_id: str) -> SessionSummary | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_summary(row)

    def _read_series(
        self,
        path: Path,
        x_key: str,
        y_key: str,
        *,
        max_points: int,
        zero_at: float | None,
    ) -> tuple[list[float], list[float], float | None]:
        xs: list[float] = []
        ys: list[float] = []
        first_ts = None
        if not path.exists():
            return xs, ys, None
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                t = _safe_float(row.get(x_key))
                y = _safe_float(row.get(y_key))
                if t is None or y is None:
                    continue
                if first_ts is None:
                    first_ts = t
                xs.append(float(t))
                ys.append(float(y))
                if len(xs) > max_points * 8:
                    xs = xs[::2]
                    ys = ys[::2]
        if not xs:
            return [], [], first_ts
        xs, ys = _decimate_xy(xs, ys, max_points)
        origin = zero_at if zero_at is not None else xs[0]
        xs = [float(v - origin) for v in xs]
        return xs, ys, first_ts

    def load_session_series(
        self,
        session_id: str,
        *,
        max_bpm_points: int = 12000,
        max_rr_points: int = 12000,
        max_ecg_points: int = 70000,
    ) -> SessionSeries | None:
        session = self.get_session(session_id)
        if session is None:
            return None
        session_path = Path(session.session_path)

        bpm_t, bpm_v, first_hr_ts = self._read_series(
            session_path / HR_FILE,
            "timestamp_s",
            "heart_rate_bpm",
            max_points=max_bpm_points,
            zero_at=None,
        )
        zero_at = first_hr_ts if first_hr_ts is not None else session.start_ts
        if first_hr_ts is not None:
            bpm_t = [float(v + (first_hr_ts - zero_at)) for v in bpm_t]

        rr_t, rr_v, _ = self._read_series(
            session_path / RR_FILE,
            "timestamp_s",
            "rr_ms",
            max_points=max_rr_points,
            zero_at=zero_at,
        )
        ecg_t, ecg_v, _ = self._read_series(
            session_path / ECG_FILE,
            "timestamp_s",
            "ecg_uV",
            max_points=max_ecg_points,
            zero_at=zero_at,
        )

        profile = {}
        energy = session_path / ENERGY_FILE
        if energy.exists():
            try:
                payload = json.loads(energy.read_text(encoding="utf-8"))
                p = payload.get("profile") or {}
                if isinstance(p, dict):
                    profile = p
            except Exception:
                pass
        age = float(profile.get("age_years") or 30.0)
        hr_max_est = float(profile.get("hr_max") or max(120.0, 220.0 - age))

        zones_seconds = [0.0] * 5
        if len(bpm_t) >= 2 and len(bpm_t) == len(bpm_v):
            bounds = [0.5 * hr_max_est, 0.6 * hr_max_est, 0.7 * hr_max_est, 0.8 * hr_max_est, 0.9 * hr_max_est]
            for i in range(len(bpm_v) - 1):
                hr = bpm_v[i]
                dt = max(0.0, bpm_t[i + 1] - bpm_t[i])
                if hr < bounds[1]:
                    idx = 0
                elif hr < bounds[2]:
                    idx = 1
                elif hr < bounds[3]:
                    idx = 2
                elif hr < bounds[4]:
                    idx = 3
                else:
                    idx = 4
                zones_seconds[idx] += dt
        zone_total = max(1e-9, sum(zones_seconds))
        zones_percent = [float(v * 100.0 / zone_total) for v in zones_seconds]

        return SessionSeries(
            session=session,
            bpm_t=bpm_t,
            bpm_v=bpm_v,
            rr_t=rr_t,
            rr_v=rr_v,
            ecg_t=ecg_t,
            ecg_v=ecg_v,
            hr_max_est=hr_max_est,
            zones_seconds=zones_seconds,
            zones_percent=zones_percent,
        )


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
        title_font = _scaled_font(self.font(), 1.05, weight=QtGui.QFont.DemiBold)
        meta_font = _scaled_font(self.font(), 0.94, weight=QtGui.QFont.Normal)

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
        heading.setFont(_scaled_font(self.font(), 1.16, weight=QtGui.QFont.DemiBold))
        heading.setStyleSheet("color:#e2e8f0;")
        root.addWidget(heading)

        filters = QtWidgets.QFrame(self)
        filters.setObjectName("panel")
        f = QtWidgets.QGridLayout(filters)
        f.setContentsMargins(10, 10, 10, 10)
        f.setHorizontalSpacing(8)
        f.setVerticalSpacing(8)
        filter_label_font = _scaled_font(self.font(), 0.95, weight=QtGui.QFont.Medium)

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
        self.table.setFont(_scaled_font(self.font(), 0.98))
        header_font = _scaled_font(self.font(), 1.0, weight=QtGui.QFont.DemiBold)
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
        self.title_label.setFont(_scaled_font(self.font(), 0.90, weight=QtGui.QFont.Medium))
        self.title_label.setStyleSheet("color:#7f91ab;")
        self.value_label = QtWidgets.QLabel("--", self)
        self.value_label.setFont(_scaled_font(self.font(), 1.50, weight=QtGui.QFont.Bold))
        self.value_label.setStyleSheet("color:#e2e8f0;")
        self.unit_label = QtWidgets.QLabel("", self)
        self.unit_label.setFont(_scaled_font(self.font(), 0.86, weight=QtGui.QFont.Medium))
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
        self._zone_labels = [
            "Z1 (50-60% HRmax)",
            "Z2 (60-70% HRmax)",
            "Z3 (70-80% HRmax)",
            "Z4 (80-90% HRmax)",
            "Z5 (90-100% HRmax)",
        ]

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
        zone_title.setFont(_scaled_font(self.font(), 1.02, weight=QtGui.QFont.DemiBold))
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
            legend_lbl.setFont(_scaled_font(self.font(), 0.90))
            legend_lbl.setToolTip(f"{legend}: training intensity zone.")
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
        self.zone_text.setFont(_scaled_font(self.font(), 0.92))
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
        chart_layout.addWidget(self.tabs, 1)
        root.addWidget(chart_wrap, 1)

        self._create_plot_tab("bpm", "BPM", "#22d3ee")
        self._create_plot_tab("rr", "RR", "#f59e0b")
        self._create_plot_tab("ecg", "ECG", "#f43f5e")
        self._load()

    def _create_plot_tab(self, key: str, title: str, color: str):
        tab = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        detail = pg.PlotWidget(tab)
        detail.setBackground("#0b1324")
        detail.showGrid(x=True, y=True, alpha=0.14)
        detail.setMouseEnabled(x=True, y=False)
        curve_color = QtGui.QColor(color)
        curve = detail.plot(pen=pg.mkPen(curve_color, width=1.25))
        layout.addWidget(detail, 1)

        overview = pg.PlotWidget(tab)
        overview.setBackground("#0b1324")
        overview.setFixedHeight(max(90, int(self.fontMetrics().lineSpacing() * 5.5)))
        overview.showGrid(x=False, y=False, alpha=0.0)
        overview.setMouseEnabled(x=False, y=False)
        overview.setMenuEnabled(False)
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
            "x": [],
            "y": [],
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
            reg.setRegion((min_x, max_x))
            plot.setXRange(min_x, max_x, padding=0.0)
            self._autoscale_visible_y(key)
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
            reg.setRegion((left, right))
            self._autoscale_visible_y(key)
        finally:
            self._nav_sync = False

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

        self.zone_bar.set_percent(series.zones_percent)
        zone_chunks = [
            f"Z{i + 1}: {format_duration(series.zones_seconds[i])} ({series.zones_percent[i]:.1f}%)"
            for i in range(5)
        ]
        self.zone_text.setText("  |  ".join(zone_chunks) + f"  |  HRmax est: {series.hr_max_est:.0f}")
        self.zone_bar.setToolTip("\n".join([f"{self._zone_labels[i]}: {series.zones_percent[i]:.1f}%" for i in range(5)]))

        self._set_plot_data("bpm", series.bpm_t, series.bpm_v)
        self._set_plot_data("rr", series.rr_t, series.rr_v)
        self._set_plot_data("ecg", series.ecg_t, series.ecg_v)

    def _set_plot_data(self, key: str, x: list[float], y: list[float]):
        refs = self._plots[key]
        refs["curve"].setData(x, y)
        refs["overview_curve"].setData(x, y)
        if not x:
            return
        left = float(x[0])
        right = float(x[-1])
        ymin = min(float(v) for v in y)
        ymax = max(float(v) for v in y)
        yspan = max(1e-3, ymax - ymin)
        ypad = yspan * 0.12
        ylow = ymin - ypad
        yhigh = ymax + ypad
        refs["x"] = list(x)
        refs["y"] = list(y)
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
        x = refs.get("x") or []
        y = refs.get("y") or []
        if not x or not y:
            return
        plot = refs["plot"]
        x_range = plot.viewRange()[0]
        left = float(x_range[0])
        right = float(x_range[1])
        if right <= left:
            return

        start = bisect.bisect_left(x, left)
        end = bisect.bisect_right(x, right)
        if end <= start:
            idx = min(max(0, start), len(y) - 1)
            y_slice = [float(y[idx])]
        else:
            y_slice = [float(v) for v in y[start:end]]
        if not y_slice:
            return
        ymin = min(y_slice)
        ymax = max(y_slice)
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
