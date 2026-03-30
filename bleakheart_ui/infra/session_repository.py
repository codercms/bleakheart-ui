import csv
import json
import sqlite3
import time
from datetime import datetime
from collections import OrderedDict
from pathlib import Path
from typing import Any

import numpy as np
from bleakheart_ui.features.sessions.models import SessionSeries, SessionSummary


HR_FILE = "HeartRate_recording.csv"
RR_FILE = "RRinterval_recording.csv"
ECG_FILE = "RawECG_recording.csv"
ENERGY_FILE = "energy_summary.json"


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


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


def _downsample_minmax_np(
    x: np.ndarray,
    y: np.ndarray,
    max_points: int,
) -> tuple[np.ndarray, np.ndarray]:
    # Morphology-preserving downsample: keep local min and max in each bucket.
    # This avoids flattening sharp ECG QRS peaks when zoomed out.
    n = int(len(x))
    if max_points <= 0 or n <= max_points:
        return x, y
    if max_points < 4:
        stride = max(1, int(np.ceil(n / float(max_points))))
        return x[::stride], y[::stride]

    buckets = max(2, int(max_points // 2))
    edges = np.linspace(0, n, buckets + 1, dtype=np.int64)
    out_idx: list[int] = []
    for i in range(buckets):
        a = int(edges[i])
        b = int(edges[i + 1])
        if b <= a:
            continue
        seg = y[a:b]
        lo = int(np.argmin(seg)) + a
        hi = int(np.argmax(seg)) + a
        if lo <= hi:
            out_idx.append(lo)
            if hi != lo:
                out_idx.append(hi)
        else:
            out_idx.append(hi)
            if hi != lo:
                out_idx.append(lo)
    if not out_idx:
        return x[:max_points], y[:max_points]
    idx = np.asarray(out_idx, dtype=np.int64)
    idx = np.unique(idx)
    if len(idx) > max_points:
        step = max(1, int(np.ceil(len(idx) / float(max_points))))
        idx = idx[::step]
    return x[idx], y[idx]


def _downsample_stride_np(x: np.ndarray, y: np.ndarray, max_points: int) -> tuple[np.ndarray, np.ndarray]:
    n = int(len(x))
    if max_points <= 0 or n <= max_points:
        return x, y
    stride = max(1, int(np.ceil(n / float(max_points))))
    return x[::stride], y[::stride]


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

class SessionIndexRepository:
    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.sessions_dir = self.root_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.sessions_dir / "session_index.sqlite3"
        self._signal_cache_limit_bytes = 100 * 1024 * 1024
        self._signal_cache_bytes = 0
        self._signal_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
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
        hr_cache = self._get_or_load_signal_cache(
            session_id=session_id,
            kind="hr",
            session_path=session_path,
            filename=HR_FILE,
            x_key="timestamp_s",
            y_key="heart_rate_bpm",
        )
        if hr_cache is not None:
            zero_at = float(hr_cache["t0"])
            x_hr = np.asarray(hr_cache["x_off"], dtype=np.float64)
            y_hr = np.asarray(hr_cache["y"], dtype=np.float64)
            x_hr, y_hr = _downsample_stride_np(x_hr, y_hr, int(max_bpm_points))
            bpm_t = x_hr.tolist()
            bpm_v = y_hr.tolist()
        else:
            zero_at = float(session.start_ts)
            bpm_t = []
            bpm_v = []

        rr_cache = self._get_or_load_signal_cache(
            session_id=session_id,
            kind="rr",
            session_path=session_path,
            filename=RR_FILE,
            x_key="timestamp_s",
            y_key="rr_ms",
        )
        rr_t: list[float] = []
        rr_v: list[float] = []
        if rr_cache is not None:
            shift = float(rr_cache["t0"]) - float(zero_at)
            x_rr = np.asarray(rr_cache["x_off"], dtype=np.float64) + shift
            y_rr = np.asarray(rr_cache["y"], dtype=np.float64)
            x_rr, y_rr = _downsample_minmax_np(x_rr, y_rr, int(max_rr_points))
            rr_t = x_rr.tolist()
            rr_v = y_rr.tolist()

        ecg_cache = self._get_or_load_signal_cache(
            session_id=session_id,
            kind="ecg",
            session_path=session_path,
            filename=ECG_FILE,
            x_key="timestamp_s",
            y_key="ecg_uV",
        )
        ecg_t: list[float] = []
        ecg_v: list[float] = []
        if ecg_cache is not None:
            shift = float(ecg_cache["t0"]) - float(zero_at)
            x_ecg = np.asarray(ecg_cache["x_off"], dtype=np.float64) + shift
            y_ecg = np.asarray(ecg_cache["y"], dtype=np.float64)
            x_ecg, y_ecg = _downsample_minmax_np(x_ecg, y_ecg, int(max_ecg_points))
            ecg_t = x_ecg.tolist()
            ecg_v = y_ecg.tolist()

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
            zero_at_ts=float(zero_at),
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

    def load_signal_window(
        self,
        session_id: str,
        *,
        kind: str,
        filename: str,
        x_key: str,
        y_key: str,
        zero_at_ts: float,
        start_s: float,
        end_s: float,
        max_points: int,
        preserve_peaks: bool = False,
        include_neighbors: bool = False,
    ) -> tuple[list[float], list[float]]:
        session = self.get_session(session_id)
        if session is None:
            return [], []
        cache = self._get_or_load_signal_cache(
            session_id=session_id,
            kind=kind,
            session_path=Path(session.session_path),
            filename=filename,
            x_key=x_key,
            y_key=y_key,
        )
        if cache is None:
            return [], []
        left = max(0.0, float(start_s))
        right = max(left, float(end_s))
        shift = float(cache["t0"]) - float(zero_at_ts)
        x_rel = np.asarray(cache["x_off"], dtype=np.float64) + shift
        y = np.asarray(cache["y"], dtype=np.float64)
        i0 = int(np.searchsorted(x_rel, left, side="left"))
        i1 = int(np.searchsorted(x_rel, right, side="right"))
        if i1 <= i0:
            return [], []
        if include_neighbors:
            i0 = max(0, i0 - 1)
            i1 = min(len(x_rel), i1 + 1)
        view_x = x_rel[i0:i1]
        view_y = y[i0:i1]
        if len(view_x) > int(max_points):
            if preserve_peaks:
                view_x, view_y = _downsample_minmax_np(view_x, view_y, int(max_points))
            else:
                view_x, view_y = _downsample_stride_np(view_x, view_y, int(max_points))
        return view_x.tolist(), view_y.tolist()

    def load_ecg_window(
        self,
        session_id: str,
        *,
        zero_at_ts: float,
        start_s: float,
        end_s: float,
        max_points: int = 24000,
    ) -> tuple[list[float], list[float]]:
        return self.load_signal_window(
            session_id,
            kind="ecg",
            filename=ECG_FILE,
            x_key="timestamp_s",
            y_key="ecg_uV",
            zero_at_ts=zero_at_ts,
            start_s=start_s,
            end_s=end_s,
            max_points=max_points,
            preserve_peaks=True,
        )

    def _get_or_load_signal_cache(
        self,
        *,
        session_id: str,
        kind: str,
        session_path: Path,
        filename: str,
        x_key: str,
        y_key: str,
    ) -> dict[str, Any] | None:
        key = f"{session_id}:{kind}"
        cached = self._signal_cache.get(key)
        if cached is not None:
            self._signal_cache.move_to_end(key, last=True)
            return cached

        file_path = session_path / filename
        if not file_path.exists():
            return None

        t_vals: list[float] = []
        y_vals: list[float] = []
        try:
            with file_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    t = _safe_float(row.get(x_key))
                    y = _safe_float(row.get(y_key))
                    if t is None or y is None:
                        continue
                    t_vals.append(float(t))
                    y_vals.append(float(y))
        except Exception:
            return None
        if not t_vals:
            return None

        t0 = float(t_vals[0])
        x_off = (np.asarray(t_vals, dtype=np.float64) - t0).astype(np.float32, copy=False)
        y_arr = np.asarray(y_vals, dtype=np.float32)
        entry = {"t0": t0, "x_off": x_off, "y": y_arr, "bytes": int(x_off.nbytes + y_arr.nbytes)}
        self._signal_cache[key] = entry
        self._signal_cache_bytes += int(entry["bytes"])
        self._signal_cache.move_to_end(key, last=True)
        self._trim_signal_cache()
        return entry

    def _trim_signal_cache(self):
        while self._signal_cache and self._signal_cache_bytes > int(self._signal_cache_limit_bytes):
            _old_key, old_entry = self._signal_cache.popitem(last=False)
            self._signal_cache_bytes -= int(old_entry.get("bytes", 0))


