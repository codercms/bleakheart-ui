from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return bool(default)
    return bool(value)


def _as_int(value: Any, default: int, lo: int | None = None, hi: int | None = None) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if lo is not None:
        out = max(int(lo), out)
    if hi is not None:
        out = min(int(hi), out)
    return out


def _as_str(value: Any, default: str) -> str:
    if value is None:
        return str(default)
    return str(value)


@dataclass
class RuntimeSettings:
    last_device_address: str | None = None
    last_device_name: str | None = None
    auto_connect_on_startup: bool = True
    auto_reconnect_enabled: bool = True
    auto_reconnect_interval_ms: int = 5000
    recording_disconnect_mode: str = "pause_then_stop"
    recording_disconnect_grace_ms: int = 300000
    sdk_mode: bool = False
    hr_enabled: bool = True
    hr_instant: bool = False
    hr_unpack: bool = True
    activity_type: str = ""
    live_measurements: dict[str, bool] = field(default_factory=dict)
    initial_profile_prompt_shown: bool = False

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | None) -> RuntimeSettings:
        data = raw if isinstance(raw, Mapping) else {}
        mode = _as_str(data.get("recording_disconnect_mode"), "pause_then_stop").strip().lower()
        if mode not in ("pause_then_stop", "stop_immediately", "pause_indefinitely"):
            mode = "pause_then_stop"
        live_raw = data.get("live_measurements")
        live: dict[str, bool] = {}
        if isinstance(live_raw, Mapping):
            for k, v in live_raw.items():
                kk = str(k).strip()
                if kk:
                    live[kk] = bool(v)
        addr = data.get("last_device_address")
        name = data.get("last_device_name")
        return cls(
            last_device_address=(str(addr) if addr else None),
            last_device_name=(str(name) if name else None),
            auto_connect_on_startup=_as_bool(data.get("auto_connect_on_startup"), True),
            auto_reconnect_enabled=_as_bool(data.get("auto_reconnect_enabled"), True),
            auto_reconnect_interval_ms=_as_int(data.get("auto_reconnect_interval_ms"), 5000, 1000, 30000),
            recording_disconnect_mode=mode,
            recording_disconnect_grace_ms=_as_int(data.get("recording_disconnect_grace_ms"), 300000, 10000, 3600000),
            sdk_mode=_as_bool(data.get("sdk_mode"), False),
            hr_enabled=_as_bool(data.get("hr_enabled"), True),
            hr_instant=_as_bool(data.get("hr_instant"), False),
            hr_unpack=_as_bool(data.get("hr_unpack"), True),
            activity_type=_as_str(data.get("activity_type"), "").strip(),
            live_measurements=live,
            initial_profile_prompt_shown=_as_bool(data.get("initial_profile_prompt_shown"), False),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_device_address": self.last_device_address,
            "last_device_name": self.last_device_name,
            "auto_connect_on_startup": bool(self.auto_connect_on_startup),
            "auto_reconnect_enabled": bool(self.auto_reconnect_enabled),
            "auto_reconnect_interval_ms": int(self.auto_reconnect_interval_ms),
            "recording_disconnect_mode": str(self.recording_disconnect_mode),
            "recording_disconnect_grace_ms": int(self.recording_disconnect_grace_ms),
            "sdk_mode": bool(self.sdk_mode),
            "hr_enabled": bool(self.hr_enabled),
            "hr_instant": bool(self.hr_instant),
            "hr_unpack": bool(self.hr_unpack),
            "activity_type": str(self.activity_type),
            "live_measurements": dict(self.live_measurements),
            "initial_profile_prompt_shown": bool(self.initial_profile_prompt_shown),
        }


@dataclass
class UiSettings:
    window_geometry: list[int] | None = None
    window_state: str = "normal"
    main_splitter_sizes: list[int] | None = None
    sidebar_collapsed: bool = False
    auto_collapse_sidebar_on_record: bool = True
    startup_window_mode: str = "remember_last"
    render_fps_mode: str = "manual"
    render_fps_manual: int = 30
    show_fps_overlay: bool = False
    combine_hr_rr_chart: bool = False
    focus_mode_on_record: bool = True
    focus_chart_preference: str = "ECG"

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | None) -> UiSettings:
        data = raw if isinstance(raw, Mapping) else {}
        startup_mode = _as_str(data.get("startup_window_mode"), "remember_last").strip().lower()
        if startup_mode not in ("remember_last", "normal", "maximized", "fullscreen"):
            startup_mode = "remember_last"
        window_state = _as_str(data.get("window_state"), "normal").strip().lower()
        if window_state not in ("normal", "maximized", "fullscreen"):
            window_state = "normal"
        fps_mode = _as_str(data.get("render_fps_mode"), "manual").strip().lower()
        if fps_mode not in ("auto", "manual"):
            fps_mode = "manual"
        focus = _as_str(data.get("focus_chart_preference"), "ECG").strip().upper()
        if focus not in ("HR", "RR", "ECG"):
            focus = "ECG"
        geom_raw = data.get("window_geometry")
        geom: list[int] | None = None
        if isinstance(geom_raw, list) and len(geom_raw) == 4:
            try:
                geom = [int(geom_raw[0]), int(geom_raw[1]), int(geom_raw[2]), int(geom_raw[3])]
            except Exception:
                geom = None
        split_raw = data.get("main_splitter_sizes")
        split: list[int] | None = None
        if isinstance(split_raw, list) and len(split_raw) == 2:
            try:
                split = [int(split_raw[0]), int(split_raw[1])]
            except Exception:
                split = None
        return cls(
            window_geometry=geom,
            window_state=window_state,
            main_splitter_sizes=split,
            sidebar_collapsed=_as_bool(data.get("sidebar_collapsed"), False),
            auto_collapse_sidebar_on_record=_as_bool(data.get("auto_collapse_sidebar_on_record"), True),
            startup_window_mode=startup_mode,
            render_fps_mode=fps_mode,
            render_fps_manual=_as_int(data.get("render_fps_manual"), 30, 1, 240),
            show_fps_overlay=_as_bool(data.get("show_fps_overlay"), False),
            combine_hr_rr_chart=_as_bool(data.get("combine_hr_rr_chart"), False),
            focus_mode_on_record=_as_bool(data.get("focus_mode_on_record"), True),
            focus_chart_preference=focus,
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "window_state": str(self.window_state),
            "sidebar_collapsed": bool(self.sidebar_collapsed),
            "auto_collapse_sidebar_on_record": bool(self.auto_collapse_sidebar_on_record),
            "startup_window_mode": str(self.startup_window_mode),
            "render_fps_mode": str(self.render_fps_mode),
            "render_fps_manual": int(self.render_fps_manual),
            "show_fps_overlay": bool(self.show_fps_overlay),
            "combine_hr_rr_chart": bool(self.combine_hr_rr_chart),
            "focus_mode_on_record": bool(self.focus_mode_on_record),
            "focus_chart_preference": str(self.focus_chart_preference),
        }
        if self.window_geometry is not None:
            out["window_geometry"] = [int(v) for v in self.window_geometry]
        if self.main_splitter_sizes is not None:
            out["main_splitter_sizes"] = [int(v) for v in self.main_splitter_sizes]
        return out


@dataclass
class AppSettings:
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)
    ui: UiSettings = field(default_factory=UiSettings)

    @classmethod
    def from_storage(
        cls,
        runtime_raw: Mapping[str, Any] | None,
        ui_raw: Mapping[str, Any] | None,
    ) -> AppSettings:
        return cls(
            runtime=RuntimeSettings.from_dict(runtime_raw),
            ui=UiSettings.from_dict(ui_raw),
        )

    def to_dialog_payload(self) -> dict[str, Any]:
        out = {}
        out.update(self.runtime.to_dict())
        out.update(self.ui.to_dict())
        return out
