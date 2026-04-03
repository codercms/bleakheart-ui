# Architecture

## Entrypoints

- `app.py`
  - Thin launcher that imports and runs `bleakheart_ui.features.main.window.main`.
- `python -m bleakheart_ui`
  - Package entrypoint via `bleakheart_ui/__main__.py`.

## Runtime modules (current)

- `bleakheart_ui/features/main/window.py`
  - Main Qt application/controller (`QtBleakHeartQtGraphUI`).
  - Owns app lifecycle, settings load/save (`qt_ui_settings.json`), and top-level UI orchestration.
- `bleakheart_ui/features/main/constants.py`
  - Main-window constants and configuration values.
- `bleakheart_ui/features/main/widgets.py`
  - Shared main-window helper widgets (event pump, guarded controls, scroll containment).
- `bleakheart_ui/features/sessions/*`
  - Session UI feature:
  - recent cards, session manager window, session details window, shared session models/UI utils.
- `bleakheart_ui/core/engine.py`
  - BLE + `bleakheart` backend.
  - Scan/connect/disconnect, live stream subscriptions, recording lifecycle, and event emission.
- `bleakheart_ui/core/connection_manager.py`
  - Connection state/reconnect policy helpers.
- `bleakheart_ui/core/recording_manager.py`
  - Recording state transitions and guards.
- `bleakheart_ui/shared/render.py`
  - Chart widget composition built with `pyqtgraph`.
- `bleakheart_ui/shared/render_controller.py`
  - Rendering pipeline/state:
  - buffering, playback offsets, dirty/motion redraw decisions, and chart updates.
- `bleakheart_ui/shared/ecg_render_core.py`
  - ECG rendering primitives (ring buffer/resampling helpers).
- `bleakheart_ui/infra/session_repository.py`
  - Session indexing and signal loading from disk/cache (SQLite + CSV).
  - UI-agnostic persistence/data-access layer.

## Data flow

1. `bleakheart_ui.core.engine.BleakHeartEngine` emits events into the app event queue (`log`, `battery`, `hr_sample`, `ecg_samples`, `acc_samples`, ...).
2. `QtBleakHeartQtGraphUI` (main window feature) receives events and updates UI state, forwarding stream payloads to `RenderController`.
3. `RenderController` buffers/time-aligns streams and decides redraw mode.
4. `QtGraphCharts` receives plot updates for live rendering.
5. Session history/details use `SessionIndexRepository` for indexed metadata + on-demand signal windows.

## Rendering model

- Separate event and render timers.
- Dirty redraw for new data.
- Motion-only redraw for smooth scrolling between sparse updates.
- ECG uses ring-buffered chunk ingestion and transform-based motion between geometry rebuilds.
- FPS lock supports `Auto` (display refresh probe) and fixed caps.
- Session viewer uses cached signal arrays with adaptive downsampling/high-detail windows.

## Persistence

`qt_ui_settings.json` stores:
- window/layout state
- last device
- measurement selections
- profile set + active profile
- render FPS mode/cap
- sidebar visibility state

Session persistence:
- `app_data.sqlite3`: app database (session index/cache + app settings + profiles)
- per-session CSV + summary files in `sessions/<profile>_<activity>_<timestamp>/`
