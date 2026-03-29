# Architecture

## Runtime modules

- `app.py`
  - Main Qt application/controller (`QtBleakHeartQtGraphUI`).
  - Owns UI widgets, settings persistence (`qt_ui_settings.json`), connection/recording actions, and app lifecycle.
- `engine.py`
  - BLE + `bleakheart` backend.
  - Scan/connect/disconnect, live stream subscriptions, CSV recording, and event emission.
- `render.py`
  - Chart widgets built with `pyqtgraph` (`QtGraphCharts` and per-row chart setup).
- `render_controller.py`
  - Rendering pipeline/state:
  - stream buffering, playback offsets, dirty/motion redraw decisions, and chart updates.
- `ecg_render_core.py`
  - ECG ring buffer primitive used by `render_controller.py`.

## Data flow

1. `engine.py` emits events into the app event queue (`log`, `battery`, `hr_sample`, `ecg_samples`, `acc_samples`, ...).
2. `app.py` receives events and updates UI state (badges/status/recording state), forwarding stream payloads to `RenderController`.
3. `RenderController` buffers and time-aligns streams.
4. Render tick requests dirty/motion redraw from `RenderController`, which updates `QtGraphCharts`.

## Rendering model

- Separate event and render timers.
- Dirty redraw for new data.
- Motion-only redraw for smooth scrolling between sparse updates.
- ECG uses ring-buffered chunk ingestion and transform-based motion between geometry rebuilds.
- FPS lock supports `Auto` (display refresh probe) and fixed caps.

## Persistence

`qt_ui_settings.json` stores:
- window/layout state
- last device
- measurement selections
- profile set + active profile
- render FPS mode/cap
- sidebar visibility state
