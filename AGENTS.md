# AGENTS.md

## Project Intent
This app is a user-facing desktop BLE recorder UI built on top of `bleakheart`.
Engineering decisions should prioritize clarity, reliability, and smooth end-user workflows over cleverness.

## Product Priorities
1. Clear UX for non-technical users.
2. Stable BLE recording behavior.
3. Consistent, modern dark UI.
4. Measurable performance improvements without breaking UX.

## Runtime / Tooling
- Use Python from `.venv` for all local commands and validation:
  - Windows: `.\\.venv\\Scripts\\python.exe`
  - Linux/macOS: `.venv/bin/python`
- Primary app entrypoint is `app.py`.
- Package entrypoint is `python -m bleakheart_ui`.

## Canonical Repo Structure

```text
app.py
bleakheart_ui/
  __main__.py
  core/
    engine.py
    connection_manager.py
    recording_manager.py
  shared/
    render.py
    render_controller.py
    ecg_render_core.py
  infra/
    session_repository.py
  features/
    main/
      window.py
      constants.py
      widgets.py
    sessions/
      models.py
      ui_utils.py
      sparkline_widget.py
      recent_sessions_widget.py
      session_manager_window.py
      session_details_window.py
tests/
plans/
```

## Layer Boundaries (mandatory)
- `features/*`: Qt UI composition, view-level interaction logic.
- `core/*`: BLE engine, recording lifecycle, connection behavior.
- `infra/*`: persistence and session data loading/indexing.
- `shared/*`: cross-feature rendering/runtime utilities.

Rules:
- UI modules must not directly perform BLE operations; use core APIs.
- Core modules must not import feature UI code.
- Infra modules must stay UI-agnostic (no `PySide6` / widget logic).
- Keep dependencies one-way: `features -> core/infra/shared`, not inverse.

## UX Rules
- Favor obvious workflows: scan -> connect -> choose profile/activity -> record.
- Minimize ambiguous controls and labels.
- Use plain language in status and error messages.
- Avoid exposing internal/technical terms unless necessary.
- If a setting is advanced, add concise tooltip/help text.
- Require confirmation for destructive actions (delete session, reset-like operations).

## UI Consistency Rules (mandatory)
- Keep a consistent dark theme and spacing rhythm across all windows.
- Prefer scalable sizing (font-relative / DPI-aware) over hardcoded fixed pixels where practical.
- Keep control groups visually homogeneous:
  - same vertical rhythm
  - aligned baselines
  - consistent control heights inside a row
- Prevent accidental wheel-driven value changes on focused selectors unless intentional.
- Avoid scroll-chain glitches between nested scroll areas.

## Chart & Session Viewer Rules
- Keep selected time-range navigation synchronized across BPM/RR/ECG when intended.
- Do not allow panning/region dragging outside data bounds.
- Keep active tab state visually obvious (high-contrast selected tab style).
- Preserve ECG detail/performance behavior:
  - default to performant rendering for wide windows
  - show higher-fidelity morphology for narrow zoom windows
- Any downsampling change must preserve clinically meaningful shape characteristics.

## Recording / State Rules
- Single source of truth for selected measurements and live-preview state.
- During active recording, lock controls that would create inconsistent state.
- `Pause` must keep live streams visible but stop file writes.
- `Stop` must release resources cleanly and finalize outputs.

## Performance Rules
- Avoid full redraws when only partial updates are needed.
- Cap render FPS and process events efficiently.
- Prefer cached/in-memory signal access over repeated CSV IO in interactive chart paths.
- Measure before and after any performance change.
- Keep rendering changes incremental and reversible.

## Code Style & Quality
- Prefer explicit, maintainable code over terse abstractions.
- Keep functions focused and module responsibilities clear.
- Add short comments only where intent is not obvious.
- Avoid speculative abstractions; extract only when there is real reuse.
- Do not leave dead wrappers or compatibility shims once migration is complete.

## Testing & Validation
- Minimum before finalizing changes:
  - Windows:
    - `.\\.venv\\Scripts\\python.exe -m compileall -q app.py bleakheart_ui tests`
    - `.\\.venv\\Scripts\\python.exe -m pytest -q tests\\test_ecg_render_core.py`
  - Linux/macOS:
    - `.venv/bin/python -m compileall -q app.py bleakheart_ui tests`
    - `.venv/bin/python -m pytest -q tests/test_ecg_render_core.py`
- If adding behavior in another area, add/update focused tests near that area.
- Call out any unrun or failing test scopes explicitly.

## Refactor Safety Rules
- No behavior-change refactors must preserve UI behavior and data flow exactly.
- Prefer incremental moves with import rewiring over large rewrites.
- If a module is split, update imports to canonical targets immediately.
- Remove obsolete files after migration to avoid dual-source ambiguity.

## Repository Hygiene
- Do not commit machine-local runtime state (`qt_ui_settings.json`, temp logs, caches, screenshots).
- Commit deterministic source, docs, and reproducible configuration only.
- Keep `plans/` docs aligned with actual architecture after major refactors.
