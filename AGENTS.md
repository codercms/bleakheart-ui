# AGENTS.md

## Project Intent
This app is a user-facing desktop recorder UI built on top of `bleakheart`.
Engineering decisions should prioritize clarity, reliability, and smooth end-user workflows over cleverness.

## Product Priorities
1. Clear UX for non-technical users.
2. Stable BLE recording behavior.
3. Consistent, modern dark UI.
4. Measurable performance improvements without breaking UX.

## UX Rules
- Favor obvious workflows: scan -> connect -> choose profile/activity -> record.
- Minimize ambiguous controls and labels.
- Use plain language in status and error messages.
- Avoid exposing internal/technical terms unless necessary.
- If a setting is advanced, add concise tooltip/help text.

## UI Consistency Rules (mandatory)
- Use shared design tokens for spacing, padding, sizing, and typography.
- No magic numbers in widget layout when a token exists.
- Keep control groups visually homogeneous:
  - same vertical rhythm
  - aligned baselines
  - consistent button/chip heights within a row
- Keep dark theme consistent across ttk + tk widgets.
- Any new control must match existing gap/padding scale.

## Layout Rules
- Prefer predictable, responsive structure over dense UI.
- Support common DPI/zoom settings cleanly.
- Avoid clipping and hidden controls at typical laptop resolutions.
- Sidebar/logs visibility and split positions should persist.

## Recording/State Rules
- Single source of truth for selected measurements and live-preview state.
- During active recording, lock controls that would create inconsistent state.
- `Pause` must keep live streams visible but stop file writes.
- `Stop` must release resources cleanly and finalize outputs.

## Code Quality Rules
- Keep modules separated by responsibility:
  - `app.py`: orchestration/controller
  - `engine.py`: BLE + recording backend
  - `ui_layout.py`: widget composition/layout
  - `ui.py`: theme/helpers
  - `settings.py`: persistence only
- Remove dead code and compatibility hacks once migration is complete.
- Prefer explicit, maintainable code over terse abstractions.

## Performance Rules
- Avoid full redraws when only partial updates are needed.
- Cap render FPS and process events efficiently.
- Measure before and after any performance change.
- If changing rendering stack, keep migration risk low and incremental.

## Repository Hygiene
- Do not commit machine-local runtime state (`ui_settings.json`, temp logs, caches).
- Commit deterministic source, docs, and reproducible configuration only.
