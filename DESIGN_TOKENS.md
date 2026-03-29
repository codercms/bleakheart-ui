# UI Style Sources

Current UI styles are defined directly in code, not in a centralized token module.

## Primary sources

- `app.py`
  - `QSS_THEME` for app-level dark style (`QWidget`, `QFrame#panel`, buttons, checkboxes, inputs, list selection).
  - Header badge style via per-badge `setStyleSheet(...)`.
- `render.py`
  - Chart row appearance (`_PgChartRow`):
  - plot background, border, axis colors, tick fonts, and chart line colors/widths.

## Practical rule

When changing UI appearance:
- update `QSS_THEME` for global widget styles,
- update `render.py` for chart-specific visuals.

There is currently no separate design-token file consumed by runtime code.
