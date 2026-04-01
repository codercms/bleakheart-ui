# BleakHeart Recorder UI

This app is a desktop UI for recording physiological data using the [`bleakheart`](https://github.com/fsmeraldi/bleakheart) library.

Desktop recorder UI for Polar devices built on:
- `bleakheart` (data decoding / PMD handling)
- `bleak` (BLE transport)
- `PySide6` + `pyqtgraph` (UI + live charts)

Entrypoints:
- `app.py`
- `python -m bleakheart_ui`

## Features

- Scan/connect/disconnect BLE devices.
- Auto-connect to last remembered device.
- Live streams:
  - HR / RR
  - PMD measurements (device-dependent): `ECG`, `ACC`, `PPG`, `PPI`, `GYRO`, `MAG`
  - Battery
- Record sessions to CSV files with `Start / Pause / Resume / Stop`.
- Profile management (name/sex/age/height/weight/hr_rest/hr_max).
- Activity selection and live kcal estimate (active kcal).
- FPS lock (`Auto` or fixed caps).

## Setup & Run

### Windows (PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m bleakheart_ui
```

### Linux / macOS (bash/zsh)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
python -m bleakheart_ui
```

## Architecture (current)

Code is organized into package layers:
- `bleakheart_ui/features/*`: UI features (`main`, `sessions`)
- `bleakheart_ui/core/*`: BLE + recording engine/state
- `bleakheart_ui/infra/*`: persistence/indexing/signal data loading
- `bleakheart_ui/shared/*`: shared rendering/runtime utilities

See `ARCHITECTURE.md` for module-level details and data flow.

## Basic validation

```bash
# Windows:
.\.venv\Scripts\python.exe -m compileall -q app.py bleakheart_ui tests
.\.venv\Scripts\python.exe -m pytest -q tests\test_ecg_render_core.py
.\.venv\Scripts\python.exe -m pytest -q tests\test_calorie_estimator.py

# Linux/macOS:
python -m compileall -q app.py bleakheart_ui tests
python -m pytest -q tests/test_ecg_render_core.py
python -m pytest -q tests/test_calorie_estimator.py
```

## Calorie estimator

Live/session calories now use a tiered estimator:
1. Workload branch when reliable workload inputs are available (power or speed/grade).
2. HRR branch (profile-aware MET bands) when `hr_rest` and `hr_max` are available.
3. Keytel fallback when richer inputs are missing.

Implementation notes:
- Keytel fallback does not apply an extra activity multiplier.
- Integration remains timestamp-based over irregular `dt`.
- Guardrails include HR sanity filtering, short-gap hold, and EMA smoothing.
- Session output stores active and gross totals plus estimator metadata in `energy_summary.json`.

## Platform dependencies & permissions

### Linux (BlueZ)
- Install and run BlueZ (`bluetoothd`) on the host system.
- Ensure your user can access Bluetooth (typically via the `bluetooth` group or distro-specific policy).
- If scans return no devices, verify adapter state:

```bash
bluetoothctl show
```

### macOS
- Grant Bluetooth permission to your terminal app (Terminal/iTerm) in macOS privacy settings.
- If launching from an IDE, that IDE may also need Bluetooth permission.
- If device discovery fails, fully quit and reopen the terminal after granting permission.

### Windows
- Use a BLE-capable adapter with current drivers.
- Keep Bluetooth enabled in Windows settings while scanning/recording.

## Known BLE troubleshooting

If scan/connect behavior is unstable, try this checklist in order:

1. Stop the app and power-cycle Bluetooth on the host.
2. Ensure the wearable/sensor is not connected to another app/device.
3. Remove stale pairing and pair again:
   - Windows: Bluetooth settings -> remove device -> rescan.
   - macOS: Bluetooth settings -> forget device -> rescan.
   - Linux: `bluetoothctl` -> `remove <MAC>` -> scan/pair again.
4. Restart Bluetooth services/stack:
   - Linux (systemd): `sudo systemctl restart bluetooth`
   - Windows/macOS: toggle Bluetooth off/on, then retry.
5. Verify permissions again (especially macOS terminal/IDE Bluetooth access).
6. Try a fresh app restart after adapter reset.
7. If still failing, capture logs from app output and include:
   - OS/version
   - adapter model
   - device model
   - exact step where failure occurs (scan/connect/start stream)

## Recording output

Session directory:

`sessions/<profile_id>_<activity>_<YYYYMMDD_HHMMSS>/`

Possible files (based on enabled streams):
- `HeartRate_recording.csv`
- `RRinterval_recording.csv`
- `RawECG_recording.csv`
- `Accelerometer_recording.csv`
- `PPG_recording.csv`
- `PPI_recording.csv`
- `GYRO_recording.csv`
- `MAG_recording.csv`
- `RawPMD_recording.csv`
- `energy_summary.json`
- `kcal_timeline.csv`

## Notes

- During active recording, controls that could create inconsistent state are locked.
- `Pause` keeps live streams visible and stops file writes.
- `Stop` finalizes outputs and releases recording resources.
