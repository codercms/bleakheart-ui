# BleakHeart Recorder UI

This app is a desktop UI for recording physiological data using the [`bleakheart`](https://github.com/fsmeraldi/bleakheart) library.

Desktop recorder UI for Polar devices built on:
- `bleakheart` (data decoding / PMD handling)
- `bleak` (BLE transport)
- `PySide6` + `pyqtgraph` (UI + live charts)

Main entrypoint:
- `app.py`

## Features

- Scan/connect/disconnect BLE devices.
- Auto-connect to last remembered device.
- Live streams:
  - HR / RR
  - PMD measurements (device-dependent): `ECG`, `ACC`, `PPG`, `PPI`, `GYRO`, `MAG`
  - Battery
- Record sessions to CSV files with `Start / Pause / Resume / Stop`.
- Profile management (name/sex/age/height/weight/hr_rest/hr_max).
- Activity selection and live kcal estimate.
- FPS lock (`Auto` or fixed caps).

## Run (Windows)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -U pip
py -m pip install -r requirements.txt
py app.py
```

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
