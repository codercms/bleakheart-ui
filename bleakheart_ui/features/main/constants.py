ACTIVITY_OPTIONS = [
    "Rest",
    "Elliptical",
    "Walking",
    "Running",
    "Cycling",
    "Strength Training",
    "Other",
]

DEFAULT_PROFILE = {
    "name": "Participant 001",
    "sex": "male",
    "age_years": 30,
    "weight_kg": 75.0,
    "height_cm": 175.0,
    "hr_rest": 60,
    "hr_max": 190,
}

PMD_HELP = {
    "ECG": "Electrocardiogram. Electrical heart signal in microvolts (uV).",
    "ACC": "Accelerometer. Motion signal on X/Y/Z axes (milli-g).",
    "PPG": "Photoplethysmography. Optical pulse waveform from supported Polar sensors.",
    "PPI": "Peak-to-peak interval. Beat interval frames from supported sensors.",
    "GYRO": "Gyroscope. Angular velocity stream.",
    "MAG": "Magnetometer. Magnetic field stream.",
}

SDK_HELP = (
    "Enable SDK mode on devices that support it (for example Polar Verity) "
    "to unlock additional measurement options."
)

HR_HELP = {
    "base": "Enable live Heart Rate and RR interval data from the standard BLE Heart Rate service.",
    "instant": "When enabled, heart rate is computed from each RR interval (beat-to-beat) instead of using the device average in the frame.",
    "unpack": "When enabled, multi-beat HR frames are split into individual beats so RR/HR timestamps are per-beat.",
}

# Rendering/perf tuning (kept centralized for safe A/B changes).
ECG_RENDER_DELAY_S = 0.030
