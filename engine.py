import asyncio as aio
import csv
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bleak import BleakClient, BleakScanner
import bleakheart as bh
from recording_manager import RecordingManager


PMD_TYPES = ("ECG", "ACC", "PPG", "PPI", "GYRO", "MAG")


@dataclass
class RecordingConfig:
    session_id: str
    record_hr: bool = True
    instant_rate: bool = True
    unpack_hr: bool = True
    enable_sdk_mode: bool = False
    pmd_measurements: tuple[str, ...] = ()


class BleakHeartEngine:
    def __init__(self, events: queue.Queue, base_dir: Path | None = None):
        self.events = events
        self.base_dir = base_dir or (Path(__file__).resolve().parent / "sessions")
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.loop = aio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

        self.client: BleakClient | None = None
        self.device = None
        self.connected = False
        self.recording_mgr = RecordingManager()
        self.available_measurements: list[str] = []

        self.hr: bh.HeartRate | None = None
        self.hr_live_enabled = False
        self.pmd: bh.PolarMeasurementData | None = None
        self.active_measurements: set[str] = set()
        self.preview_measurements: set[str] = set()
        self.recording_measurements: set[str] = set()
        self.sdk_active = False

        self.files: dict[str, Any] = {}
        self.writers: dict[str, Any] = {}
        self.session_path: Path | None = None
        self.sample_rates = {"ECG": 130.0, "ACC": 200.0, "PPG": 55.0}
        self._flush_interval_s = 1.0
        self._last_flush_mono = 0.0

    @property
    def recording(self) -> bool:
        return bool(self.recording_mgr.recording)

    @property
    def paused(self) -> bool:
        return bool(self.recording_mgr.paused)

    def _run_loop(self):
        aio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _emit(self, event_type: str, **payload):
        self.events.put({"type": event_type, **payload})

    def _log(self, message: str, level: str = "info"):
        self._emit("log", level=level, message=message)

    def run(self, coro):
        return aio.run_coroutine_threadsafe(coro, self.loop)

    def shutdown(self):
        self.run(self._shutdown()).result(timeout=10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=2)

    async def _shutdown(self):
        if self.recording:
            await self._stop_recording()
        if self.connected:
            await self._disconnect()

    async def _scan(self, timeout: float = 4.0):
        devices = await BleakScanner.discover(timeout=timeout)
        result = []
        for d in devices:
            name = d.name or ""
            if "polar" not in name.lower():
                continue
            result.append(
                {
                    "name": d.name or "(no name)",
                    "address": d.address,
                    "rssi": getattr(d, "rssi", None),
                }
            )
        result.sort(key=lambda x: (x["name"].lower(), x["address"]))
        self._log(f"Scan complete: {len(result)} devices found.")
        return result

    def scan(self, timeout: float = 4.0):
        return self.run(self._scan(timeout))

    async def _connect(
        self,
        address: str,
        hr_live_enabled: bool = True,
        preview_pmd_measurements: tuple[str, ...] = (),
    ):
        if self.connected:
            await self._disconnect()

        device = await BleakScanner.find_device_by_address(address, timeout=8.0)
        self.device = device or address
        self.client = BleakClient(self.device, disconnected_callback=self._on_disconnected)
        await self.client.connect(timeout=12.0)
        if not self.client.is_connected:
            raise RuntimeError("Connection failed")
        self.connected = True
        self._log(f"Connected to {address}")

        battery = None
        try:
            battery = await bh.BatteryLevel(self.client).read()
            self._emit("battery", value=battery)
            self._log(f"Battery: {battery}%")
        except Exception as exc:
            self._log(f"Battery read unavailable: {exc}", level="warn")

        self.pmd = bh.PolarMeasurementData(self.client, callback=self._on_pmd_frame)
        try:
            self.available_measurements = await self.pmd.available_measurements()
        except Exception as exc:
            self.available_measurements = []
            self._log(f"PMD capability query failed: {exc}", level="warn")

        try:
            await self._set_hr_live(hr_live_enabled)
        except Exception as exc:
            self._log(f"Live HR setup failed: {exc}", level="warn")
        try:
            await self._set_preview_measurements(preview_pmd_measurements)
        except Exception as exc:
            self._log(f"Live PMD setup failed (continuing without PMD): {exc}", level="warn")

        snap = self.recording_mgr.reconnect_snapshot()
        if snap.config is not None:
            try:
                await self._activate_recording_streams(snap.config)
                if snap.should_resume:
                    self.recording_mgr.reconnect_restored(resumed=True)
                    self._emit("recording_resumed")
                    self._log("Recording resumed after reconnect.")
                else:
                    self.recording_mgr.reconnect_restored(resumed=False)
                    self._log("Recording streams restored after reconnect (still paused).")
            except Exception as exc:
                self.recording_mgr.reconnect_restore_failed()
                self._log(f"Recording restore after reconnect failed: {exc}", level="warn")

        self._emit(
            "connected",
            address=address,
            battery=battery,
            available_measurements=self.available_measurements,
        )
        return {
            "address": address,
            "battery": battery,
            "available_measurements": self.available_measurements,
        }

    def connect(
        self,
        address: str,
        hr_live_enabled: bool = True,
        preview_pmd_measurements: tuple[str, ...] = (),
    ):
        return self.run(self._connect(address, hr_live_enabled, preview_pmd_measurements))

    def _on_disconnected(self, _client: BleakClient):
        self._log("Device disconnected.", level="warn")
        self._emit("disconnected")
        try:
            self.run(self._handle_disconnect_cleanup())
        except RuntimeError:
            pass

    async def _handle_disconnect_cleanup(self):
        if self.recording:
            if self.recording_mgr.pause_for_disconnect():
                self._maybe_flush(force=True)
                self._emit("recording_paused")
                self._log("Recording paused due to disconnect.", level="warn")
        self.connected = False
        self.client = None
        if self.hr is not None:
            try:
                await self.hr.stop_notify()
            except Exception:
                pass
        self.hr = None
        self.pmd = None
        self.active_measurements.clear()
        self.preview_measurements.clear()
        self.recording_measurements.clear()
        self.sdk_active = False

    async def _disconnect(self):
        if self.recording:
            await self._stop_recording()
        if self.hr is not None:
            try:
                await self.hr.stop_notify()
            except Exception:
                pass
            self.hr = None
        if self.client is not None and self.client.is_connected:
            await self.client.disconnect()
        self.connected = False
        self.client = None
        self.hr = None
        self.pmd = None
        self.active_measurements.clear()
        self.preview_measurements.clear()
        self.recording_measurements.clear()
        self.sdk_active = False
        self._log("Disconnected.")

    def disconnect(self):
        return self.run(self._disconnect())

    async def _activate_recording_streams(self, config: RecordingConfig):
        if self.client is None:
            raise RuntimeError("Not connected")

        if config.record_hr and self.hr is None:
            self.hr = bh.HeartRate(
                self.client,
                callback=self._on_hr_frame,
                instant_rate=config.instant_rate,
                unpack=config.unpack_hr,
            )
            await self.hr.start_notify()
            self._log("Heart rate notifications started.")

        if self.pmd is None:
            self.pmd = bh.PolarMeasurementData(self.client, callback=self._on_pmd_frame)
        self.recording_measurements.clear()
        self.sdk_active = False

        if config.enable_sdk_mode and "SDK" in self.available_measurements:
            err_code, err_msg, _ = await self.pmd.start_streaming("SDK")
            if err_code == 0:
                self.sdk_active = True
                self._log("SDK mode enabled.")
            else:
                self._log(f"SDK mode failed: {err_msg}", level="warn")

        for meas in config.pmd_measurements:
            if meas not in self.available_measurements:
                self._log(f"{meas} is not available on this device.", level="warn")
                continue
            if meas in self.active_measurements:
                self.recording_measurements.add(meas)
                continue
            err_code, err_msg, _ = await self.pmd.start_streaming(meas)
            if err_code != 0:
                self._log(f"Failed to start {meas}: {err_msg}", level="warn")
                continue
            self.active_measurements.add(meas)
            self.recording_measurements.add(meas)
            try:
                settings = await self.pmd.available_settings(meas)
                if isinstance(settings, dict) and "SAMPLE_RATE" in settings and settings["SAMPLE_RATE"]:
                    self.sample_rates[meas] = float(settings["SAMPLE_RATE"][0])
            except Exception:
                pass
            self._log(f"{meas} streaming started.")

    async def _set_hr_live(self, enabled: bool):
        self.hr_live_enabled = enabled
        if not self.connected or self.client is None:
            return
        if enabled and self.hr is None:
            self.hr = bh.HeartRate(self.client, callback=self._on_hr_frame, instant_rate=True, unpack=True)
            await self.hr.start_notify()
            self._log("Live heart rate stream started.")
            return
        if (not enabled) and self.hr is not None and (not self.recording):
            try:
                await self.hr.stop_notify()
            except Exception as exc:
                self._log(f"Stopping live HR stream failed: {exc}", level="warn")
            self.hr = None
            self._log("Live heart rate stream stopped.")

    async def _set_preview_measurements(self, measurements: tuple[str, ...]):
        if not self.connected or self.pmd is None:
            self.preview_measurements = set(measurements)
            return

        desired = {m for m in measurements if m in self.available_measurements}

        # Stop preview streams that are no longer desired and not recording-owned
        for meas in list(self.preview_measurements):
            if meas in desired:
                continue
            if meas in self.recording_measurements:
                continue
            try:
                await self.pmd.stop_streaming(meas)
            except Exception as exc:
                self._log(f"Stopping preview {meas} failed: {exc}", level="warn")
            self.active_measurements.discard(meas)
            self.preview_measurements.discard(meas)
            self._log(f"Live {meas} preview stopped.")

        # Start newly requested preview streams
        for meas in desired:
            if meas in self.active_measurements:
                self.preview_measurements.add(meas)
                continue
            err_code, err_msg, _ = await self.pmd.start_streaming(meas)
            if err_code != 0:
                self._log(f"Live {meas} preview failed: {err_msg}", level="warn")
                continue
            self.active_measurements.add(meas)
            self.preview_measurements.add(meas)
            self._log(f"Live {meas} preview started.")

            try:
                settings = await self.pmd.available_settings(meas)
                if isinstance(settings, dict) and "SAMPLE_RATE" in settings and settings["SAMPLE_RATE"]:
                    self.sample_rates[meas] = float(settings["SAMPLE_RATE"][0])
            except Exception:
                pass

    def set_preview_measurements(self, measurements: tuple[str, ...]):
        return self.run(self._set_preview_measurements(measurements))

    async def _set_live_config(self, hr_live_enabled: bool, preview_measurements: tuple[str, ...]):
        await self._set_hr_live(hr_live_enabled)
        await self._set_preview_measurements(preview_measurements)

    def set_live_config(self, hr_live_enabled: bool, preview_measurements: tuple[str, ...]):
        return self.run(self._set_live_config(hr_live_enabled, preview_measurements))

    async def _read_battery(self):
        if not self.connected or self.client is None:
            raise RuntimeError("Not connected")
        battery = await bh.BatteryLevel(self.client).read()
        self._emit("battery", value=battery)
        self._log(f"Battery: {battery}%")
        return battery

    def read_battery(self):
        return self.run(self._read_battery())

    def _open_csv(self, key: str, filename: str, header: list[str]):
        path = self.session_path / filename
        f = path.open("w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(header)
        self.files[key] = f
        self.writers[key] = w
        self._log(f"Opened {path}")

    def _close_all_files(self):
        for handle in self.files.values():
            try:
                handle.flush()
                handle.close()
            except Exception:
                pass
        self.files.clear()
        self.writers.clear()

    def _maybe_flush(self, force: bool = False):
        if not self.files:
            return
        now = time.monotonic()
        if (not force) and ((now - float(self._last_flush_mono)) < float(self._flush_interval_s)):
            return
        for handle in self.files.values():
            try:
                handle.flush()
            except Exception:
                pass
        self._last_flush_mono = now

    async def _start_recording(self, config: RecordingConfig):
        if not self.connected or self.client is None:
            raise RuntimeError("Not connected")
        if self.recording:
            raise RuntimeError("Recording already running")

        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_id = "".join(c if c.isalnum() or c in "-_." else "_" for c in config.session_id.strip())
        safe_id = safe_id or "session"
        self.session_path = self.base_dir / f"{safe_id}_{now}"
        self.session_path.mkdir(parents=True, exist_ok=True)

        if config.record_hr:
            self._open_csv("HR", "HeartRate_recording.csv", ["timestamp_s", "heart_rate_bpm", "energy_kj"])
            self._open_csv("RR", "RRinterval_recording.csv", ["timestamp_s", "rr_ms", "heart_rate_bpm"])

        for meas in config.pmd_measurements:
            if meas == "ECG":
                self._open_csv("ECG", "RawECG_recording.csv", ["timestamp_s", "ecg_uV"])
            elif meas == "ACC":
                self._open_csv("ACC", "Accelerometer_recording.csv", ["timestamp_s", "x_mg", "y_mg", "z_mg"])
            elif meas == "PPG":
                self._open_csv("PPG", "PPG_recording.csv", ["timestamp_s", "ppg0", "ppg1", "ppg2", "ambient"])
            else:
                self._open_csv(meas, f"{meas}_recording.csv", ["timestamp_s", "payload"])

        self._open_csv("RAW", "RawPMD_recording.csv", ["dtype", "timestamp_s", "payload"])

        await self._activate_recording_streams(config)

        self.recording_mgr.start(config)
        self._last_flush_mono = time.monotonic()
        self._emit("recording_started", path=str(self.session_path))
        return str(self.session_path)

    def start_recording(self, config: RecordingConfig):
        return self.run(self._start_recording(config))

    async def _stop_recording(self):
        if not self.recording:
            return

        if self.pmd is not None:
            for meas in list(self.recording_measurements):
                try:
                    await self.pmd.stop_streaming(meas)
                    self.active_measurements.discard(meas)
                except Exception as exc:
                    self._log(f"Stopping {meas} failed: {exc}", level="warn")
            if self.sdk_active:
                try:
                    await self.pmd.stop_streaming("SDK")
                except Exception:
                    pass

        self.recording_measurements.clear()
        self.sdk_active = False
        self.recording_mgr.stop()
        # Restore live preview streams after recording-owned streams are stopped.
        if self.connected and self.pmd is not None and self.preview_measurements:
            try:
                await self._set_preview_measurements(tuple(sorted(self.preview_measurements)))
            except Exception as exc:
                self._log(f"Restoring preview streams failed: {exc}", level="warn")
        if (not self.hr_live_enabled) and self.hr is not None:
            try:
                await self.hr.stop_notify()
            except Exception:
                pass
            self.hr = None
        self._close_all_files()
        self._emit("recording_stopped")
        self._log("Recording stopped.")

    def stop_recording(self):
        return self.run(self._stop_recording())

    async def _pause_recording(self):
        if not self.recording:
            raise RuntimeError("Recording is not running")
        if not self.recording_mgr.pause_manual():
            return
        self._maybe_flush(force=True)
        self._emit("recording_paused")
        self._log("Recording paused.")

    def pause_recording(self):
        return self.run(self._pause_recording())

    async def _resume_recording(self):
        if not self.recording:
            raise RuntimeError("Recording is not running")
        if not self.recording_mgr.resume_manual():
            return
        self._emit("recording_resumed")
        self._log("Recording resumed.")

    def resume_recording(self):
        return self.run(self._resume_recording())

    def _on_hr_frame(self, frame):
        # Frame format (unpack=True): ('HR', t_ns, (hr, rr), energy)
        # Frame format (unpack=False): ('HR', t_ns, (avg_hr, [rr...]), energy)
        _, t_ns, payload, energy = frame
        ts = t_ns / 1e9

        rr_values: list[int] = []
        hr_value = None
        if isinstance(payload, tuple) and len(payload) == 2:
            hr_value = payload[0]
            if isinstance(payload[1], list):
                rr_values = payload[1]
            elif payload[1] is not None:
                rr_values = [payload[1]]

        write_enabled = self.recording and (not self.paused)

        if write_enabled and "HR" in self.writers and hr_value is not None:
            self.writers["HR"].writerow([f"{ts:.9f}", hr_value, energy if energy is not None else ""])

        if write_enabled and "RR" in self.writers:
            for rr in rr_values:
                self.writers["RR"].writerow([f"{ts:.9f}", rr, hr_value if hr_value is not None else ""])

        if write_enabled:
            self._maybe_flush()

        self._emit("hr_sample", timestamp_s=ts, heart_rate=hr_value, rr_values=rr_values)

    def _write_ecg(self, t_ns: int, samples: list[int], write_enabled: bool = True):
        sr = self.sample_rates.get("ECG", 130.0)
        step_ns = int(1e9 / sr)
        start_ns = t_ns - (len(samples) - 1) * step_ns
        if write_enabled and "ECG" in self.writers:
            for idx, sample in enumerate(samples):
                sample_ns = start_ns + idx * step_ns
                self.writers["ECG"].writerow([f"{sample_ns / 1e9:.9f}", sample])
        self._emit("ecg_samples", end_timestamp_s=t_ns / 1e9, samples=samples, sample_rate=sr)

    def _write_acc(self, t_ns: int, samples: list[tuple[int, int, int]], write_enabled: bool = True):
        sr = self.sample_rates.get("ACC", 200.0)
        step_ns = int(1e9 / sr)
        start_ns = t_ns - (len(samples) - 1) * step_ns
        if write_enabled and "ACC" in self.writers:
            for idx, (x, y, z) in enumerate(samples):
                sample_ns = start_ns + idx * step_ns
                self.writers["ACC"].writerow([f"{sample_ns / 1e9:.9f}", x, y, z])
        self._emit("acc_samples", end_timestamp_s=t_ns / 1e9, samples=samples, sample_rate=sr)

    def _write_ppg(self, t_ns: int, samples: list[tuple[int, int, int, int]], write_enabled: bool = True):
        if (not write_enabled) or ("PPG" not in self.writers):
            return
        sr = self.sample_rates.get("PPG", 55.0)
        step_ns = int(1e9 / sr)
        start_ns = t_ns - (len(samples) - 1) * step_ns
        for idx, sample in enumerate(samples):
            sample_ns = start_ns + idx * step_ns
            row = [f"{sample_ns / 1e9:.9f}"] + list(sample)
            self.writers["PPG"].writerow(row)

    def _on_pmd_frame(self, frame):
        # Generic format: (dtype, timestamp_ns, payload)
        dtype, t_ns, payload = frame
        ts = t_ns / 1e9

        write_enabled = self.recording and (not self.paused)

        if write_enabled and "RAW" in self.writers:
            self.writers["RAW"].writerow([dtype, f"{ts:.9f}", repr(payload)])

        if dtype == "ECG" and isinstance(payload, list):
            self._write_ecg(t_ns, payload, write_enabled=write_enabled)
            return
        if dtype == "ACC" and isinstance(payload, list):
            self._write_acc(t_ns, payload, write_enabled=write_enabled)
            return
        if dtype == "PPG" and isinstance(payload, list):
            self._write_ppg(t_ns, payload, write_enabled=write_enabled)
            return

        if write_enabled and dtype in self.writers:
            self.writers[dtype].writerow([f"{ts:.9f}", repr(payload)])
        if write_enabled:
            self._maybe_flush()
