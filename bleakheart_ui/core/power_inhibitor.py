import ctypes
import shutil
import subprocess
import sys


class PowerInhibitor:
    """Prevent sleep/display-off while critical work (recording) is active."""

    ES_SYSTEM_REQUIRED = 0x00000001
    ES_DISPLAY_REQUIRED = 0x00000002
    ES_CONTINUOUS = 0x80000000

    def __init__(self, *, app_name: str, reason: str):
        self._app_name = str(app_name or "Application")
        self._reason = str(reason or "Critical activity in progress")
        self._active = False
        self._mode = None
        self._process = None

    @property
    def is_active(self) -> bool:
        return bool(self._active)

    def set_active(self, active: bool) -> bool:
        if active:
            return self.acquire()
        self.release()
        return True

    def acquire(self) -> bool:
        if self._active:
            return True
        if sys.platform.startswith("win"):
            return self._acquire_windows()
        if sys.platform == "darwin":
            return self._acquire_caffeinate()
        return self._acquire_systemd_inhibit()

    def release(self):
        if not self._active:
            return
        if self._mode == "windows":
            try:
                ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)
            except Exception:
                pass
        elif self._mode == "process":
            proc = self._process
            self._process = None
            if proc is not None:
                try:
                    proc.terminate()
                    proc.wait(timeout=1.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        self._active = False
        self._mode = None

    def _acquire_windows(self) -> bool:
        try:
            flags = self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED | self.ES_DISPLAY_REQUIRED
            res = ctypes.windll.kernel32.SetThreadExecutionState(flags)
        except Exception:
            return False
        if not res:
            return False
        self._active = True
        self._mode = "windows"
        return True

    def _acquire_caffeinate(self) -> bool:
        if shutil.which("caffeinate") is None:
            return False
        cmd = ["caffeinate", "-d", "-i"]
        return self._start_process_inhibitor(cmd)

    def _acquire_systemd_inhibit(self) -> bool:
        if shutil.which("systemd-inhibit") is None:
            return False
        cmd = [
            "systemd-inhibit",
            "--what=sleep:idle",
            "--who",
            self._app_name,
            "--why",
            self._reason,
            "sh",
            "-c",
            "while true; do sleep 3600; done",
        ]
        return self._start_process_inhibitor(cmd)

    def _start_process_inhibitor(self, cmd: list[str]) -> bool:
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return False
        if proc.poll() is not None:
            return False
        self._process = proc
        self._active = True
        self._mode = "process"
        return True
