from dataclasses import dataclass
from typing import Any


@dataclass
class RecordingSnapshot:
    config: Any | None
    should_resume: bool


class RecordingManager:
    def __init__(self):
        self.recording = False
        self.paused = False
        self._config: Any | None = None
        self._resume_after_reconnect = False

    @property
    def config(self) -> Any | None:
        return self._config

    def start(self, config: Any):
        if self.recording:
            raise RuntimeError("Recording already running")
        self.recording = True
        self.paused = False
        self._config = config
        self._resume_after_reconnect = False

    def stop(self):
        self.recording = False
        self.paused = False
        self._config = None
        self._resume_after_reconnect = False

    def pause_manual(self) -> bool:
        if (not self.recording) or self.paused:
            return False
        self.paused = True
        self._resume_after_reconnect = False
        return True

    def resume_manual(self) -> bool:
        if (not self.recording) or (not self.paused):
            return False
        self.paused = False
        self._resume_after_reconnect = False
        return True

    def pause_for_disconnect(self) -> bool:
        if not self.recording:
            return False
        self._resume_after_reconnect = (not self.paused)
        if self.paused:
            return False
        self.paused = True
        return True

    def reconnect_snapshot(self) -> RecordingSnapshot:
        if (not self.recording) or (self._config is None):
            return RecordingSnapshot(config=None, should_resume=False)
        return RecordingSnapshot(config=self._config, should_resume=bool(self._resume_after_reconnect))

    def reconnect_restored(self, resumed: bool):
        if not self.recording:
            return
        self.paused = (not resumed)
        self._resume_after_reconnect = False

    def reconnect_restore_failed(self):
        if not self.recording:
            return
        self.paused = True
        self._resume_after_reconnect = True
