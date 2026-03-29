from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Tuple

import numpy as np


@dataclass(frozen=True, slots=True)
class RenderConfig:
    window_s: float = 20.0
    render_delay_s: float = 0.015
    clamp_left: bool = True
    clamp_right: bool = True


class EcgRingBuffer:
    __slots__ = ("capacity", "_t", "_v", "_start", "_size")

    def __init__(self, capacity: int):
        if capacity < 2:
            raise ValueError("capacity must be >= 2")
        self.capacity = int(capacity)
        self._t = np.empty(self.capacity, dtype=np.float64)
        self._v = np.empty(self.capacity, dtype=np.float32)
        self._start = 0
        self._size = 0

    @property
    def size(self) -> int:
        return self._size

    def clear(self):
        self._start = 0
        self._size = 0

    def push(self, ts: float, uv: float) -> None:
        ts = float(ts)
        uv = float(uv)
        if self._size > 0:
            last_t = self.last_timestamp()
            if ts < last_t:
                raise ValueError(f"timestamps must be monotonic: got {ts} < {last_t}")
        idx = (self._start + self._size) % self.capacity
        if self._size < self.capacity:
            self._t[idx] = ts
            self._v[idx] = uv
            self._size += 1
        else:
            self._t[self._start] = ts
            self._v[self._start] = uv
            self._start = (self._start + 1) % self.capacity

    def last_timestamp(self) -> float:
        if self._size <= 0:
            raise IndexError("buffer is empty")
        idx = (self._start + self._size - 1) % self.capacity
        return float(self._t[idx])

    def snapshot(self) -> Tuple[np.ndarray, np.ndarray]:
        if self._size == 0:
            return np.empty(0, dtype=np.float64), np.empty(0, dtype=np.float32)
        end = (self._start + self._size) % self.capacity
        if self._start < end:
            return self._t[self._start:end].copy(), self._v[self._start:end].copy()
        t = np.concatenate((self._t[self._start:], self._t[:end]))
        v = np.concatenate((self._v[self._start:], self._v[:end]))
        return t, v


class EcgResampler:
    __slots__ = ("width", "cfg", "_tx", "_x_rel", "_pos")

    def __init__(self, width: int, cfg: RenderConfig):
        if width < 2:
            raise ValueError("width must be >= 2")
        self.width = int(width)
        self.cfg = cfg
        self._tx = np.empty(self.width, dtype=np.float64)
        self._x_rel = np.linspace(0.0, cfg.window_s, self.width, dtype=np.float32)
        self._pos = np.empty((self.width, 2), dtype=np.float32)
        self._pos[:, 0] = self._x_rel

    def resize(self, width: int):
        if width < 2:
            raise ValueError("width must be >= 2")
        if int(width) == self.width:
            return
        self.width = int(width)
        self._tx = np.empty(self.width, dtype=np.float64)
        self._x_rel = np.linspace(0.0, self.cfg.window_s, self.width, dtype=np.float32)
        self._pos = np.empty((self.width, 2), dtype=np.float32)
        self._pos[:, 0] = self._x_rel

    def build_series(
        self,
        sample_t: np.ndarray,
        sample_uv: np.ndarray,
        now_monotonic: float | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        if sample_t.ndim != 1 or sample_uv.ndim != 1:
            raise ValueError("sample arrays must be 1-D")
        if len(sample_t) != len(sample_uv):
            raise ValueError("sample arrays must have equal length")
        if len(sample_t) < 2:
            raise ValueError("need at least 2 samples")
        if not np.all(sample_t[1:] >= sample_t[:-1]):
            raise ValueError("sample_t must be monotonic nondecreasing")

        now = time.monotonic() if now_monotonic is None else float(now_monotonic)
        t_end = now - float(self.cfg.render_delay_s)
        t_start = t_end - float(self.cfg.window_s)
        self._tx[:] = np.linspace(t_start, t_end, self.width, dtype=np.float64)

        left = float(sample_uv[0]) if self.cfg.clamp_left else np.nan
        right = float(sample_uv[-1]) if self.cfg.clamp_right else np.nan
        y = np.interp(self._tx, sample_t, sample_uv, left=left, right=right).astype(np.float32, copy=False)
        return self._x_rel, y

    def build_pos(
        self,
        sample_t: np.ndarray,
        sample_uv: np.ndarray,
        now_monotonic: float | None = None,
    ) -> np.ndarray:
        x, y = self.build_series(sample_t, sample_uv, now_monotonic=now_monotonic)
        self._pos[:, 0] = x
        self._pos[:, 1] = y
        return self._pos
