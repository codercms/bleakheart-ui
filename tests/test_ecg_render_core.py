import numpy as np
import pytest

from ecg_render_core import EcgResampler, EcgRingBuffer, RenderConfig


def test_ring_push_snapshot_order():
    rb = EcgRingBuffer(capacity=4)
    rb.push(1.0, 10.0)
    rb.push(2.0, 20.0)
    rb.push(3.0, 30.0)
    t, v = rb.snapshot()
    assert np.allclose(t, [1.0, 2.0, 3.0])
    assert np.allclose(v, [10.0, 20.0, 30.0])


def test_ring_overwrite_keeps_latest():
    rb = EcgRingBuffer(capacity=3)
    rb.push(1.0, 10.0)
    rb.push(2.0, 20.0)
    rb.push(3.0, 30.0)
    rb.push(4.0, 40.0)
    t, v = rb.snapshot()
    assert np.allclose(t, [2.0, 3.0, 4.0])
    assert np.allclose(v, [20.0, 30.0, 40.0])


def test_ring_rejects_non_monotonic():
    rb = EcgRingBuffer(capacity=4)
    rb.push(1.0, 10.0)
    with pytest.raises(ValueError):
        rb.push(0.9, 20.0)


def test_resampler_interpolates_linear():
    cfg = RenderConfig(window_s=1.0, render_delay_s=0.0)
    rs = EcgResampler(width=5, cfg=cfg)
    t = np.array([0.0, 1.0], dtype=np.float64)
    v = np.array([0.0, 100.0], dtype=np.float32)
    x, y = rs.build_series(t, v, now_monotonic=1.0)
    assert np.allclose(x, [0.0, 0.25, 0.5, 0.75, 1.0], atol=1e-6)
    assert np.allclose(y, [0.0, 25.0, 50.0, 75.0, 100.0], atol=1e-4)


def test_resampler_clamp_behavior():
    cfg = RenderConfig(window_s=4.0, render_delay_s=0.0, clamp_left=True, clamp_right=True)
    rs = EcgResampler(width=5, cfg=cfg)
    t = np.array([10.0, 11.0], dtype=np.float64)
    v = np.array([5.0, 15.0], dtype=np.float32)
    _x, y = rs.build_series(t, v, now_monotonic=12.0)  # tx=[8,9,10,11,12]
    assert np.allclose(y, [5.0, 5.0, 5.0, 15.0, 15.0], atol=1e-4)
