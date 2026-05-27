import numpy as np
import pytest

from pothole_backend.models import AccelBurst
from pothole_backend.scoring import haversine_m, score_severity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _burst(z_values):
    n = len(z_values)
    return AccelBurst(z_values=z_values, timestamps_ms=list(range(0, n * 5, 5)))


def _sine_burst(freq_hz: float, amplitude: float, n: int = 100, sample_rate: int = 200):
    t = np.linspace(0, n / sample_rate, n)
    z = (amplitude * np.sin(2 * np.pi * freq_hz * t)).tolist()
    return _burst(z)


# ---------------------------------------------------------------------------
# score_severity
# ---------------------------------------------------------------------------

class TestScoreSeverity:
    def test_flat_zero_signal_returns_zero(self):
        assert score_severity(_burst([0.0] * 100)) == 0.0

    def test_dc_offset_only_returns_zero(self):
        # Mean subtraction removes DC; no frequency energy remains
        assert score_severity(_burst([9.81] * 100)) == 0.0

    def test_pothole_band_12hz_scores_high(self):
        # 12 Hz sits squarely in the 8–20 Hz pothole band
        score = score_severity(_sine_burst(freq_hz=12, amplitude=4 * 9.81))
        assert score > 0.3

    def test_out_of_band_50hz_scores_low(self):
        score = score_severity(_sine_burst(freq_hz=50, amplitude=4 * 9.81))
        assert score < 0.2

    def test_score_clamped_to_one(self):
        score = score_severity(_sine_burst(freq_hz=12, amplitude=1_000))
        assert score <= 1.0

    def test_score_is_non_negative(self):
        score = score_severity(_sine_burst(freq_hz=10, amplitude=2 * 9.81))
        assert score >= 0.0

    def test_higher_amplitude_scores_higher(self):
        low  = score_severity(_sine_burst(freq_hz=12, amplitude=1.0))
        high = score_severity(_sine_burst(freq_hz=12, amplitude=4 * 9.81))
        assert high > low

    def test_minimum_burst_length(self):
        # 50 samples is the model minimum — must not raise
        burst = _burst([0.5] * 50)
        assert isinstance(score_severity(burst), float)


# ---------------------------------------------------------------------------
# haversine_m
# ---------------------------------------------------------------------------

class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine_m(47.0, -122.0, 47.0, -122.0) == 0.0

    def test_one_degree_latitude_approx_111km(self):
        d = haversine_m(0.0, 0.0, 1.0, 0.0)
        assert abs(d - 111_195) < 200

    def test_symmetric(self):
        a = haversine_m(47.25, -122.44, 47.30, -122.50)
        b = haversine_m(47.30, -122.50, 47.25, -122.44)
        assert abs(a - b) < 0.001

    def test_points_within_cluster_radius(self):
        # ~1 m apart — well inside default 5 m radius
        d = haversine_m(47.252900, -122.444300, 47.252901, -122.444301)
        assert d < 5.0

    def test_points_outside_cluster_radius(self):
        # 1 km apart
        d = haversine_m(47.2529, -122.4443, 47.2619, -122.4443)
        assert d > 5.0
