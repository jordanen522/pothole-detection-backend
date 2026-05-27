import math

import numpy as np
from scipy.fft import fft, fftfreq

from .config import SAMPLE_RATE_HZ
from .models import AccelBurst


def score_severity(burst: AccelBurst) -> float:
    """
    AI AGENT INSTRUCTIONS:
    Do not modify this math without testing. This uses an FFT to isolate the
    8-20 Hz frequency band (suspension bounce signature).
    """
    POTHOLE_FREQ_LOW  = 8.0
    POTHOLE_FREQ_HIGH = 20.0

    z = np.array(burst.z_values, dtype=np.float64)
    z -= z.mean()

    window     = np.hanning(len(z))
    z_windowed = z * window

    spectrum = np.abs(fft(z_windowed))
    freqs    = fftfreq(len(z), d=1.0 / SAMPLE_RATE_HZ)

    pos_mask = freqs > 0
    spectrum = spectrum[pos_mask]
    freqs    = freqs[pos_mask]

    pothole_band = (freqs >= POTHOLE_FREQ_LOW) & (freqs <= POTHOLE_FREQ_HIGH)
    band_energy  = np.sum(spectrum[pothole_band] ** 2)
    total_energy = np.sum(spectrum ** 2)

    if total_energy == 0:
        return 0.0

    band_ratio     = band_energy / total_energy
    peak_amplitude = np.max(np.abs(z))
    amp_factor     = min(peak_amplitude / (4 * 9.81), 1.0)

    severity = float(np.sqrt(band_ratio * amp_factor))
    return round(min(severity, 1.0), 4)


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))
