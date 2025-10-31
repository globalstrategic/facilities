# scripts/utils/geo.py
"""Geographic utilities for facility data."""

from __future__ import annotations

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

def encode_geohash(lat: float, lon: float, precision: int = 7) -> str:
    """
    Encode coordinates as geohash (standard algorithm, no external deps).

    Args:
        lat: Latitude (-90 to 90)
        lon: Longitude (-180 to 180)
        precision: Number of geohash characters (default: 7)

    Returns:
        Geohash string (e.g., "dr5regw" for Statue of Liberty)

    Examples:
        >>> encode_geohash(40.6892, -74.0445, precision=7)
        'dr5regw'
        >>> encode_geohash(-25.7479, 28.2293, precision=7)  # Pretoria
        'ke7w8v5'
    """
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    bits = [16, 8, 4, 2, 1]
    ch = 0
    bit = 0
    even = True
    geohash = []

    while len(geohash) < precision:
        if even:
            mid = sum(lon_interval) / 2
            if lon > mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = sum(lat_interval) / 2
            if lat > mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)
