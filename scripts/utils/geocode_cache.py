# scripts/utils/geocode_cache.py
from __future__ import annotations
import os, io, json, tempfile, shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

DEFAULT_PATH = os.path.join("data", "geocode_cache.parquet")

@dataclass
class _Stats:
    hits: int = 0
    misses: int = 0
    loads: int = 0
    saves: int = 0
    pruned: int = 0

class GeocodeCache:
    """
    Persistent cache for Nominatim reverse geocoding.
    Keyed by (lat_r, lon_r, zoom), where lat/lon are rounded to `precision` decimals (~11m at precision=4).
    Values are dicts with:
      - 'address' (full Nominatim address dict)
      - 'town','city','municipality','village','hamlet' (flattened for convenience)
      - 'ts' ISO timestamp
    """
    def __init__(
        self,
        path: str = DEFAULT_PATH,
        ttl_days: int = 365,
        precision: int = 4,
        default_zoom: int = 10,
        prefer_parquet: bool = True
    ):
        self.path = path
        self.ttl = timedelta(days=ttl_days)
        self.precision = precision
        self.zoom = default_zoom
        self.prefer_parquet = prefer_parquet
        self._df = None  # pandas DataFrame or list (fallback)
        self._stats = _Stats()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    # ------------- context manager -------------
    def __enter__(self):
        self._load()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._prune()
        self._save()

    # ------------- public API -------------
    def get(self, lat: float, lon: float, zoom: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Return address dict if present and not expired; else None."""
        key = self._key(lat, lon, zoom)
        row = self._lookup_row(key)
        if row is None:
            self._stats.misses += 1
            return None

        if self._expired(row["ts"]):
            self._stats.misses += 1
            return None

        self._stats.hits += 1
        return row["address"]

    def set(self, lat: float, lon: float, address: Dict[str, Any], zoom: Optional[int] = None) -> None:
        """Insert/update cache entry for (lat, lon, zoom)."""
        now_iso = datetime.utcnow().isoformat() + "Z"
        key = self._key(lat, lon, zoom)
        flat = self._flatten_address(address)
        record = {
            "lat_r": key[0],
            "lon_r": key[1],
            "zoom": key[2],
            "town": flat.get("town"),
            "city": flat.get("city"),
            "municipality": flat.get("municipality"),
            "village": flat.get("village"),
            "hamlet": flat.get("hamlet"),
            "address": address,
            "ts": now_iso,
        }
        if pd is not None:
            if self._df is None:
                self._df = pd.DataFrame([record])
            else:
                # upsert on (lat_r, lon_r, zoom)
                mask = (
                    (self._df["lat_r"] == record["lat_r"]) &
                    (self._df["lon_r"] == record["lon_r"]) &
                    (self._df["zoom"] == record["zoom"])
                )
                if mask.any():
                    self._df.loc[mask, list(record.keys())] = record  # overwrite row
                else:
                    self._df.loc[len(self._df)] = record
        else:
            # Minimal JSONL fallback: store in-memory list of records; dedupe on save
            if self._df is None:
                self._df = []
            self._df.append(record)

    # ------------- helpers -------------
    def _key(self, lat: float, lon: float, zoom: Optional[int]) -> Tuple[float, float, int]:
        return (round(float(lat), self.precision), round(float(lon), self.precision), int(zoom or self.zoom))

    def _flatten_address(self, addr: Dict[str, Any]) -> Dict[str, Optional[str]]:
        get = addr.get
        return {
            "town": get("town"),
            "city": get("city"),
            "municipality": get("municipality"),
            "village": get("village"),
            "hamlet": get("hamlet"),
        }

    def _expired(self, ts_iso: str) -> bool:
        try:
            ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
            # Make both naive for comparison (convert to UTC)
            if ts.tzinfo is not None:
                ts = ts.replace(tzinfo=None)
        except Exception:
            return True
        return (datetime.utcnow() - ts) > self.ttl

    def _lookup_row(self, key: Tuple[float, float, int]) -> Optional[Dict[str, Any]]:
        if self._df is None:
            return None
        lat_r, lon_r, zoom = key
        if pd is not None:
            mask = (
                (self._df["lat_r"] == lat_r) &
                (self._df["lon_r"] == lon_r) &
                (self._df["zoom"] == zoom)
            )
            if not mask.any():
                return None
            row = self._df[mask].iloc[-1].to_dict()
            return row
        else:
            # JSONL fallback: linear scan (small)
            for row in reversed(self._df):  # latest first
                if row["lat_r"] == lat_r and row["lon_r"] == lon_r and row["zoom"] == zoom:
                    return row
            return None

    def _prune(self):
        """Drop expired rows; update stats."""
        if self._df is None:
            return
        if pd is not None:
            before = len(self._df)
            self._df = self._df[~self._df["ts"].map(self._expired)]
            self._stats.pruned += (before - len(self._df))
        else:
            before = len(self._df)
            self._df = [r for r in self._df if not self._expired(r["ts"])]
            self._stats.pruned += (before - len(self._df))

    def stats(self) -> Dict[str, Any]:
        size = 0 if self._df is None else (len(self._df) if pd is None else int(self._df.shape[0]))
        return {
            "size": size,
            "hits": self._stats.hits,
            "misses": self._stats.misses,
            "loads": self._stats.loads,
            "saves": self._stats.saves,
            "pruned": self._stats.pruned,
            "path": self.path,
            "ttl_days": int(self.ttl.total_seconds() // 86400),
            "precision": self.precision,
            "zoom": self.zoom,
            "backend": "parquet" if (pd is not None and self.prefer_parquet) else "jsonl",
        }

    # ------------- persistence -------------
    def _load(self):
        """Load existing cache from disk if present."""
        if not os.path.exists(self.path):
            # JSONL fallback path (same prefix)
            alt = self.path.rsplit(".", 1)[0] + ".jsonl"
            if os.path.exists(alt):
                self._load_jsonl(alt)
            else:
                self._df = (pd.DataFrame(columns=[
                    "lat_r","lon_r","zoom","town","city","municipality","village","hamlet","address","ts"
                ]) if pd is not None else [])
            return

        try:
            if pd is not None and self.prefer_parquet:
                self._df = pd.read_parquet(self.path)
            else:
                self._load_jsonl(self.path.rsplit(".", 1)[0] + ".jsonl")
            self._stats.loads += 1
        except Exception:
            # Corrupt or missing deps: fallback to empty
            self._df = (pd.DataFrame(columns=[
                "lat_r","lon_r","zoom","town","city","municipality","village","hamlet","address","ts"
            ]) if pd is not None else [])

    def _save(self):
        """Persist cache to disk (atomic write)."""
        if self._df is None:
            return
        tmpdir = tempfile.mkdtemp(prefix="geocache_")
        try:
            if pd is not None and self.prefer_parquet:
                tmp_path = os.path.join(tmpdir, "cache.parquet")
                # ensure address column stays JSON-serializable
                df = self._df.copy()
                if not df.empty:
                    df["address"] = df["address"].map(lambda x: json.dumps(x) if isinstance(x, dict) else (x or ""))
                    df["address"] = df["address"].map(json.loads)
                df.to_parquet(tmp_path, index=False)
                self._replace_file(tmp_path, self.path)
            else:
                # JSONL fallback
                jsonl_path = self.path.rsplit(".", 1)[0] + ".jsonl"
                tmp_path = os.path.join(tmpdir, "cache.jsonl")
                with io.open(tmp_path, "w", encoding="utf-8") as f:
                    rows = self._df.to_dict("records") if pd is not None else self._df
                    for r in rows:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                self._replace_file(tmp_path, jsonl_path)
            self._stats.saves += 1
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def _load_jsonl(self, path: str):
        rows = []
        try:
            with io.open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        rows.append(json.loads(line))
        except Exception:
            rows = []
        if pd is not None:
            self._df = pd.DataFrame(rows, columns=[
                "lat_r","lon_r","zoom","town","city","municipality","village","hamlet","address","ts"
            ])
        else:
            self._df = rows

    def _replace_file(self, tmp_path: str, dest_path: str):
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        # atomic move
        if os.path.exists(dest_path):
            os.replace(tmp_path, dest_path)
        else:
            shutil.move(tmp_path, dest_path)
