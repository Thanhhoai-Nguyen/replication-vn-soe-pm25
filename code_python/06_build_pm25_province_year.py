# -*- coding: utf-8 -*-
"""
06_build_pm25_province_year.py

Build province-year PM2.5 (zonal mean) for Vietnam using:
- GADM level-1 (provinces) GeoJSON
- SEDAC/ACAG Global Annual PM2.5 GeoTIFFs for 2020–2022

Output:
- pm25_province_year.csv (long format): one row per province-year
  Columns: gid_1, name_1, hasc_1, year, pm25_mean, pm25_std, pm25_min, pm25_max, n_pixels

Usage (example):
python 06_build_pm25_province_year.py \
  --gadm /path/to/gadm41_VNM_1.json \
  --tif2020 /path/to/...-2020-geotiff.tif \
  --tif2021 /path/to/...-2021-geotiff.tif \
  --tif2022 /path/to/...-2022-geotiff.tif \
  --out  /path/to/pm25_province_year.csv

Notes:
- Handles NoData (typically -3.4e38) automatically via raster metadata.
- CRS expected EPSG:4326 (works directly with your files).
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterstats import zonal_stats


def _fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix invalid geometries (self-intersections etc.) safely."""
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].buffer(0)
    gdf = gdf[~gdf["geometry"].is_empty & gdf["geometry"].notna()].copy()
    return gdf


def _load_gadm(gadm_path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(gadm_path)

    # Prefer standard key fields for stable merges later
    required = ["GID_1", "NAME_1"]
    for col in required:
        if col not in gdf.columns:
            raise ValueError(f"GADM file missing required column: {col}")

    # Ensure CRS is EPSG:4326 (lon/lat). Your GADM GeoJSON is typically CRS84/WGS84.
    if gdf.crs is None:
        # If CRS missing, assume WGS84 lon/lat
        gdf = gdf.set_crs(epsg=4326)
    else:
        gdf = gdf.to_crs(epsg=4326)

    gdf = _fix_geometries(gdf)

    # Keep only relevant columns to reduce memory
    keep_cols = [c for c in ["GID_1", "NAME_1", "HASC_1"] if c in gdf.columns] + ["geometry"]
    gdf = gdf[keep_cols].copy()
    gdf = gdf.rename(columns={"GID_1": "gid_1", "NAME_1": "name_1", "HASC_1": "hasc_1"})
    return gdf


def _zonal_mean_for_year(
    provinces: gpd.GeoDataFrame,
    tif_path: str,
    year: int,
    all_touched: bool = False,
) -> pd.DataFrame:
    if not os.path.exists(tif_path):
        raise FileNotFoundError(f"GeoTIFF not found for year {year}: {tif_path}")

    with rasterio.open(tif_path) as src:
        nodata = src.nodata
        # Some rasters store nodata as None; your PM2.5 files usually set -3.4e38
        if nodata is None:
            # Fallback: treat extremely negative float as nodata
            nodata = -3.4e38

        # Compute zonal stats
        zs = zonal_stats(
            vectors=provinces.geometry,
            raster=tif_path,
            stats=["mean", "std", "min", "max", "count"],
            nodata=nodata,
            all_touched=all_touched,
            geojson_out=False,
        )

    # Build dataframe
    out = provinces[["gid_1", "name_1"]].copy()
    if "hasc_1" in provinces.columns:
        out["hasc_1"] = provinces["hasc_1"].values

    out["year"] = year
    out["pm25_mean"] = [d.get("mean", np.nan) for d in zs]
    out["pm25_std"] = [d.get("std", np.nan) for d in zs]
    out["pm25_min"] = [d.get("min", np.nan) for d in zs]
    out["pm25_max"] = [d.get("max", np.nan) for d in zs]
    out["n_pixels"] = [d.get("count", 0) for d in zs]

    # Ensure numeric
    for c in ["pm25_mean", "pm25_std", "pm25_min", "pm25_max"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["n_pixels"] = pd.to_numeric(out["n_pixels"], errors="coerce").fillna(0).astype(int)

    return out


def build_pm25_province_year(
    gadm_path: str,
    tifs_by_year: Dict[int, str],
    out_path: str,
    all_touched: bool = False,
) -> pd.DataFrame:
    provinces = _load_gadm(gadm_path)

    frames: List[pd.DataFrame] = []
    for year, tif_path in sorted(tifs_by_year.items()):
        df_year = _zonal_mean_for_year(provinces, tif_path, year, all_touched=all_touched)
        frames.append(df_year)

    df = pd.concat(frames, ignore_index=True)

    # Basic sanity: 63 provinces * 3 years = 189 rows (if using current admin count)
    # Do not hard-fail because boundary files could differ slightly, but report.
    expected = len(provinces) * len(tifs_by_year)
    if len(df) != expected:
        print(f"[WARN] Row count {len(df)} != expected {expected} (provinces={len(provinces)}).")

    # Save
    os.makedirs(os.path.dirname(out_path), exist_ok=True) if os.path.dirname(out_path) else None
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build pm25_province_year for Vietnam (2020–2022).")
    p.add_argument("--gadm", required=True, help="Path to gadm41_VNM_1.geojson/.json")
    p.add_argument("--tif2020", required=True, help="Path to PM2.5 GeoTIFF for 2020")
    p.add_argument("--tif2021", required=True, help="Path to PM2.5 GeoTIFF for 2021")
    p.add_argument("--tif2022", required=True, help="Path to PM2.5 GeoTIFF for 2022")
    p.add_argument("--out", required=True, help="Output CSV path (e.g., data/processed/pm25_province_year.csv)")
    p.add_argument(
        "--all_touched",
        action="store_true",
        help="If set, include all pixels touched by polygon edges (can slightly change means). Default: False.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    tifs_by_year = {2020: args.tif2020, 2021: args.tif2021, 2022: args.tif2022}
    df = build_pm25_province_year(
        gadm_path=args.gadm,
        tifs_by_year=tifs_by_year,
        out_path=args.out,
        all_touched=args.all_touched,
    )
    print(f"[OK] Saved: {args.out}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()