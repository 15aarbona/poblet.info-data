"""
Llegeix els Parquet `historico_*` generats pels extractors, normalitza tipus i dates
i escriu snapshots nets a `data/staging/latest_<platform>.parquet`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from paths import repo_root

_PROJECT_ROOT = repo_root()


def _project_data_dir() -> Path:
    return _PROJECT_ROOT / "data"


def _staging_dir() -> Path:
    d = _project_data_dir() / "staging"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _parse_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=False)


def _clean_instagram(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["post_id"] = out["post_id"].astype(str)
    out["fecha_publicacion"] = _parse_datetime_series(out["fecha_publicacion"])
    for c in ("likes", "comentarios", "visualizaciones", "seguidores"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.drop_duplicates(subset=["post_id"], keep="last")
    return out


def _clean_tiktok(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["video_id"] = out["video_id"].astype(str)
    out["fecha_publicacion"] = _parse_datetime_series(out["fecha_publicacion"])
    for c in ("likes", "comentarios", "visualizaciones", "seguidores"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.drop_duplicates(subset=["video_id"], keep="last")
    return out


def _clean_youtube(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["id_video"] = out["id_video"].astype(str)
    out["fecha_publicacion"] = _parse_datetime_series(out["fecha_publicacion"])
    for c in ("likes", "comentarios", "visualizaciones", "seguidores"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.drop_duplicates(subset=["id_video"], keep="last")
    return out


def _clean_twitch(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["id_video"] = out["id_video"].astype(str)
    out["fecha_publicacion"] = _parse_datetime_series(out["fecha_publicacion"])
    for c in ("visualizaciones", "seguidores"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.drop_duplicates(subset=["id_video"], keep="last")
    return out


_CLEANERS = {
    "instagram": ("historico_instagram.parquet", "latest_instagram.parquet", _clean_instagram),
    "tiktok": ("historico_tiktok.parquet", "latest_tiktok.parquet", _clean_tiktok),
    "youtube": ("historico_youtube.parquet", "latest_youtube.parquet", _clean_youtube),
    "twitch": ("historico_twitch.parquet", "latest_twitch.parquet", _clean_twitch),
}


class DataCleaner:
    def process(self) -> dict[str, int]:
        """
        Neteja cada `historico_*.parquet` i escriu el resultat a staging.
        Retorna dict plataforma -> nombre de files escrites.
        """
        data_dir = _project_data_dir()
        staging = _staging_dir()
        counts: dict[str, int] = {}

        for platform, (src_name, dst_name, cleaner) in _CLEANERS.items():
            src = data_dir / src_name
            dst = staging / dst_name
            if not src.exists():
                print(f"[DataCleaner] Omet {platform}: no existeix {src.name}")
                counts[platform] = 0
                continue
            df = pd.read_parquet(src)
            cleaned = cleaner(df)
            cleaned.to_parquet(dst, index=False)
            counts[platform] = len(cleaned)
            print(f"[DataCleaner] {platform}: {len(cleaned)} files -> {dst.relative_to(_PROJECT_ROOT)}")

        return counts
