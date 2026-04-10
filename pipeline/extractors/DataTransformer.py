"""
Llegeix `data/staging/latest_*.parquet`, mapeja a un esquema canònic amb `extracted_at`,
afegeix la fila al magatzem `data/warehouse/content_observations.parquet` i genera JSON
per al dashboard a `web/src/data/dashboard/`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from paths import repo_root

_PROJECT_ROOT = repo_root()


def _data_dir() -> Path:
    return _PROJECT_ROOT / "data"


def _staging_dir() -> Path:
    return _data_dir() / "staging"


def _warehouse_path() -> Path:
    w = _data_dir() / "warehouse"
    w.mkdir(parents=True, exist_ok=True)
    return w / "content_observations.parquet"


def _dashboard_dir() -> Path:
    d = _PROJECT_ROOT / "web" / "src" / "data" / "dashboard"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _staging_files() -> list[tuple[str, Path]]:
    mapping = [
        ("instagram", _staging_dir() / "latest_instagram.parquet"),
        ("tiktok", _staging_dir() / "latest_tiktok.parquet"),
        ("youtube", _staging_dir() / "latest_youtube.parquet"),
        ("twitch", _staging_dir() / "latest_twitch.parquet"),
    ]
    return [(p, path) for p, path in mapping if path.exists()]


def _to_canonical(
    platform: str,
    df: pd.DataFrame,
    extracted_at: datetime,
    run_id: str,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "platform",
                "content_id",
                "creador",
                "nick",
                "seguidores",
                "fecha_publicacion",
                "tipo_publicacion",
                "likes",
                "comentarios",
                "visualizaciones",
                "url",
                "titulo",
                "extracted_at",
                "run_id",
            ]
        )

    rows: list[dict[str, Any]] = []

    if platform == "instagram":
        for _, r in df.iterrows():
            rows.append(
                {
                    "platform": platform,
                    "content_id": str(r["post_id"]),
                    "creador": r["creador"],
                    "nick": r["nick_instagram"],
                    "seguidores": r.get("seguidores"),
                    "fecha_publicacion": r.get("fecha_publicacion"),
                    "tipo_publicacion": r.get("tipo_publicacion"),
                    "likes": r.get("likes"),
                    "comentarios": r.get("comentarios"),
                    "visualizaciones": r.get("visualizaciones"),
                    "url": r.get("url_post"),
                    "titulo": None,
                    "extracted_at": extracted_at,
                    "run_id": run_id,
                }
            )
    elif platform == "tiktok":
        for _, r in df.iterrows():
            rows.append(
                {
                    "platform": platform,
                    "content_id": str(r["video_id"]),
                    "creador": r["creador"],
                    "nick": r["nick_tiktok"],
                    "seguidores": r.get("seguidores"),
                    "fecha_publicacion": r.get("fecha_publicacion"),
                    "tipo_publicacion": "VIDEO",
                    "likes": r.get("likes"),
                    "comentarios": r.get("comentarios"),
                    "visualizaciones": r.get("visualizaciones"),
                    "url": r.get("url_post"),
                    "titulo": None,
                    "extracted_at": extracted_at,
                    "run_id": run_id,
                }
            )
    elif platform == "youtube":
        for _, r in df.iterrows():
            rows.append(
                {
                    "platform": platform,
                    "content_id": str(r["id_video"]),
                    "creador": r["creador"],
                    "nick": r["nick_youtube"],
                    "seguidores": r.get("seguidores"),
                    "fecha_publicacion": r.get("fecha_publicacion"),
                    "tipo_publicacion": r.get("tipo_publicacion"),
                    "likes": r.get("likes"),
                    "comentarios": r.get("comentarios"),
                    "visualizaciones": r.get("visualizaciones"),
                    "url": r.get("url_video"),
                    "titulo": None,
                    "extracted_at": extracted_at,
                    "run_id": run_id,
                }
            )
    elif platform == "twitch":
        for _, r in df.iterrows():
            rows.append(
                {
                    "platform": platform,
                    "content_id": str(r["id_video"]),
                    "creador": r["creador"],
                    "nick": r["nick_twitch"],
                    "seguidores": r.get("seguidores"),
                    "fecha_publicacion": r.get("fecha_publicacion"),
                    "tipo_publicacion": r.get("tipo_publicacion"),
                    "likes": None,
                    "comentarios": None,
                    "visualizaciones": r.get("visualizaciones"),
                    "url": r.get("url_post"),
                    "titulo": r.get("titulo"),
                    "extracted_at": extracted_at,
                    "run_id": run_id,
                }
            )
    else:
        raise ValueError(f"Plataforma desconeguda: {platform}")

    return pd.DataFrame(rows)


def _append_warehouse(snapshot: pd.DataFrame) -> int:
    path = _warehouse_path()
    if snapshot.empty:
        return 0
    if path.exists():
        old = pd.read_parquet(path)
        combined = pd.concat([old, snapshot], ignore_index=True)
    else:
        combined = snapshot
    # Evita duplicats exactes (mateixa extracció repetida)
    key_cols = ["platform", "content_id", "extracted_at"]
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    combined.to_parquet(path, index=False)
    return len(snapshot)


def _numeric_sum(s: pd.Series) -> float:
    return float(pd.to_numeric(s, errors="coerce").fillna(0).sum())


def _numeric_max(s: pd.Series) -> float:
    n = pd.to_numeric(s, errors="coerce").dropna()
    return float(n.max()) if len(n) > 0 else 0.0


def _utc_month_start() -> pd.Timestamp:
    u = datetime.now(timezone.utc)
    return pd.Timestamp(u.year, u.month, 1)


def _utc_week_monday() -> pd.Timestamp:
    u = datetime.now(timezone.utc)
    t = pd.Timestamp(u.date())
    mon = t.normalize() - pd.Timedelta(days=int(t.weekday()))
    return pd.Timestamp(mon)


def _build_summary(snapshot: pd.DataFrame, extracted_at: datetime, run_id: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "generated_at": extracted_at.isoformat(),
        "run_id": run_id,
        "snapshot_rows": int(len(snapshot)),
        "platforms": {},
        "creators": [],
    }
    if snapshot.empty:
        return summary

    for plat in snapshot["platform"].dropna().unique():
        sub = snapshot[snapshot["platform"] == plat]
        seg_col = sub["seguidores"] if "seguidores" in sub.columns else pd.Series(dtype=float)
        summary["platforms"][str(plat)] = {
            "content_count": int(len(sub)),
            "total_visualizaciones": _numeric_sum(sub["visualizaciones"]),
            "total_likes": _numeric_sum(sub["likes"]),
            "total_comentarios": _numeric_sum(sub["comentarios"]),
            "max_seguidores": _numeric_max(seg_col) if len(seg_col) else 0.0,
        }

    if "seguidores" in snapshot.columns:
        by_creator = (
            snapshot.groupby(["creador", "platform"], dropna=False)
            .agg(
                content_count=("content_id", "count"),
                visualizaciones=("visualizaciones", lambda x: _numeric_sum(x)),
                likes=("likes", lambda x: _numeric_sum(x)),
                seguidores=("seguidores", lambda x: _numeric_max(x)),
            )
            .reset_index()
        )
    else:
        by_creator = (
            snapshot.groupby(["creador", "platform"], dropna=False)
            .agg(
                content_count=("content_id", "count"),
                visualizaciones=("visualizaciones", lambda x: _numeric_sum(x)),
                likes=("likes", lambda x: _numeric_sum(x)),
            )
            .reset_index()
        )
        by_creator["seguidores"] = 0.0

    for _, row in by_creator.iterrows():
        summary["creators"].append(
            {
                "creador": row["creador"],
                "platform": row["platform"],
                "content_count": int(row["content_count"]),
                "visualizaciones": float(row["visualizaciones"]),
                "likes": float(row["likes"]),
                "seguidores": float(row["seguidores"]),
            }
        )
    summary["creators"].sort(key=lambda x: (str(x["creador"]).lower(), x["platform"]))
    return summary


def _write_json(name: str, payload: dict[str, Any]) -> Path:
    path = _dashboard_dir() / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def _json_safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        if pd.isna(v):
            return None
        return v.isoformat()
    if isinstance(v, np.generic):
        if pd.isna(v):
            return None
        return v.item()
    if isinstance(v, float) and np.isnan(v):
        return None
    if pd.isna(v):
        return None
    return v


def _df_records_for_json(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        records.append({k: _json_safe_value(v) for k, v in row.items()})
    return records


def _build_history_payload(warehouse: pd.DataFrame) -> dict[str, Any]:
    """Agregats per run (run_id + extracted_at) per al dashboard d’evolució temporal."""
    if warehouse.empty:
        return {
            "run_count": 0,
            "runs": [],
            "chart_labels": [],
            "series_visualizaciones": {},
            "series_contents": {},
            "series_likes": {},
            "series_seguidores": {},
            "series_total_visualizaciones": [],
            "series_total_seguidores": [],
        }

    df = warehouse.copy()
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])

    run_keys = df[["run_id", "extracted_at"]].drop_duplicates()
    runs: list[dict[str, Any]] = []
    for _, key in run_keys.iterrows():
        rid = key["run_id"]
        ext = key["extracted_at"]
        g = df[(df["run_id"] == rid) & (df["extracted_at"] == ext)]
        platforms: dict[str, dict[str, float | int]] = {}
        for p, gp in g.groupby("platform"):
            seg_s = gp["seguidores"] if "seguidores" in gp.columns else pd.Series(dtype=float)
            platforms[str(p)] = {
                "contents": int(len(gp)),
                "visualizaciones": float(gp["visualizaciones"].fillna(0).sum()),
                "likes": float(gp["likes"].fillna(0).sum()),
                "comentarios": float(gp["comentarios"].fillna(0).sum()),
                "seguidores": _numeric_max(seg_s) if len(seg_s) else 0.0,
            }
        ext_out = ext.isoformat() if hasattr(ext, "isoformat") else str(ext)
        runs.append(
            {
                "run_id": str(rid),
                "extracted_at": ext_out,
                "total_contents": int(len(g)),
                "platforms": platforms,
            }
        )

    runs.sort(key=lambda r: r["extracted_at"])

    all_platforms = sorted(df["platform"].dropna().unique().tolist(), key=str)

    chart_labels: list[str] = []
    for r in runs:
        try:
            chart_labels.append(pd.Timestamp(r["extracted_at"]).strftime("%d/%m %H:%M"))
        except (ValueError, TypeError):
            chart_labels.append(str(r["run_id"])[-8:])

    series_visualizaciones: dict[str, list[int]] = {p: [] for p in all_platforms}
    series_contents: dict[str, list[int]] = {p: [] for p in all_platforms}
    series_likes: dict[str, list[int]] = {p: [] for p in all_platforms}
    series_seguidores: dict[str, list[int]] = {p: [] for p in all_platforms}
    for r in runs:
        for p in all_platforms:
            st = r["platforms"].get(p, {})
            series_visualizaciones[p].append(int(round(st.get("visualizaciones", 0))))
            series_contents[p].append(int(st.get("contents", 0)))
            series_likes[p].append(int(round(st.get("likes", 0))))
            series_seguidores[p].append(int(round(st.get("seguidores", 0))))

    series_total_visualizaciones: list[int] = []
    series_total_seguidores: list[int] = []
    for r in runs:
        total_v = sum(float(pl.get("visualizaciones", 0)) for pl in r["platforms"].values())
        series_total_visualizaciones.append(int(round(total_v)))
        total_s = sum(float(pl.get("seguidores", 0)) for pl in r["platforms"].values())
        series_total_seguidores.append(int(round(total_s)))

    return {
        "run_count": len(runs),
        "runs": runs,
        "chart_labels": chart_labels,
        "series_visualizaciones": series_visualizaciones,
        "series_contents": series_contents,
        "series_likes": series_likes,
        "series_seguidores": series_seguidores,
        "series_total_visualizaciones": series_total_visualizaciones,
        "series_total_seguidores": series_total_seguidores,
    }


def _empty_creator_timeseries() -> dict[str, Any]:
    return {
        "source": "fecha_publicacion",
        "dedupe": "latest_extracted_at_per_content",
        "default_granularity": "month",
        "granularities": {
            "month": {"creators": {}},
            "week": {"creators": {}},
        },
    }


def _dedupe_latest_observation(df: pd.DataFrame) -> pd.DataFrame:
    """Una fila per (creador, platform, content_id): la de extracted_at més recent."""
    out = df.copy()
    out["extracted_at"] = pd.to_datetime(out["extracted_at"], errors="coerce")
    out["pub"] = pd.to_datetime(out["fecha_publicacion"], errors="coerce")
    out = out.dropna(subset=["pub"])
    if out.empty:
        return out
    out = out.sort_values("extracted_at", na_position="last")
    out = out.drop_duplicates(subset=["platform", "content_id", "creador"], keep="last")
    return out


def _month_index(pub_min: pd.Timestamp, pub_max: pd.Timestamp) -> pd.DatetimeIndex:
    start = pd.Timestamp(pub_min).to_period("M").to_timestamp(how="start")
    end_data = pd.Timestamp(pub_max).to_period("M").to_timestamp(how="start")
    end_now = _utc_month_start()
    end = max(end_data, end_now)
    return pd.date_range(start, end, freq="MS")


def _week_index(pub_min: pd.Timestamp, pub_max: pd.Timestamp) -> pd.DatetimeIndex:
    pm = pd.Timestamp(pub_min).normalize()
    px = pd.Timestamp(pub_max).normalize()
    start = pm - pd.Timedelta(days=int(pm.weekday()))
    end_data = px - pd.Timedelta(days=int(px.weekday()))
    end_now = _utc_week_monday()
    end = max(end_data, end_now)
    return pd.date_range(start, end, freq="W-MON")


def _series_for_platform_bucket(
    ps: pd.DataFrame,
    idx: pd.DatetimeIndex,
    rule: str,
) -> dict[str, list[float]]:
    n = len(idx)
    if ps.empty or n == 0:
        z = [0.0] * n
        return {"visualizaciones": z, "likes": z, "comentarios": z, "pieces": z, "seguidores": z}

    t = ps.set_index("pub").sort_index()
    vz = pd.to_numeric(t["visualizaciones"], errors="coerce").fillna(0)
    lk = pd.to_numeric(t["likes"], errors="coerce").fillna(0)
    cm = pd.to_numeric(t["comentarios"], errors="coerce").fillna(0)
    sg = pd.to_numeric(t["seguidores"], errors="coerce") if "seguidores" in t.columns else pd.Series(0.0, index=t.index)
    frame = pd.DataFrame({"vz": vz, "lk": lk, "cm": cm}, index=t.index)
    g = frame.resample(rule).sum()
    pieces = t.resample(rule).size()
    g["pieces"] = pieces
    g_sg = sg.resample(rule).max()
    g["seguidores"] = g_sg
    g = g.reindex(idx, fill_value=0).fillna(0)
    return {
        "visualizaciones": [float(x) for x in g["vz"].tolist()],
        "likes": [float(x) for x in g["lk"].tolist()],
        "comentarios": [float(x) for x in g["cm"].tolist()],
        "pieces": [float(x) for x in g["pieces"].tolist()],
        "seguidores": [float(x) for x in g["seguidores"].tolist()],
    }


def _labels_for_buckets(idx: pd.DatetimeIndex, rule: str) -> tuple[list[str], list[str]]:
    labels: list[str] = []
    iso: list[str] = []
    for ts in idx:
        t = pd.Timestamp(ts)
        iso.append(t.isoformat())
        if rule == "MS":
            labels.append(t.strftime("%Y-%m"))
        else:
            wk = t.isocalendar()
            labels.append(f"{t.strftime('%Y-%m-%d')} (S{wk.week})")
    return labels, iso


def _build_creator_timeseries(warehouse: pd.DataFrame) -> dict[str, Any]:
    """Sèries per data de publicació (parquet), agregades per mes o setmana; mètriques de l’última extracció per peça."""
    base = _empty_creator_timeseries()
    if warehouse.empty:
        return base

    deduped = _dedupe_latest_observation(warehouse)
    if deduped.empty:
        return base

    for gran_key, rule in (("month", "MS"), ("week", "W-MON")):
        creators_blob: dict[str, Any] = {}
        for creador in sorted(deduped["creador"].dropna().unique(), key=lambda x: str(x).lower()):
            sub = deduped[deduped["creador"] == creador]
            pm = sub["pub"].min()
            px = sub["pub"].max()
            idx = _month_index(pm, px) if rule == "MS" else _week_index(pm, px)
            if len(idx) == 0:
                continue
            chart_labels, bucket_iso = _labels_for_buckets(idx, rule)
            platforms_out: dict[str, Any] = {}
            for platform in sorted(sub["platform"].dropna().unique(), key=str):
                cols = ["pub", "visualizaciones", "likes", "comentarios"]
                if "seguidores" in sub.columns:
                    cols.append("seguidores")
                ps = sub[sub["platform"] == platform][cols]
                platforms_out[str(platform)] = _series_for_platform_bucket(ps, idx, rule)
            creators_blob[str(creador)] = {
                "chart_labels": chart_labels,
                "bucket_starts_iso": bucket_iso,
                "platforms": platforms_out,
            }
        base["granularities"][gran_key] = {"creators": creators_blob}

    return base


def _write_creator_timeseries_json() -> Path:
    path = _warehouse_path()
    if not path.exists():
        payload = _empty_creator_timeseries()
    else:
        payload = _build_creator_timeseries(pd.read_parquet(path))
    out = _write_json("creator_timeseries.json", payload)
    n_c = len(payload["granularities"]["month"]["creators"])
    max_b = 0
    for v in payload["granularities"]["month"]["creators"].values():
        max_b = max(max_b, len(v.get("chart_labels", [])))
    print(
        f"[DataTransformer] Sèries per creador (data publicació) -> {out.relative_to(_PROJECT_ROOT)} "
        f"({n_c} creadors, fins a {max_b} buckets mensuals)"
    )
    return out


def _refresh_history_json() -> Path:
    path = _warehouse_path()
    if not path.exists():
        payload = _build_history_payload(pd.DataFrame())
    else:
        payload = _build_history_payload(pd.read_parquet(path))
    out = _write_json("history.json", payload)
    print(f"[DataTransformer] Històric -> {out.relative_to(_PROJECT_ROOT)} ({payload['run_count']} runs)")
    _write_creator_timeseries_json()
    return out


class DataTransformer:
    def process(self, extracted_at: datetime | None = None) -> None:
        extracted_at = extracted_at or datetime.now(timezone.utc)
        # Emmagatzemem en UTC sense forçar tipus datetime64 amb tz a parquet
        if extracted_at.tzinfo is not None:
            extracted_at = extracted_at.astimezone(timezone.utc).replace(tzinfo=None)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        parts: list[pd.DataFrame] = []
        for platform, path in _staging_files():
            df = pd.read_parquet(path)
            canon = _to_canonical(platform, df, extracted_at, run_id)
            parts.append(canon)
            print(f"[DataTransformer] {platform}: {len(canon)} files canòniques")

        if not parts:
            print(
                "[DataTransformer] No hi ha fitxers a data/staging/. "
                "Executa DataCleaner després de tenir historico_*.parquet."
            )
            _refresh_history_json()
            return

        snapshot = pd.concat(parts, ignore_index=True)
        n_new = _append_warehouse(snapshot)
        print(f"[DataTransformer] Magatzem: +{n_new} files -> {_warehouse_path().relative_to(_PROJECT_ROOT)}")

        summary = _build_summary(snapshot, extracted_at, run_id)
        p_summary = _write_json("summary.json", summary)
        print(f"[DataTransformer] Resum escrit a {p_summary.relative_to(_PROJECT_ROOT)}")

        # Dades per a gràfiques: últim snapshot només (aquest run)
        latest_path = _write_json(
            "latest_snapshot.json",
            {
                "generated_at": summary["generated_at"],
                "run_id": run_id,
                "rows": _df_records_for_json(snapshot),
            },
        )
        print(f"[DataTransformer] Snapshot JSON -> {latest_path.relative_to(_PROJECT_ROOT)}")

        _refresh_history_json()
