import pandas as pd
import requests
import time
from pathlib import Path
import json
import re
import asyncio
from playwright.async_api import async_playwright

from paths import repo_root


class Extractor:
    def __init__(self, config_path: str | Path | None = None, ui_callback=None):
        self.ui_callback = ui_callback

        self.repo_root = repo_root()
        self.data_path = self.repo_root / "data"

        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")

        cfg = Path(config_path) if config_path else self.repo_root / "tokens.json"
        with open(cfg, "r", encoding="utf-8") as f:
            self.config = json.load(f)


    def _reportar_estado(self, red: str, creador: str, accion: str, total=None):
        if self.ui_callback:
            self.ui_callback(red, creador, accion, total)


    def _obtener_columna_red_social(self, red_social: str) -> pd.DataFrame:
        df = self.df_creadors[["creador", f"nick_{red_social}"]].copy()
        df = df.dropna(subset=[f"nick_{red_social}"])
        return df


    async def _bloquear_recursos_innecesarios(self, route):
        url = route.request.url.lower()
        tipo = route.request.resource_type
        trackers = ["googleads", "doubleclick", "analytics", "tiktok.com/log/", "facebook.com/tr/", "pixel", "adsystem"]
        if any(t in url for t in trackers):
            await route.abort()
            return
        if tipo in ["font", "stylesheet", "image", "media", "imageset"]:
            await route.abort()
            return
        await route.continue_()