import pandas as pd
import requests
import time
from pathlib import Path
import json
import re
import asyncio
from playwright.async_api import async_playwright

class Extractor:
    def __init__(self, config_path="tokens.json", ui_callback=None):
        # UI callback para actualizar la interfaz de usuario
        self.ui_callback = ui_callback

        # Ruta de la carpeta de datos
        self.data_path = Path().cwd() / "data"

        # Cargar los datos de los creadores
        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")
        
        # Leer el archivo de configuración
        f = open(config_path, 'r')
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