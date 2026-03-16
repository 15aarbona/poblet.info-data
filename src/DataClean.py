import pandas as pd
import re
from pathlib import Path

class Cleaner:
    def __init__(self):
        # Rutas de datos
        self.data_path = Path().cwd() / "data"
        
        # Cargamos los datos crudos generados por el Extractor
        print("📥 Cargando datos crudos para limpieza...")
        self.df_instagram = pd.read_parquet(self.data_path / "historico_instagram.parquet")
        self.df_tiktok = pd.read_parquet(self.data_path / "historico_tiktok.parquet")
        self.df_youtube = pd.read_parquet(self.data_path / "historico_youtube.parquet")
        self.df_twitch = pd.read_parquet(self.data_path / "historico_twitch.parquet")
        #self.df_podcast = pd.read_parquet(self.data_path / "historico_podcast.parquet")

    # ----------------------------------------------
    # LIMPIEZA DE INSTAGRAM
    # ----------------------------------------------
    def _limpiar_instagram(self):
        df = self.df_instagram.copy()
        df = df[["creador", "nick_instagram", "seguidores", "fecha_publicacion", "likes", "comentarios"]]
        if not df.empty:
            # Convertir fecha a datetime
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            
            # Rellenar nulos con 0 y convertir a entero
            for col in ['likes', 'comentarios', 'seguidores']:
                df[col] = df[col].fillna(0).astype(int)
                
            self.df_instagram = df
        
            print("   ✨ Instagram limpio.")

    # ----------------------------------------------
    # LIMPIEZA DE TIKTOK
    # ----------------------------------------------
    def _limpiar_tiktok(self):
        df = self.df_tiktok.copy()
        if not df.empty:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            
            for col in ['vistas', 'likes']:
                df[col] = df[col].fillna(0).astype(int)
                
            self.df_tiktok = df
            print("   ✨ TikTok limpio.")

    # ----------------------------------------------
    # LIMPIEZA DE YOUTUBE
    # ----------------------------------------------
    def _limpiar_youtube(self):
        df = self.df_youtube.copy()
        df = df[["creador", "nick_youtube", "seguidores", "fecha_publicacion", "visualizaciones", "likes", "duracion_segundos"]]
        
        if not df.empty:
            # YouTube suele venir como string '20231225'
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], format='%Y%m%d', errors='coerce')
            
            for col in ['visualizaciones', 'likes', 'duracion_segundos']:
                df[col] = df[col].fillna(0).astype(int)
                
            self.df_youtube = df
            print("   ✨ YouTube limpio.")

    # ----------------------------------------------
    # LIMPIEZA DE TWITCH
    # ----------------------------------------------
    def _parsear_duracion_twitch(self, duracion_str):
        """Convierte formatos como '1h30m15s' o '45m' a segundos totales"""
        if pd.isna(duracion_str): return 0
        duracion_str = str(duracion_str)
        
        h = re.search(r'(\d+)h', duracion_str)
        m = re.search(r'(\d+)m', duracion_str)
        s = re.search(r'(\d+)s', duracion_str)
        
        horas = int(h.group(1)) if h else 0
        minutos = int(m.group(1)) if m else 0
        segundos = int(s.group(1)) if s else 0
        
        return horas * 3600 + minutos * 60 + segundos

    def _limpiar_twitch(self):
        df = self.df_twitch.copy()
        df = df[["creador", "nick_twitch", "seguidores", "fecha_publicacion", "visualizaciones", "duracion"]]

        if not df.empty:
            # Fechas en formato ISO
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            
            df['visualizaciones'] = df['visualizaciones'].fillna(0).astype(int)
            
            # Aplicamos nuestra función para pasar el texto raro a segundos
            df['duracion_segundos'] = df['duracion'].apply(self._parsear_duracion_twitch)
            df = df.drop(columns=['duracion']) # Borramos la columna sucia original
            
            self.df_twitch = df
            print("   ✨ Twitch limpio.")

    # # ----------------------------------------------
    # # LIMPIEZA DE PODCASTS (SPOTIFY)
    # # ----------------------------------------------
    # def _limpiar_podcast(self):
    #     df = self.df_podcast.copy()
    #     if not df.empty:
    #         # A veces Spotify solo da el año ('2019'), esto lo parsea bien rellenando el mes/día
    #         df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            
    #         # Convertimos milisegundos a minutos redondeados a 2 decimales
    #         df['duracion_minutos'] = (df['duracion_ms'].fillna(0) / 60000).round(2)
    #         df = df.drop(columns=['duracion_ms'])
            
    #         self.df_podcast = df
    #         print("   ✨ Podcasts limpios.")


    # ----------------------------------------------
    # EJECUCIÓN GENERAL
    # ----------------------------------------------
    def clean_all(self):
        print("\n🧼 Iniciando proceso de limpieza de datos...")
        self._limpiar_instagram()
        self._limpiar_tiktok()
        self._limpiar_youtube()
        self._limpiar_twitch()
        #self._limpiar_podcast()
        
        print("\n✅ Limpieza completada. Guardando archivos limpios...")
        
        # Guardamos en nuevos archivos para no pisar los "crudos" en caso de error
        self.df_instagram.to_parquet(self.data_path / "clean_instagram.parquet", index=False)
        self.df_tiktok.to_parquet(self.data_path / "clean_tiktok.parquet", index=False)
        self.df_youtube.to_parquet(self.data_path / "clean_youtube.parquet", index=False)
        self.df_twitch.to_parquet(self.data_path / "clean_twitch.parquet", index=False)
        #self.df_podcast.to_parquet(self.data_path / "clean_podcast.parquet", index=False)
        
        print("✅ Archivos limpios guardados correctamente con el prefijo 'clean_'.")