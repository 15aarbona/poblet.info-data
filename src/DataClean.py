import pandas as pd
import re
from pathlib import Path
from exportar_web import exportar_a_json

class Cleaner:
    def __init__(self):
        self.data_path = Path().cwd() / "data"

    def _fusionar_y_agrupar(self, df_nuevo, red_social, columna_id, metricas_suma):
        if df_nuevo.empty:
            print(f"   ⚠️ No hi ha dades noves per a {red_social}. Ometent fusió.")
            return

        df_nuevo[columna_id] = df_nuevo[columna_id].astype(str)

        archivo_posts = self.data_path / f"raw_posts_{red_social}.parquet"
        
        if archivo_posts.exists():
            df_maestro = pd.read_parquet(archivo_posts)
            df_maestro[columna_id] = df_maestro[columna_id].astype(str) 
            
            df_final_posts = pd.concat([df_maestro, df_nuevo], ignore_index=True)
            df_final_posts = df_final_posts.drop_duplicates(subset=[columna_id], keep='last')
        else:
            df_final_posts = df_nuevo.copy()
            df_maestro = pd.DataFrame(columns=[columna_id])

        df_final_posts.to_parquet(archivo_posts, index=False)
        
        nuevos_reales = len(df_final_posts) - len(df_maestro)
        print(f"🧹 Netejant i fusionant dades de {red_social}...")
        if nuevos_reales <= 0:
            print(f"   ✔️ 0 posts nous. Mantenim l'històric actual.")
        else:
            print(f"   ✔️ {nuevos_reales} posts nous afegits. Total històric: {len(df_final_posts)}")

        df_clean = df_final_posts.copy()
        df_clean['fecha_publicacion'] = pd.to_datetime(df_clean['fecha_publicacion'], utc=True).dt.tz_localize(None)
        
        df_clean['semana'] = df_clean['fecha_publicacion'].dt.to_period('W-MON').dt.start_time
        
        agrupaciones = {
            'fecha_publicacion': 'max', 
        }
        for metrica in metricas_suma:
            agrupaciones[metrica] = 'sum'
            df_clean[metrica] = pd.to_numeric(df_clean[metrica], errors='coerce').fillna(0)

        df_clean = df_clean.groupby(['creador', 'semana']).agg(agrupaciones).reset_index()
        
        df_clean = df_clean.sort_values(by=['creador', 'semana'])

        archivo_clean = self.data_path / f"clean_{red_social}.parquet"
        df_clean.to_parquet(archivo_clean, index=False)
        print(f"   ✔️ Generat dataset net agrupat per setmanes: {len(df_clean)} registres.")

        print(f"   🌐 Actualitzant JSON del Dashboard amb les noves dades de {red_social}...")
        try:
            exportar_a_json()
            print("   ✅ Dashboard web actualitzat correctament.")
        except Exception as e:
            print(f"   ❌ Error a l'exportar les dades per a la web: {e}")

    def _limpiar_instagram(self):
        ruta_temp = self.data_path / "historico_instagram.parquet"
        if ruta_temp.exists():
            df = pd.read_parquet(ruta_temp)
            df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
            df['comentarios'] = pd.to_numeric(df['comentarios'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            print("   ✨ Instagram temporal extret i sanejat.")
            
            metricas_presentes = [m for m in ["likes", "comentarios"] if m in df.columns]
            self._fusionar_y_agrupar(df, "instagram", "post_id", metricas_suma=metricas_presentes)
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Arxiu temporal eliminat.")

    def _limpiar_tiktok(self):
        ruta_temp = self.data_path / "historico_tiktok.parquet"
        if ruta_temp.exists():
            df = pd.read_parquet(ruta_temp)
            df['vistas'] = pd.to_numeric(df['vistas'], errors='coerce').fillna(0).astype(int)
            df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            print("   ✨ TikTok temporal extret i sanejat.")
            
            metricas_presentes = [m for m in ["vistas", "likes"] if m in df.columns]
            self._fusionar_y_agrupar(df, "tiktok", "video_id", metricas_suma=metricas_presentes)
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Arxiu temporal eliminat.")

    def _limpiar_youtube(self):
        ruta_temp = self.data_path / "historico_youtube.parquet"
        if ruta_temp.exists():
            df = pd.read_parquet(ruta_temp)
            df['visualizaciones'] = pd.to_numeric(df['visualizaciones'], errors='coerce').fillna(0).astype(int)
            df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            print("   ✨ YouTube temporal extret i sanejat.")
            
            metricas_presentes = [m for m in ["visualizaciones", "likes"] if m in df.columns]
            self._fusionar_y_agrupar(df, "youtube", "id_video", metricas_suma=metricas_presentes)
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Arxiu temporal eliminat.")

    def _parsear_duracion_twitch(self, duracion_str):
        if not isinstance(duracion_str, str): return 0
        horas = re.search(r'(\d+)h', duracion_str)
        minutos = re.search(r'(\d+)m', duracion_str)
        segundos = re.search(r'(\d+)s', duracion_str)
        h = int(horas.group(1)) if horas else 0
        m = int(minutos.group(1)) if minutos else 0
        s = int(segundos.group(1)) if segundos else 0
        return h * 3600 + m * 60 + s

    def _limpiar_twitch(self):
        ruta_temp = self.data_path / "historico_twitch.parquet"
        if ruta_temp.exists():
            df = pd.read_parquet(ruta_temp)
            df['visualizaciones'] = pd.to_numeric(df['visualizaciones'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            
            if 'duracion' in df.columns:
                df['duracion'] = df['duracion'].astype(str).apply(self._parsear_duracion_twitch)
                df['duracion'] = pd.to_numeric(df['duracion'], errors='coerce').fillna(0).astype(int)
            
            print("   ✨ Twitch temporal extret i sanejat.")
            
            metricas_presentes = [m for m in ["visualizaciones", "duracion"] if m in df.columns]
            self._fusionar_y_agrupar(df, "twitch", "id_video", metricas_suma=metricas_presentes)
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Arxiu temporal eliminat.")

    def clean_all(self):
        print("\n🧼 Iniciant procés de neteja i fusió de Base de Dades...")
        self._limpiar_instagram()
        self._limpiar_tiktok()
        self._limpiar_youtube()
        self._limpiar_twitch()
        print("\n✅ Base de Dades mestra actualitzada correctament.")