import pandas as pd
import re
from pathlib import Path

class Cleaner:
    def __init__(self):
        self.data_path = Path().cwd() / "data"

    def _fusionar_y_agrupar(self, df_nuevo, red_social, columna_id, metricas_suma):
        """
        1. Guarda los posts individuales en 'raw_posts' para matar duplicados reales.
        2. Agrupa por semanas y guarda en 'clean' listo para la web.
        """
        if df_nuevo.empty:
            print(f"   ⚠️ No hay datos nuevos para {red_social}. Saltando fusión.")
            return

        # 🛡️ ESCUDO ANTI-ERRORES PARQUET: Forzamos que el ID sea SIEMPRE texto
        df_nuevo[columna_id] = df_nuevo[columna_id].astype(str)

        # --- 1. MANTENER BASE DE DATOS DE POSTS ÚNICOS (Escudo anti-duplicados) ---
        archivo_posts = self.data_path / f"raw_posts_{red_social}.parquet"
        
        if archivo_posts.exists():
            df_maestro = pd.read_parquet(archivo_posts)
            df_maestro[columna_id] = df_maestro[columna_id].astype(str) 
            
            df_final_posts = pd.concat([df_maestro, df_nuevo], ignore_index=True)
            df_final_posts = df_final_posts.drop_duplicates(subset=[columna_id], keep='last')
        else:
            df_final_posts = df_nuevo

        # 🛡️ CURA HISTÓRICA: Aseguramos que todas las métricas a sumar sean enteros puros
        # Esto soluciona el error "Expected bytes, got int" si tenías datos viejos guardados como texto
        for metrica in metricas_suma:
            if metrica in df_final_posts.columns:
                df_final_posts[metrica] = pd.to_numeric(df_final_posts[metrica], errors='coerce').fillna(0).astype(int)

        # Guardamos el histórico en bruto pero 100% sin duplicados y saneado
        df_final_posts.to_parquet(archivo_posts, index=False)

        # --- 2. GENERAR AGRUPACIÓN SEMANAL PARA LA WEB ---
        df_final_posts['fecha_publicacion'] = pd.to_datetime(df_final_posts['fecha_publicacion'], utc=True).dt.tz_localize(None)
        
        lista_semanal = []
        fecha_fin_global = pd.Timestamp.now().normalize()

        for creador in df_final_posts['creador'].unique():
            df_creador = df_final_posts[df_final_posts['creador'] == creador].copy().set_index('fecha_publicacion')
            
            agg_dict = {metrica: 'sum' for metrica in metricas_suma}
            if 'seguidores' in df_final_posts.columns:
                agg_dict['seguidores'] = 'last' 
            
            df_semanal = df_creador.resample('W-MON').agg(agg_dict)
            
            if not df_semanal.empty:
                fecha_inicio = df_semanal.index.min()
                idx = pd.date_range(start=fecha_inicio, end=fecha_fin_global, freq='W-MON')
                df_semanal = df_semanal.reindex(idx)
                
                if 'seguidores' in df_semanal.columns:
                    df_semanal['seguidores'] = df_semanal['seguidores'].ffill().fillna(0)
                
                for metrica in metricas_suma:
                    df_semanal[metrica] = df_semanal[metrica].fillna(0)
                    
                df_semanal = df_semanal.reset_index().rename(columns={'index': 'fecha_publicacion'})
                df_semanal.insert(0, 'creador', creador)
                lista_semanal.append(df_semanal)
                
        if lista_semanal:
            df_clean = pd.concat(lista_semanal, ignore_index=True)
            archivo_clean = self.data_path / f"clean_{red_social}.parquet"
            df_clean.to_parquet(archivo_clean, index=False)
            print(f"   🔄 Fusión completada: 'clean_{red_social}' agrupado por semanas con éxito.")

    # ----------------------------------------------
    # LIMPIEZA DE INSTAGRAM
    # ----------------------------------------------
    def _limpiar_instagram(self):
        ruta_temp = self.data_path / "historico_instagram.parquet"
        if not ruta_temp.exists(): return
        
        df = pd.read_parquet(ruta_temp)
        # Escudo robusto de columnas
        columnas_deseadas = ["creador", "post_id", "seguidores", "fecha_publicacion", "likes", "comentarios"]
        df = df[[col for col in columnas_deseadas if col in df.columns]]
        
        if not df.empty and "post_id" in df.columns:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            for col in ['likes', 'comentarios', 'seguidores']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            print("   ✨ Instagram temporal extraído.")
            self._fusionar_y_agrupar(df, "instagram", "post_id", metricas_suma=["likes", "comentarios"])
        
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Archivo temporal eliminado.")

    # ----------------------------------------------
    # LIMPIEZA DE TIKTOK
    # ----------------------------------------------
    def _limpiar_tiktok(self):
        ruta_temp = self.data_path / "historico_tiktok.parquet"
        if not ruta_temp.exists(): return
        
        df = pd.read_parquet(ruta_temp)
        columnas_deseadas = ["creador", "video_id", "seguidores", "fecha_publicacion", "vistas", "likes"]
        df = df[[col for col in columnas_deseadas if col in df.columns]]
        
        if not df.empty and "video_id" in df.columns:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            for col in ['vistas', 'likes', 'seguidores']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
            print("   ✨ TikTok temporal extraído.")
            self._fusionar_y_agrupar(df, "tiktok", "video_id", metricas_suma=["vistas", "likes"])
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Archivo temporal eliminado.")

    # ----------------------------------------------
    # LIMPIEZA DE YOUTUBE
    # ----------------------------------------------
    def _limpiar_youtube(self):
        ruta_temp = self.data_path / "historico_youtube.parquet"
        if not ruta_temp.exists(): return
        
        df = pd.read_parquet(ruta_temp)
        columnas_deseadas = ["creador", "id_video", "seguidores", "fecha_publicacion", "visualizaciones", "likes", "duracion_segundos"]
        df = df[[col for col in columnas_deseadas if col in df.columns]]
        
        if not df.empty and "id_video" in df.columns:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], format='%Y%m%d', errors='coerce')
            for col in ['visualizaciones', 'likes', 'duracion_segundos', 'seguidores']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
            print("   ✨ YouTube temporal extraído.")
            self._fusionar_y_agrupar(df, "youtube", "id_video", metricas_suma=["visualizaciones", "likes", "duracion_segundos"])
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Archivo temporal eliminado.")

    # ----------------------------------------------
    # LIMPIEZA DE TWITCH
    # ----------------------------------------------
    def _parsear_duracion_twitch(self, duracion_str):
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
        ruta_temp = self.data_path / "historico_twitch.parquet"
        if not ruta_temp.exists(): return
        
        df = pd.read_parquet(ruta_temp)
        
        # Adaptación robusta de nombres por si el Extractor trae variaciones
        if "id" in df.columns and "id_video" not in df.columns:
            df = df.rename(columns={"id": "id_video"})
        elif "id_video" not in df.columns:
            df["id_video"] = df["creador"] + "_" + df["fecha_publicacion"].astype(str)

        columnas_deseadas = ["creador", "id_video", "seguidores", "fecha_publicacion", "visualizaciones", "duracion"]
        df = df[[col for col in columnas_deseadas if col in df.columns]]
        
        if not df.empty:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
            
            if 'visualizaciones' in df.columns:
                df['visualizaciones'] = pd.to_numeric(df['visualizaciones'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            
            # EL PARCHE PARA LA DURACIÓN: Lo tratamos explícitamente y forzamos a entero.
            if 'duracion' in df.columns:
                # 1. Todo a string por si acaso, para que el regex lo entienda.
                df['duracion'] = df['duracion'].astype(str).apply(self._parsear_duracion_twitch)
                # 2. Forzamos conversión final a entero.
                df['duracion'] = pd.to_numeric(df['duracion'], errors='coerce').fillna(0).astype(int)
            
            print("   ✨ Twitch temporal extraído y saneado.")
            
            metricas_presentes = [m for m in ["visualizaciones", "duracion"] if m in df.columns]
            self._fusionar_y_agrupar(df, "twitch", "id_video", metricas_suma=metricas_presentes)
            
        ruta_temp.unlink(missing_ok=True)
        print("   🗑️ Archivo temporal eliminado.")

    # ----------------------------------------------
    # EJECUCIÓN GENERAL
    # ----------------------------------------------
    def clean_all(self):
        print("\n🧼 Iniciando proceso de limpieza y fusión de Base de Datos...")
        self._limpiar_instagram()
        self._limpiar_tiktok()
        self._limpiar_youtube()
        self._limpiar_twitch()
        
        print("\n✅ Base de Datos maestra actualizada correctamente.")