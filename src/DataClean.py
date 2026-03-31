import pandas as pd
import re
from pathlib import Path
import json
import math

class Cleaner:
    def __init__(self):
        # Rutes absolutes ancorades a l'arxiu actual (Intactes, com vas demanar)
        self.src_path = Path(__file__).parent.resolve() 
        self.data_path = self.src_path.parent / "data"  

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

        self.data_path.mkdir(parents=True, exist_ok=True)
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
        
        agrupaciones = {'fecha_publicacion': 'max'}
        for metrica in metricas_suma:
            agrupaciones[metrica] = 'sum'
            df_clean[metrica] = pd.to_numeric(df_clean[metrica], errors='coerce').fillna(0)

        df_clean = df_clean.groupby(['creador', 'semana']).agg(agrupaciones).reset_index()
        df_clean = df_clean.sort_values(by=['creador', 'semana'])

        archivo_clean = self.data_path / f"clean_{red_social}.parquet"
        df_clean.to_parquet(archivo_clean, index=False)
        print(f"   ✔️ Generat dataset net agrupat per setmanes: {len(df_clean)} registres.")

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
            if 'comentarios' in df.columns: df['comentarios'] = pd.to_numeric(df['comentarios'], errors='coerce').fillna(0).astype(int)
            if 'compartidos' in df.columns: df['compartidos'] = pd.to_numeric(df['compartidos'], errors='coerce').fillna(0).astype(int)
            if 'seguidores' in df.columns:
                df['seguidores'] = pd.to_numeric(df['seguidores'], errors='coerce').fillna(0).astype(int)
            print("   ✨ TikTok temporal extret i sanejat.")
            
            metricas_presentes = [m for m in ["vistas", "likes", "comentarios", "compartidos"] if m in df.columns]
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

    def _exportar_web(self):
        print("\n🌐 Transformant i preparant dades per al Dashboard Web...")
        ruta_json = self.src_path / "web" / "dashboard_data.json"
        ruta_json.parent.mkdir(parents=True, exist_ok=True)
        
        redes = ["instagram", "tiktok", "youtube", "twitch"]
        datos_web = {
            "redes": redes,
            "creadores": [],
            "datos_temporales": {r: {} for r in redes}, 
            "top_posts": {r: {} for r in redes}
        }
        
        creadores_set = set()

        for red in redes:
            archivo = self.data_path / f"raw_posts_{red}.parquet"
            if not archivo.exists():
                continue
                
            df = pd.read_parquet(archivo)
            if df.empty: continue

            df = df.dropna(subset=['fecha_publicacion']).copy()
            if df.empty: continue

            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
            creadores_set.update(df['creador'].dropna().unique())

            # TOP 10 POSTS
            col_sort = None
            if 'visualizaciones' in df.columns: col_sort = 'visualizaciones'
            elif 'vistas' in df.columns: col_sort = 'vistas'
            elif 'likes' in df.columns: col_sort = 'likes'

            if col_sort:
                top_10 = df.sort_values(by=col_sort, ascending=False).head(10).fillna(0)
                top_10['fecha_publicacion'] = top_10['fecha_publicacion'].dt.strftime('%Y-%m-%d')
                datos_web["top_posts"][red] = top_10.to_dict(orient='records')
            else:
                datos_web["top_posts"][red] = []

            # AGRUPACIÓN SEMANAL (Timeline)
            df_temp = df.copy()
            # Fijamos la fecha al Lunes de cada semana
            df_temp['semana'] = df_temp['fecha_publicacion'].dt.to_period('W-MON').dt.start_time
            
            cols_numericas = df_temp.select_dtypes(include=['number']).columns.tolist()
            dict_agg = {}
            for col in cols_numericas:
                if col in ['post_id', 'video_id', 'id_video', 'duracion_segundos']: continue
                if col == 'seguidores':
                    dict_agg[col] = 'max' 
                else:
                    dict_agg[col] = 'sum' 

            if not dict_agg: continue

            agrupado_creador = df_temp.groupby(['creador', 'semana']).agg(dict_agg).reset_index()

            for creador, df_c in agrupado_creador.groupby('creador'):
                # 1. Renombramos la columna
                df_c = df_c.rename(columns={'semana': 'fecha'})
                
                # 2. ORDENAMOS CRONOLÓGICAMENTE
                df_c = df_c.sort_values('fecha', ascending=True)
                
                # 3. Lo pasamos a texto (quedará formato "YYYY-MM-DD")
                df_c['fecha'] = df_c['fecha'].astype(str)
                
                datos_web["datos_temporales"][red][creador] = df_c.drop(columns=['creador'], errors='ignore').to_dict(orient='records')

            # AGRUPACIÓN "TODOS"
            # 1. Separar las métricas normales (que sí se suman por semana) de los seguidores
            cols_sumar = [c for c in dict_agg.keys() if c != 'seguidores']
            
            if cols_sumar:
                df_todos = agrupado_creador.groupby('semana')[cols_sumar].sum().reset_index()
            else:
                df_todos = pd.DataFrame({'semana': agrupado_creador['semana'].unique()})

            # 2. Arreglo definitivo para seguidores
            if 'seguidores' in agrupado_creador.columns:
                # Pivotamos: filas=semanas, columnas=creadores, valores=seguidores
                df_seg = agrupado_creador.pivot(index='semana', columns='creador', values='seguidores')
                
                # ffill() arrastra el último valor conocido de cada creador hacia el futuro.
                # fillna(0) pone a 0 los creadores antes de su primer vídeo.
                df_seg = df_seg.ffill().fillna(0)
                
                # Sumamos la fila entera (todos los creadores activos en esa semana)
                serie_seg_total = df_seg.sum(axis=1).reset_index(name='seguidores')
                
                # Unimos este cálculo al dataframe total
                df_todos = pd.merge(df_todos, serie_seg_total, on='semana', how='outer')

            if not df_todos.empty:
                df_todos = df_todos.rename(columns={'semana': 'fecha'})
                
                # 1. Ordenamos cronológicamente
                df_todos = df_todos.sort_values('fecha', ascending=True)
                
                # 2. Pasamos a texto
                df_todos['fecha'] = df_todos['fecha'].astype(str)
                
                datos_web["datos_temporales"][red]["Todos"] = df_todos.to_dict(orient='records')
        datos_web["creadores"] = sorted(list(creadores_set))

        # ESCUDO ANTI-ERRORES JSON DEFINITIVO
        def limpiar_dict(d):
            if isinstance(d, dict): return {k: limpiar_dict(v) for k, v in d.items()}
            elif isinstance(d, list): return [limpiar_dict(v) for v in d]
            elif isinstance(d, float): return 0 if math.isnan(d) or math.isinf(d) else d
            elif hasattr(d, 'item') and callable(getattr(d, 'item')): return d.item() 
            elif pd.isna(d): return None 
            return d

        datos_web = limpiar_dict(datos_web)

        try:
            with open(ruta_json, 'w', encoding='utf-8') as f:
                json.dump(datos_web, f, ensure_ascii=False, indent=4)
            print(f"   ✅ Arxiu dashboard_data.json generat i llest a: {ruta_json}")
        except Exception as e:
            print(f"   ❌ ERROR FATAL guardant el JSON: {e}")

    def clean_all(self):
        print("\n🧼 Iniciant procés de neteja i fusió de Base de Dades...")
        self._limpiar_instagram()
        self._limpiar_tiktok()
        self._limpiar_youtube()
        self._limpiar_twitch()
        print("\n✅ Base de Dades mestra actualitzada correctament.")
        
        print("\n🌐 Exportant dades netes per al Dashboard Web...")
        self._exportar_web()
        print("✅ Procés de neteja i exportació completat.")