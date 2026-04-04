import pandas as pd
from pathlib import Path
import json
import math

class DataTransformer:
    def __init__(self):
        # Rutes absolutes ancorades a l'arxiu actual
        self.src_path = Path(__file__).parent.resolve() 
        self.data_path = self.src_path.parent / "data"

    def exportar_web(self):
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

            # AGRUPACIÓN MENSUAL (Timeline)
            df_temp = df.copy()
            df_temp['mes'] = df_temp['fecha_publicacion'].dt.to_period('M')
            
            cols_numericas = df_temp.select_dtypes(include=['number']).columns.tolist()
            dict_agg = {}
            for col in cols_numericas:
                if col in ['post_id', 'video_id', 'id_video', 'duracion_segundos']: continue
                if col == 'seguidores':
                    dict_agg[col] = 'max' 
                else:
                    dict_agg[col] = 'sum' 

            if not dict_agg: continue

            agrupado_creador = df_temp.groupby(['creador', 'mes']).agg(dict_agg).reset_index()

            for creador, df_c in agrupado_creador.groupby('creador'):
                rango = pd.period_range(start=df_c['mes'].min(), end=df_c['mes'].max(), freq='M')
                df_c = df_c.set_index('mes').reindex(rango).fillna(0)
                df_c.index.name = 'fecha' 
                df_c = df_c.reset_index()
                df_c['fecha'] = df_c['fecha'].astype(str)
                
                datos_web["datos_temporales"][red][creador] = df_c.drop(columns=['creador'], errors='ignore').to_dict(orient='records')

            # AGRUPACIÓN "TODOS"
            df_todos = agrupado_creador.groupby('mes').sum(numeric_only=True)
            if not df_todos.empty:
                rango_todos = pd.period_range(start=df_todos.index.min(), end=df_todos.index.max(), freq='M')
                df_todos = df_todos.reindex(rango_todos).fillna(0)
                df_todos.index.name = 'fecha'
                df_todos = df_todos.reset_index()
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