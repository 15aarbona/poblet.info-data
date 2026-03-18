import pandas as pd
import json
import numpy as np
from pathlib import Path

def exportar_a_json():
    data_path = Path().cwd() / "data"
    redes = ["instagram", "tiktok", "youtube", "twitch"]
    
    # Estructura limpia para la web
    datos_web = {
        "redes": redes,
        "creadores": [],
        "datos_temporales": {}, 
        "top_posts": {}
    }
    
    creadores_set = set()

    for red in redes:
        archivo = data_path / f"clean_{red}.parquet"
        if not archivo.exists():
            continue
            
        df = pd.read_parquet(archivo)
        if df.empty: continue

        # 1. ESCUDO DEDUPLICADOR FINAL (Por si acaso DataClean dejó pasar algo)
        posibles_ids = ['post_id', 'video_id', 'id_video']
        col_id = next((col for col in posibles_ids if col in df.columns), None)
        if col_id:
            df = df.drop_duplicates(subset=[col_id], keep='last')

        # 2. PREPARACIÓN DE FECHAS Y MÉTRICAS
        df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
        creadores_set.update(df['creador'].dropna().unique())
        
        excluir = ['creador', 'post_id', 'video_id', 'id_video', 'url_post', 'url_video', 'titulo', 'tipo_publicacion', 'tipo', 'fecha_publicacion']
        todas_metricas = [col for col in df.columns if col not in excluir and not col.startswith('nick_')]
        df[todas_metricas] = df[todas_metricas].apply(pd.to_numeric, errors='coerce').fillna(0)
        
        metricas_suma = [col for col in todas_metricas if col != 'seguidores']
        metricas_estado = ['seguidores'] if 'seguidores' in todas_metricas else []

        datos_web["datos_temporales"][red] = {}
        dfs_creadores = []

        # 3. CREAR UN CALENDARIO CONTINUO (Desde el primer post de la BD hasta hoy)
        fecha_inicio = df['fecha_publicacion'].min()
        fecha_fin = pd.Timestamp.now().normalize()
        # Usamos 'W-MON' (Semanas empezando en lunes) para no saturar la web. 
        # Si prefieres diario, cambia 'W-MON' por 'D'.
        calendario_global = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='W-MON')

        # 4. PROCESAR CADA CREADOR
        for creador in df['creador'].unique():
            df_creador = df[df['creador'] == creador].copy().set_index('fecha_publicacion')
            
            # Agrupamos por semana
            df_agrupado = df_creador.resample('W-MON').agg(
                **{col: pd.NamedAgg(column=col, aggfunc='sum') for col in metricas_suma},
                **{col: pd.NamedAgg(column=col, aggfunc='last') for col in metricas_estado}
            )
            
            # Alineamos al calendario global (rellena con NaN las semanas vacías)
            df_alineado = df_agrupado.reindex(calendario_global)
            
            # Rellenamos Seguidores: Arrastramos el último valor conocido (ffill)
            if metricas_estado:
                df_alineado[metricas_estado] = df_alineado[metricas_estado].ffill().fillna(0)
                
            # Rellenamos Likes/Views/Comentarios: Si es NaN, es 0
            df_alineado[metricas_suma] = df_alineado[metricas_suma].fillna(0)
            
            dfs_creadores.append(df_alineado.copy())
            
            # Formatear para el JSON
            df_json = df_alineado.reset_index().rename(columns={'index': 'fecha_publicacion'})
            df_json['fecha'] = df_json['fecha_publicacion'].dt.strftime('%Y-%m-%d')
            datos_web["datos_temporales"][red][creador] = df_json.drop(columns=['fecha_publicacion']).to_dict(orient='records')

        # 5. VISTA GENERAL (Todos)
        if dfs_creadores:
            # Sumamos las líneas de tiempo ya limpias de todos los creadores
            df_general = sum(dfs_creadores)
            df_general_json = df_general.reset_index().rename(columns={'index': 'fecha_publicacion'})
            df_general_json['fecha'] = df_general_json['fecha_publicacion'].dt.strftime('%Y-%m-%d')
            datos_web["datos_temporales"][red]["Todos"] = df_general_json.drop(columns=['fecha_publicacion']).to_dict(orient='records')

    datos_web["creadores"] = sorted(list(creadores_set))

    ruta_json = Path().cwd() / "src" / "web" / "dashboard_data.json"
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(datos_web, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Datos limpios y estructurados exportados a {ruta_json}")

if __name__ == "__main__":
    exportar_a_json()