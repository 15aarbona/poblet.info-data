import pandas as pd
import yt_dlp
import time
from pathlib import Path

path = Path().cwd() / "data"
archivo_entrada = path / "creadors_poblet_youtube.parquet"
archivo_salida = path / "historico_posts_youtube.parquet"

def obtener_todos_los_videos(handle):
    handle_str = str(handle).strip()
    if not handle_str.startswith('@'):
        handle_str = f"@{handle_str}"
        
    url = f"https://www.youtube.com/{handle_str}/videos"
    videos_extraidos = []

    # PASO 1: Sacar solo la lista de IDs (es instantáneo)
    ydl_opts_rapido = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True, # Modo ultrarrápido
    }
    
    print(f"  -> Buscando cuántos vídeos tiene {handle_str}...")
    try:
        with yt_dlp.YoutubeDL(ydl_opts_rapido) as ydl:
            info_canal = ydl.extract_info(url, download=False)
            if not info_canal or 'entries' not in info_canal:
                return []
            
            lista_videos = info_canal['entries']
            total_videos = len(lista_videos)
            print(f"  -> ¡Encontrados {total_videos} vídeos! Empezando extracción profunda...")
            
    except Exception as e:
        print(f"Error al obtener la lista de {handle_str}: {e}")
        return []

    # PASO 2: Entrar vídeo a vídeo para sacar los likes
    ydl_opts_profundo = {
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': True,
    }
    
    with yt_dlp.YoutubeDL(ydl_opts_profundo) as ydl:
        for i, video_breve in enumerate(lista_videos):
            url_video = video_breve.get('url')
            if not url_video:
                continue
                
            try:
                # Extraemos a fondo solo este vídeo
                video = ydl.extract_info(url_video, download=False)
                
                if video:
                    video_data = {
                        "creador_youtube": handle_str,
                        "id_video": video.get('id'),
                        "titulo": video.get('title'),
                        "url_video": video.get('webpage_url'),
                        "fecha_publicacion": video.get('upload_date'), 
                        "visualizaciones": video.get('view_count', 0) or 0,
                        "likes": video.get('like_count', 0) or 0,
                        "duracion_segundos": video.get('duration')
                    }
                    videos_extraidos.append(video_data)
                    
                    # Imprimimos el progreso cada 10 vídeos para no saturar la consola
                    if (i + 1) % 10 == 0 or (i + 1) == total_videos:
                        print(f"     ... procesados {i + 1}/{total_videos} vídeos")
                        
            except Exception as e:
                pass # Ignoramos si un vídeo concreto falla y seguimos
                
    return videos_extraidos

# --- PARTE PRINCIPAL ---

print("Cargando datos del Parquet de creadores...")
df_creadores = pd.read_parquet(archivo_entrada)

print(f"Encontrados {len(df_creadores)} creadores. Empezando la extracción...\n")

historico_videos = []

for index, row in df_creadores.iterrows():
    usuario_yt = row.get('youtube') 
    
    if pd.isna(usuario_yt) or usuario_yt == '':
        continue
        
    print(f"[{index + 1}/{len(df_creadores)}] Extrayendo vídeos de: {usuario_yt}...")
    
    videos = obtener_todos_los_videos(usuario_yt)
    historico_videos.extend(videos)
    
    time.sleep(5)

# Convertimos a DataFrame
if historico_videos:
    df_historico = pd.DataFrame(historico_videos)
    df_historico.to_parquet(archivo_salida)
    print(f"\n¡Proceso terminado! Se han guardado {len(df_historico)} vídeos en '{archivo_salida}'")
else:
    print("\nNo se ha extraído ningún vídeo. Revisa si hay algún problema con las conexiones.")