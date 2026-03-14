import pandas as pd
import requests
import time
from pathlib import Path
import json

with open(Path().cwd() / "tokens.json") as f:
    config = json.load(f)
    # --- CONFIGURACIÓN ---
    CLIENT_ID = config.get('SPOTIFY_CLIENT_ID')
    CLIENT_SECRET = config.get('SPOTIFY_CLIENT_SECRET')

path = Path().cwd() / "data"
archivo_entrada = path / "creadors_poblet_podcast.parquet"
archivo_salida = path / "historico_podcasts_spotify.parquet"

# --- FUNCIONES DE APOYO ---

def obtener_token_spotify(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = requests.post(url, headers=headers, data=data)
    return response.json().get('access_token')

def obtener_todos_los_episodios(token, show_id):
    """Extrae episodios limpiando la URL y validando que no haya nulos"""
    episodios_lista = []
    
    # 1. Limpieza profunda del ID
    # Quitamos 'show/', quitamos lo que haya tras el '?' y quitamos espacios
    id_limpio = str(show_id).split('/')[-1].split('?')[0].strip()
    
    url = f"https://api.spotify.com/v1/shows/{id_limpio}/episodes?limit=50"
    headers = {"Authorization": f"Bearer {token}"}
    
    while url:
        try:
            response = requests.get(url, headers=headers)
            
            # Si nos pasamos de velocidad (Rate Limit), Spotify avisa con un 429
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"   ! Rate limit alcanzado. Esperando {retry_after} segundos...")
                time.sleep(retry_after)
                continue # Reintenta la misma URL
                
            if response.status_code != 200:
                print(f"   ! Error API Spotify ({response.status_code}) en ID: {id_limpio}")
                break
                
            data = response.json()
            
            # 2. Validación de seguridad (El fix para tu error)
            items = data.get('items', [])
            for ep in items:
                # Verificamos que el episodio 'ep' existe y no es None
                if ep is not None and isinstance(ep, dict) and 'id' in ep:
                    episodios_lista.append({
                        "podcast_id": id_limpio,
                        "episodio_id": ep.get('id'),
                        "titulo": ep.get('name'),
                        "fecha_publicacion": ep.get('release_date'),
                        "duracion_ms": ep.get('duration_ms', 0),
                        "descripcion_corta": (ep.get('description', '')[:200]) if ep.get('description') else "",
                        "url_spotify": ep.get('external_urls', {}).get('spotify'),
                        "explicit": ep.get('explicit', False)
                    })
            
            url = data.get('next')
            time.sleep(0.1)
            
        except Exception as e:
            print(f"   ! Error inesperado en el episodio: {e}")
            break
            
    return episodios_lista

# --- PROCESO PRINCIPAL ---

token = obtener_token_spotify(CLIENT_ID, CLIENT_SECRET)
df_creadores = pd.read_parquet(archivo_entrada)

print(f"Iniciando extracción de podcasts para {len(df_creadores)} registros...")

historico_podcasts = []

for index, row in df_creadores.iterrows():
    # CAMBIA 'columna_podcast' por el nombre real de tu columna
    show_id_raw = row.get('podcast') 
    
    if pd.isna(show_id_raw) or show_id_raw == '':
        continue
        
    print(f"[{index+1}/{len(df_creadores)}] Extrayendo episodios de: {show_id_raw}...")
    
    episodios = obtener_todos_los_episodios(token, show_id_raw)
    historico_podcasts.extend(episodios)
    
    time.sleep(0.5)

# Guardar resultados
if historico_podcasts:
    df_final = pd.DataFrame(historico_podcasts)
    # Convertimos duración a minutos para que sea más legible
    df_final['duracion_min'] = (df_final['duracion_ms'] / 60000).round(2)
    
    df_final.to_parquet(archivo_salida)
    print(f"\n¡Completado! {len(df_final)} episodios guardados en {archivo_salida}")
else:
    print("\nNo se encontraron datos de episodios.")