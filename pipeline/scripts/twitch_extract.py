import pandas as pd
import requests
import time
from pathlib import Path
import json

with open(Path().cwd() / "tokens.json") as f:
    config = json.load(f)
    # --- CONFIGURACIÓN ---
    CLIENT_ID = config.get('CLIENT_ID')
    CLIENT_SECRET = config.get('CLIENT_SECRET')

path = Path().cwd() / "data"
archivo_entrada = path / "creadors_poblet_twitch.parquet" # Ajusta el nombre
archivo_salida = path / "historico_posts_twitch.parquet"

# --- FUNCIONES DE APOYO ---

def obtener_access_token(client_id, client_secret):
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params)
    return response.json().get('access_token')

def obtener_usuario_info(token, client_id, login_name):
    login_clean = str(login_name).strip().replace('@', '')
    url = f"https://api.twitch.tv/helix/users?login={login_clean}"
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {token}'
    }
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]
    except Exception as e:
        print(f"Error buscando usuario {login_name}: {e}")
    return None

def obtener_todos_los_videos_twitch(token, client_id, user_id, user_name):
    """Extrae ABSOLUTAMENTE TODOS los vídeos disponibles usando paginación"""
    videos_lista = []
    cursor = None
    
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {token}'
    }
    
    while True:
        # URL base pidiendo el máximo por página (100)
        url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first=100"
        
        # Si tenemos un cursor de la página anterior, lo añadimos
        if cursor:
            url += f"&after={cursor}"
            
        response = requests.get(url, headers=headers)
        
        # Control de errores de la API (por ejemplo, Rate Limit)
        if response.status_code != 200:
            print(f"   ! Error API Twitch ({response.status_code})")
            break
            
        data = response.json()
        
        if 'data' in data and len(data['data']) > 0:
            for v in data['data']:
                videos_lista.append({
                    "creador_twitch": user_name,
                    "id_video": v['id'],
                    "titulo": v['title'],
                    "fecha_publicacion": v['created_at'],
                    "visualizaciones": v['view_count'],
                    "duracion": v['duration'],
                    "tipo": v['type'] # 'archive' (directo pasado), 'upload' o 'highlight'
                })
            
            # Miramos si hay una siguiente página
            cursor = data.get('pagination', {}).get('cursor')
            
            # Si no hay cursor, es que ya no quedan más vídeos
            if not cursor:
                break
            
            # Pequeña pausa para ser amigables con la API
            time.sleep(0.1)
        else:
            break
            
    return videos_lista

# --- PROCESO PRINCIPAL ---

token = obtener_access_token(CLIENT_ID, CLIENT_SECRET)
if not token:
    print("Error crítico: No se pudo obtener el token de Twitch.")
    exit()

df_creadores = pd.read_parquet(archivo_entrada)
print(f"Cargado Parquet con {len(df_creadores)} creadores.")

historico_total = []

for index, row in df_creadores.iterrows():
    # AJUSTA AQUÍ EL NOMBRE DE TU COLUMNA
    usuario = row.get('twitch') 
    
    if pd.isna(usuario) or usuario == '':
        continue
    
    print(f"[{index+1}/{len(df_creadores)}] Extrayendo todo de: {usuario}...")
    
    info_user = obtener_usuario_info(token, CLIENT_ID, usuario)
    
    if info_user:
        user_id = info_user['id']
        videos_canal = obtener_todos_los_videos_twitch(token, CLIENT_ID, user_id, usuario)
        historico_total.extend(videos_canal)
        print(f"   -> Encontrados {len(videos_canal)} vídeos.")
    else:
        print(f"   ! No se pudo encontrar el ID para {usuario}")
    
    time.sleep(0.5)

# Guardar a Parquet
if historico_total:
    df_final = pd.DataFrame(historico_total)
    df_final.to_parquet(archivo_salida)
    print(f"\n¡Éxito! Archivo guardado con {len(df_final)} registros en: {archivo_salida}")
else:
    print("\nNo se ha podido extraer ninguna información.")