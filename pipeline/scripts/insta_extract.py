import pandas as pd 
from pathlib import Path
import time
import instaloader

PATH = Path() / "data"

L = instaloader.Instaloader(
    download_pictures=False,
    download_videos=False,
    download_video_thumbnails=False,
    save_metadata=False,
    download_comments=False
)

MAX_POSTS = None

archivo_entrada = PATH / "creadors_poblet_instagram.parquet"
archivo_salida = PATH / "historico_posts_instaloader.parquet"

# ==========================================
# FUNCIONES
# ==========================================

def obtener_posts_usuario(creador, nick, max_posts=None):
    """Extrae las publicaciones usando Instaloader simulando ser un humano"""
    
    nick_limpio = str(nick).strip('/').split('?')[0]
    print(f"\nConsultando a @{nick_limpio} ({creador})...")
    
    posts_extraidos = []
    
    try:
        # 1. Obtenemos el perfil
        perfil = instaloader.Profile.from_username(L.context, nick_limpio)
        
        seguidores = perfil.followers
        posts_totales = perfil.mediacount
        print(f"   👤 Perfil OK: {seguidores} seguidores | {posts_totales} posts en total.")
        
        # 2. Iteramos sobre las publicaciones (Instaloader hace la paginación solo)
        for contador, post in enumerate(perfil.get_posts(), start=1):
            
            posts_extraidos.append({
                "creador": creador,
                "nick_instagram": nick_limpio,
                "seguidores_perfil": seguidores,
                "publicaciones_totales_perfil": posts_totales,
                "post_id": post.shortcode,
                "fecha_publicacion": post.date_utc,
                "tipo_publicacion": post.typename,
                "likes": post.likes,
                "comentarios": post.comments,
                "url_post": f"https://www.instagram.com/p/{post.shortcode}/"
            })
            
            # 3. Pausas de seguridad para no alertar al sistema anti-bots de Meta
            if contador % 25 == 0:
                print(f"   📖 Extraídos {contador} posts...")
                print("Pausando unos segundos...")
                time.sleep(1.5) 
                
            # 4. Control del límite máximo
            if max_posts is not None and contador >= max_posts:
                print(f"   🛑 Límite de {max_posts} alcanzado. Pasamos al siguiente.")
                break
                
        print(f"   ✅ TOTAL OK: {len(posts_extraidos)} publicaciones extraídas de @{nick_limpio}.")
        
    except instaloader.exceptions.ProfileNotExistsException:
        print(f"   ❌ Error: El perfil @{nick_limpio} no existe o está mal escrito.")
    except Exception as e:
        print(f"   ⚠️ Error inesperado extrayendo a @{nick_limpio}: {e}")
        print("   (Si el error es '429' o 'redirect to login', Instagram nos ha bloqueado temporalmente)")
    
    return posts_extraidos


# ==========================================
# EJECUCIÓN PRINCIPAL
# ==========================================

# 1. Leer archivo origen
try:
    df_entrada = pd.read_parquet(archivo_entrada)
    df_entrada = df_entrada.dropna(subset=['instagram']).copy()
except Exception as e:
    print(f"Error al leer el archivo {archivo_entrada}: {e}")
    exit()

# 2. Iniciar extracción
todos_los_datos = []
print(f"\n🕵️‍♂️ Iniciando extracción para {len(df_entrada)} creadores...\n")

for index, fila in df_entrada.iterrows():
    creador = fila['creador']
    nick = fila['instagram']
    
    posts_usuario = obtener_posts_usuario(creador, nick, max_posts=MAX_POSTS)
    
    if posts_usuario:
        todos_los_datos.extend(posts_usuario)
        
        df_temporal = pd.DataFrame(posts_usuario)
        if not archivo_salida.exists():
            df_temporal.to_parquet(archivo_salida, index=False)
        else:
            df_existente = pd.read_parquet(archivo_salida)
            # Unimos los datos antiguos con los nuevos
            df_actualizado = pd.concat([df_existente, df_temporal], ignore_index=True)
            df_actualizado.to_parquet(archivo_salida, index=False)
                    
    # Pausa más larga entre un creador y otro
    time.sleep(5) 

# 3. Construir y mostrar resultados finales
if todos_los_datos:
    # Leemos el archivo que se ha ido generando para confirmar que está todo
    df_final = pd.read_parquet(archivo_salida)
    
    print("\n" + "="*50)
    print(f"🎉 Extracción completada.")
    print(f"📊 Total de publicaciones guardadas en disco: {len(df_final)}")
    print(f"💾 Datos asegurados en: {archivo_salida}")
    
    print("\nMuestra de los datos:")
    print(df_final[['nick_instagram', 'tipo_publicacion', 'likes', 'fecha_publicacion']].head(10))
else:
    print("\nNo se obtuvo ninguna publicación.")