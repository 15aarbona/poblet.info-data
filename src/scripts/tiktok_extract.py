import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from pathlib import Path

# ==========================================
# CONFIGURACIÓN DE RUTAS
# ==========================================
PATH = Path().cwd() / "data"
archivo_entrada = PATH / "creadors_poblet_tiktok.parquet" 
archivo_salida = PATH / "historico_posts_tiktok.parquet"  

# Variables globales para que el espía sepa a quién estamos buscando en cada momento
VIDEOS_INTERCEPTADOS = []
CURRENT_NICK = ""
CURRENT_CREADOR = ""

# ==========================================
# EL ESPÍA DE RED (Se conecta una sola vez)
# ==========================================
async def interceptar_trafico(response):
    global VIDEOS_INTERCEPTADOS, CURRENT_NICK, CURRENT_CREADOR
    if response.request.resource_type in ["fetch", "xhr"]:
        try:
            data = await response.json()
            if 'itemList' in data:
                for video in data['itemList']:
                    autor_video = video.get('author', {}).get('uniqueId')
                    
                    if autor_video == CURRENT_NICK:
                        stats = video.get('stats', {})
                        VIDEOS_INTERCEPTADOS.append({
                            "creador": CURRENT_CREADOR, 
                            "fecha_publicacion": pd.to_datetime(video.get('createTime'), unit='s'),
                            "vistas": stats.get('playCount', 0),
                            "likes": stats.get('diggCount', 0),
                            "video_id": video.get('id') 
                        })
        except:
            pass

# ==========================================
# FUNCIÓN DE SCROLL Y EXTRACCIÓN
# ==========================================
async def extraer_perfil(pagina, creador_nombre, nick, scrolls=50, flag=True):
    global VIDEOS_INTERCEPTADOS, CURRENT_NICK, CURRENT_CREADOR
    
    VIDEOS_INTERCEPTADOS = [] 
    CURRENT_CREADOR = creador_nombre
    CURRENT_NICK = str(nick).strip('/').replace('@', '').split('?')[0]
    
    url = f"https://www.tiktok.com/@{CURRENT_NICK}"
    
    print(f"\n🌍 Entrando a @{CURRENT_NICK} ({creador_nombre})...")
    await pagina.goto(url)
    
    if flag:
        print("   ⏳ Esperando carga... (Si sale Captcha, resuélvelo)")
        await pagina.wait_for_timeout(8000) 
    
    print(f"   🖱️ Iniciando hasta {scrolls} scrolls...")
    altura_anterior = await pagina.evaluate("document.body.scrollHeight")
    intentos_sin_bajar = 0

    for i in range(scrolls):
        await pagina.evaluate("window.scrollBy(0, 1500)")
        await pagina.wait_for_timeout(1000)
        await pagina.evaluate("window.scrollBy(0, 1500)")
        await pagina.wait_for_timeout(3000) # Pausa para dejar que lleguen los datos
        
        altura_actual = await pagina.evaluate("document.body.scrollHeight")
        if altura_actual == altura_anterior:
            intentos_sin_bajar += 1
            if intentos_sin_bajar >= 4: 
                print(f"   🛑 Fondo del perfil alcanzado en el scroll {i+1}.")
                break 
        else:
            intentos_sin_bajar = 0 
            altura_anterior = altura_actual
            
    df = pd.DataFrame(VIDEOS_INTERCEPTADOS)
    if not df.empty:
        df = df.drop_duplicates(subset=['video_id'])
        df = df.drop(columns=['video_id'])
        print(f"   ✅ ¡Conseguidos {len(df)} vídeos ÚNICOS y LIMPIOS de {creador_nombre}!")
        
    return df

# ==========================================
# EJECUCIÓN PRINCIPAL (AUTOMATIZACIÓN MODO CONTINUO)
# ==========================================
async def main():
    PATH.mkdir(parents=True, exist_ok=True)

    try:
        df_entrada = pd.read_parquet(archivo_entrada)
        df_entrada = df_entrada.dropna(subset=['tiktok']).copy()
        print(f"\n🚀 Iniciando extracción masiva para {len(df_entrada)} creadores...\n")
    except Exception as e:
        print(f"❌ Error al leer el archivo {archivo_entrada}: {e}")
        return

    directorio_sesion = "./tiktok_session"
    
    async with async_playwright() as p:
        contexto = await p.chromium.launch_persistent_context(
            user_data_dir=directorio_sesion,
            headless=False,
            viewport={'width': 1920, 'height': 1080},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars"
            ]
        )
        
        pagina = contexto.pages[0]
        
        await pagina.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-ES', 'es', 'en'] });
            window.chrome = { runtime: {} };
        """)
        
        pagina.on("response", interceptar_trafico)
        flag = True
        for index, fila in df_entrada.iterrows():
            creador_nombre = fila['creador']
            nick_tiktok = fila['tiktok']
            
            df_resultados = await extraer_perfil(pagina, creador_nombre, nick_tiktok, scrolls=50, flag=flag)
            flag = False
            
            if not df_resultados.empty:
                if not archivo_salida.exists():
                    df_resultados.to_parquet(archivo_salida, index=False)
                else:
                    df_existente = pd.read_parquet(archivo_salida)
                    df_actualizado = pd.concat([df_existente, df_resultados], ignore_index=True)
                    df_actualizado.to_parquet(archivo_salida, index=False)
                print(f"   💾 Datos guardados a salvo en el archivo Parquet.")
            else:
                print(f"   ⚠️ No se obtuvieron datos para {creador_nombre}.")
                
            print("   ⏳ Esperando 5 segundos antes de cambiar de perfil...")
            await pagina.wait_for_timeout(5000)
            
        print("Cerrando navegador... Extracción finalizada.")
        await contexto.close()

    # Resumen final
    if archivo_salida.exists():
        df_final = pd.read_parquet(archivo_salida)
        print("\n" + "="*50)
        print(f"🎉 EXTRACCIÓN MASIVA COMPLETADA 🎉")
        print(f"📊 Total histórico de vídeos en disco: {len(df_final)}")
        print(f"💾 Guardado en: {archivo_salida}")

if __name__ == "__main__":
    asyncio.run(main())