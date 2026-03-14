import pandas as pd
import requests
import time
from pathlib import Path
import json

import instaloader

import asyncio
from playwright.async_api import async_playwright

import yt_dlp

class Extractor:
    def __init__(self):

        def obtener_token_twitch(self, client_id: str, client_secret: str) -> str:
            url = "https://id.twitch.tv/oauth2/token"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'
            }
            response = requests.post(url, params=params)
            return response.json().get('access_token')
        
            
        def obtener_token_spotify(self, client_id: str, client_secret: str) -> str:
            url = "https://accounts.spotify.com/api/token"
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            response = requests.post(url, headers=headers, data=data)
            return response.json().get('access_token')
        
        # RUTA DE DATOS
        self.data_path = Path().cwd() / "data"

        # OBTENEMOS TOKENS DE CONFIGURACIÓN
        with open(Path().cwd() / "tokens.json") as f:
            config = json.load(f)

            TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
            TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
            self.twitch_token = obtener_token_twitch(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)

            SPOTIFY_CLIENT_ID = config.get('SPOTIFY_CLIENT_ID')
            SPOTIFY_CLIENT_SECRET = config.get('SPOTIFY_CLIENT_SECRET')
            self.spotify_token = obtener_token_spotify(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

        # DATAFRAMES DE CREADORES POR RED SOCIAL
        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")
        self.df_instagram = self.obtener_columna_red_social("instagram")
        self.df_tiktok = self.obtener_columna_red_social("tiktok")
        self.df_youtube = self.obtener_columna_red_social("youtube")
        self.df_twitch = self.obtener_columna_red_social("twitch")
        self.df_podcast = self.obtener_columna_red_social("podcast")

        # UTILIDADES PARA INSTAGRAM
        self.L = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            save_metadata=False,
            download_comments=False
        )

        # UTILIDADES PARA TIKTOK
        self.videos_interceptados = []
        self.current_nick = ""
        self.current_creador = ""
    

    def _obtener_columna_red_social(self, red_social: str) -> pd.DataFrame:
        return self.df_creadors[[self.df_creadors["creador"].notnull() & self.df_creadors[red_social].notnull()]]
    
    # ----------------------------------------------
    # EXTRACCIÓN DE INSTAGRAM
    # ----------------------------------------------
    def _obtener_posts_usuario(self, creador: str, nick: str, max_posts=None) -> list:        
        nick_limpio = str(nick).strip('/').split('?')[0]
        print(f"\nConsultando a @{nick_limpio} ({creador})...")
        
        posts_extraidos = []
        
        try:
            # 1. Obtenemos el perfil
            perfil = instaloader.Profile.from_username(self.L.context, nick_limpio)
            
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
    
    def _extraccion_instagram(self):
        todos_los_datos = []
        print(f"\n🕵️‍♂️ Iniciando extracción para {len(self.df_instagram)} creadores...\n")

        for index, fila in self.df_instagram.iterrows():
            creador = fila.get('creador')
            nick = fila.get('instagram')
            
            # Saltamos si el nick es nulo o está vacío
            if pd.isna(nick) or nick == '':
                continue
            
            posts_usuario = self.obtener_posts_usuario(creador, nick)
            
            if posts_usuario:
                # Añadimos los posts a nuestra lista maestra en memoria
                todos_los_datos.extend(posts_usuario)
                print(f"   -> {len(posts_usuario)} posts extraídos de {nick}.")
                            
            # Pausa larga entre un creador y otro para no saturar Instagram
            time.sleep(5) 
            
        # --- AL TERMINAR EL BUCLE ---
        # Convertimos toda la lista a DataFrame y lo guardamos en el atributo de la clase
        if todos_los_datos:
            self.df_instagram = pd.DataFrame(todos_los_datos)
            print(f"\n✅ Extracción de Instagram completada. {len(self.df_instagram)} posts guardados en self.df_instagram.")
        else:
            print("\n⚠️ No se extrajo ningún dato de Instagram.")
    

    # ----------------------------------------------
    # EXTRACCIÓN DE TIKTOK
    # ----------------------------------------------

    async def _interceptar_trafico(self, response):
        if response.request.resource_type in ["fetch", "xhr"]:
            try:
                data = await response.json()
                if 'itemList' in data:
                    for video in data['itemList']:
                        autor_video = video.get('author', {}).get('uniqueId')
                        
                        # Comparamos con el atributo de la clase
                        if autor_video == self.current_nick:
                            stats = video.get('stats', {})
                            self.videos_interceptados.append({
                                "creador": self.current_creador, 
                                "fecha_publicacion": pd.to_datetime(video.get('createTime'), unit='s'),
                                "vistas": stats.get('playCount', 0),
                                "likes": stats.get('diggCount', 0),
                                "video_id": video.get('id') 
                            })
            except:
                pass
    

    async def _extraer_perfil(self, pagina, creador_nombre: str, nick: str, scrolls=50, flag=True):
        # Reseteamos y actualizamos los atributos de la clase para el nuevo perfil
        self.videos_interceptados = [] 
        self.current_creador = creador_nombre
        self.current_nick = str(nick).strip('/').replace('@', '').split('?')[0]
        
        url = f"https://www.tiktok.com/@{self.current_nick}"
        
        print(f"\n🌍 Entrando a @{self.current_nick} ({creador_nombre})...")
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
            await pagina.wait_for_timeout(3000)
            
            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 4: 
                    print(f"   🛑 Fondo del perfil alcanzado en el scroll {i+1}.")
                    break 
            else:
                intentos_sin_bajar = 0 
                altura_anterior = altura_actual
                
        df = pd.DataFrame(self.videos_interceptados)
        if not df.empty:
            df = df.drop_duplicates(subset=['video_id'])
            df = df.drop(columns=['video_id'])
            print(f"   ✅ ¡Conseguidos {len(df)} vídeos ÚNICOS y LIMPIOS de {creador_nombre}!")
            
        return df
    

    async def _ejecutar_extraccion_tiktok(self):
        # Filtramos los que no tienen TikTok
        creadores_validos = self.df_tiktok.dropna(subset=['tiktok']).copy()
        print(f"\n🚀 Iniciando extracción masiva para {len(creadores_validos)} creadores de TikTok...\n")

        directorio_sesion = "./tiktok_session"
        todos_los_datos = [] # Aquí acumularemos los DataFrames
        
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
            
            # Conectamos el evento a nuestro método de clase
            pagina.on("response", self._interceptar_trafico)
            
            flag = True
            for index, fila in creadores_validos.iterrows():
                creador_nombre = fila['creador']
                nick_tiktok = fila['tiktok']
                
                df_resultados = await self._extraer_perfil(pagina, creador_nombre, nick_tiktok, scrolls=50, flag=flag)
                flag = False
                
                if not df_resultados.empty:
                    todos_los_datos.append(df_resultados)
                else:
                    print(f"   ⚠️ No se obtuvieron datos para {creador_nombre}.")
                    
                print("   ⏳ Esperando 5 segundos antes de cambiar de perfil...")
                await pagina.wait_for_timeout(5000)
                
            print("Cerrando navegador... Extracción finalizada.")
            await contexto.close()

        # Al terminar, unimos todo y lo guardamos en el atributo de la clase
        if todos_los_datos:
            self.df_tiktok = pd.concat(todos_los_datos, ignore_index=True)
            print(f"\n🎉 EXTRACCIÓN DE TIKTOK COMPLETADA 🎉")
            print(f"📊 Total histórico de vídeos en memoria: {len(self.df_tiktok)}")
        else:
            print("\n⚠️ No se logró extraer ningún dato de TikTok.")
            self.df_tiktok = pd.DataFrame()

    def _extraccion_tiktok(self):
        """
        Este es el método que llamarás desde tu script principal.
        Se encarga de crear el bucle de eventos asíncrono y ejecutar el motor de Playwright.
        """
        asyncio.run(self._ejecutar_extraccion_tiktok())
    

    # ----------------------------------------------
    # EXTRACCIÓN DE YOUTUBE
    # ----------------------------------------------

    def _obtener_todos_los_videos(self, handle: str) -> list:
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
    
    def _extraccion_youtube(self):
        print(f"\n▶️ Encontrados {len(self.df_youtube)} creadores. Empezando la extracción de YouTube...\n")

        historico_videos = []

        for index, row in self.df_youtube.iterrows():
            usuario_yt = row.get('youtube') 
            
            if pd.isna(usuario_yt) or usuario_yt == '':
                continue
                
            print(f"[{index + 1}/{len(self.df_youtube)}] Extrayendo vídeos de: {usuario_yt}...")
            
            videos = self.obtener_todos_los_videos(usuario_yt)
            
            if videos:
                historico_videos.extend(videos)
                print(f"   -> {len(videos)} vídeos extraídos de {usuario_yt}.")
            
            time.sleep(5)

        # --- AL TERMINAR EL BUCLE ---
        # Convertimos la lista a DataFrame y lo guardamos en el atributo de la clase
        if historico_videos:
            self.df_youtube = pd.DataFrame(historico_videos)
            print(f"\n✅ Extracción de YouTube completada. {len(self.df_youtube)} vídeos guardados en self.df_youtube.")
        else:
            print("\n⚠️ No se extrajo ningún dato de YouTube.")
    
    # ----------------------------------------------
    # EXTRACCIÓN DE TWITCH
    # ----------------------------------------------
    def _obtener_usuario_info(self, token: str, client_id: str, login_name: str) -> dict:
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
    
    def _obtener_todos_los_videos_twitch(self, token: str, client_id: str, user_id: str, user_name: str) -> list:
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
    
    def _extraccion_twitch(self):
        print(f"\n🎮 Encontrados {len(self.df_twitch)} creadores. Empezando la extracción de Twitch...\n")

        historico_videos = []

        for index, row in self.df_twitch.iterrows():
            usuario_twitch = row.get('twitch') 
            
            if pd.isna(usuario_twitch) or usuario_twitch == '':
                continue
                
            print(f"[{index + 1}/{len(self.df_twitch)}] Extrayendo vídeos de: {usuario_twitch}...")
            
            user_info = self.obtener_usuario_info(self.twitch_token, self.df_creadors['TWITCH_CLIENT_ID'].iloc[0], usuario_twitch)
            
            if user_info:
                user_id = user_info.get('id')
                user_name = user_info.get('display_name')
                videos = self.obtener_todos_los_videos_twitch(self.twitch_token, self.df_creadors['TWITCH_CLIENT_ID'].iloc[0], user_id, user_name)
                
                if videos:
                    historico_videos.extend(videos)
                    print(f"   -> {len(videos)} vídeos extraídos de {usuario_twitch}.")
                else:
                    print(f"   ⚠️ No se obtuvieron vídeos para {usuario_twitch}.")
            else:
                print(f"   ⚠️ No se encontró información para el usuario {usuario_twitch}.")
            
            time.sleep(5)

        # --- AL TERMINAR EL BUCLE ---
        # Convertimos la lista a DataFrame y lo guardamos en el atributo de la clase
        if historico_videos:
            self.df_twitch = pd.DataFrame(historico_videos)
            print(f"\n✅ Extracción de Twitch completada. {len(self.df_twitch)} vídeos guardados en self.df_twitch.")
        else:
            print("\n⚠️ No se extrajo ningún dato de Twitch.")


    # ----------------------------------------------
    # EXTRACCIÓN DE PODCASTS
    # ----------------------------------------------

    def _obtener_todos_los_episodios(self, token: str, show_id: str) -> list:
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
    
    def _extraccion_podcast(self):
        print(f"\n🎙️ Encontrados {len(self.df_podcast)} creadores. Empezando la extracción de Podcasts...\n")

        historico_episodios = []

        for index, row in self.df_podcast.iterrows():
            show_id = row.get('podcast') 
            
            if pd.isna(show_id) or show_id == '':
                continue
                
            print(f"[{index + 1}/{len(self.df_podcast)}] Extrayendo episodios de: {show_id}...")
            
            episodios = self.obtener_todos_los_episodios(self.spotify_token, show_id)
            
            if episodios:
                historico_episodios.extend(episodios)
                print(f"   -> {len(episodios)} episodios extraídos de {show_id}.")
            else:
                print(f"   ⚠️ No se obtuvieron episodios para {show_id}.")
            
            time.sleep(5)

        # --- AL TERMINAR EL BUCLE ---
        # Convertimos la lista a DataFrame y lo guardamos en el atributo de la clase
        if historico_episodios:
            self.df_podcast = pd.DataFrame(historico_episodios)
            print(f"\n✅ Extracción de Podcasts completada. {len(self.df_podcast)} episodios guardados en self.df_podcast.")
        else:
            print("\n⚠️ No se extrajo ningún dato de Podcasts.")


    # ----------------------------------------------
    # EXTRACCIÓN GENERAL
    # ----------------------------------------------
    def extraction(self):
        self._extraccion_tiktok() # tiktok primero por el captcha manual
        self._extraccion_instagram()
        self._extraccion_youtube()
        self._extraccion_twitch()
        self._extraccion_podcast()

        print("\n✅ Extracción de redes sociales completada.")
        print("Guardando resultados en archivos parquet...")

        self.df_instagram.to_parquet(self.data_path / "historico_instagram.parquet", index=False)
        self.df_tiktok.to_parquet(self.data_path / "historico_tiktok.parquet", index=False)
        self.df_youtube.to_parquet(self.data_path / "historico_youtube.parquet", index=False)
        self.df_twitch.to_parquet(self.data_path / "historico_twitch.parquet", index=False)
        self.df_podcast.to_parquet(self.data_path / "historico_podcast.parquet", index=False)

        print("✅ Resultados guardados en archivos parquet.")
