import pandas as pd
import requests
import time
from pathlib import Path
import json
import re

import asyncio
from playwright.async_api import async_playwright

import yt_dlp

class Extractor:
    def __init__(self, config_path="tokens.json"):

        def obtener_token_twitch(client_id: str, client_secret: str) -> str:
            url = "https://id.twitch.tv/oauth2/token"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'
            }
            response = requests.post(url, params=params)
            return response.json().get('access_token')
        
            
        # def obtener_token_spotify(client_id: str, client_secret: str) -> str:
        #     url = "https://accounts.spotify.com/api/token"
        #     headers = {"Content-Type": "application/x-www-form-urlencoded"}
        #     data = {
        #         "grant_type": "client_credentials",
        #         "client_id": client_id,
        #         "client_secret": client_secret
        #     }
        #     response = requests.post(url, headers=headers, data=data)
        #     return response.json().get('access_token')
        
        # RUTA DE DATOS
        self.data_path = Path().cwd() / "data"

        # OBTENEMOS TOKENS DE CONFIGURACIÓN
        with open(Path().cwd() / config_path) as f:
            config = json.load(f)

            self.TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
            TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
            self.twitch_token = obtener_token_twitch(self.TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)

            # SPOTIFY_CLIENT_ID = config.get('SPOTIFY_CLIENT_ID')
            # SPOTIFY_CLIENT_SECRET = config.get('SPOTIFY_CLIENT_SECRET')
            # self.spotify_token = obtener_token_spotify(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

        # DATAFRAMES DE CREADORES POR RED SOCIAL
        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")
        self.df_instagram = self._obtener_columna_red_social("instagram")
        self.df_tiktok = self._obtener_columna_red_social("tiktok")
        self.df_youtube = self._obtener_columna_red_social("youtube")
        self.df_twitch = self._obtener_columna_red_social("twitch")
        #self.df_podcast = self._obtener_columna_red_social("podcast")

        self.df_instagram = self.df_instagram.iloc[10:15]
        self.df_tiktok = self.df_tiktok.iloc[10:15]
        self.df_youtube = self.df_youtube.iloc[5:10]
        self.df_twitch = self.df_twitch.iloc[:5]
        #self.df_podcast = self.df_podcast.iloc[10:15]

        # UTILIDADES PARA INSTAGRAM (PLAYWRIGHT)
        self.posts_interceptados = []
        self.current_nick_ig_limpio = ""
        self.current_nick_ig_original = ""
        self.current_creador_ig = ""

        # UTILIDADES PARA TIKTOK (PLAYWRIGHT)
        self.videos_interceptados = []
        self.current_nick_tiktok_limpio = ""
        self.current_nick_tiktok_original = ""
        self.current_creador_tiktok = ""
    

    def _obtener_columna_red_social(self, red_social: str) -> pd.DataFrame:
        # Extraemos el creador y su nick, manteniendo el nombre original "nick_redsocial"
        df = self.df_creadors[["creador", f"nick_{red_social}"]].copy()
        df = df.dropna(subset=[f"nick_{red_social}"])
        return df
    
    # ----------------------------------------------
    # EXTRACCIÓN DE INSTAGRAM
    # ----------------------------------------------
    
    def _parsear_nodo_ig(self, node):
        likes = node.get("edge_media_preview_like", {})
        likes_count = likes.get("count", 0) if isinstance(likes, dict) else 0
        
        comentarios = node.get("edge_media_to_comment", {})
        comentarios_count = comentarios.get("count", 0) if isinstance(comentarios, dict) else 0

        return {
            "creador": self.current_creador_ig,
            "nick_instagram": self.current_nick_ig_original, # <- EXACTO AL PARQUET INICIAL
            "seguidores": getattr(self, 'current_followers_ig', 0),
            "post_id": node.get("shortcode"),
            "fecha_publicacion": pd.to_datetime(node.get("taken_at_timestamp"), unit='s') if node.get("taken_at_timestamp") else None,
            "tipo_publicacion": node.get("__typename"),
            "likes": likes_count,
            "comentarios": comentarios_count,
            "url_post": f"https://www.instagram.com/p/{node.get('shortcode')}/" if node.get("shortcode") else None
        }

    def _parsear_item_ig(self, item):
        return {
            "creador": self.current_creador_ig,
            "nick_instagram": self.current_nick_ig_original, # <- EXACTO AL PARQUET INICIAL
            "seguidores": getattr(self, 'current_followers_ig', 0),
            "post_id": item.get("code"),
            "fecha_publicacion": pd.to_datetime(item.get("taken_at"), unit='s') if item.get("taken_at") else None,
            "tipo_publicacion": item.get("media_type"), 
            "likes": item.get("like_count", 0),
            "comentarios": item.get("comment_count", 0),
            "url_post": f"https://www.instagram.com/p/{item.get('code')}/" if item.get("code") else None
        }

    def _buscar_posts_en_json(self, data):
        if isinstance(data, dict):
            if 'edge_followed_by' in data and isinstance(data['edge_followed_by'], dict) and 'count' in data['edge_followed_by']:
                self.current_followers_ig = data['edge_followed_by']['count']

            if 'shortcode' in data and 'edge_media_preview_like' in data:
                self.posts_interceptados.append(self._parsear_nodo_ig(data))
            elif 'code' in data and 'like_count' in data:
                self.posts_interceptados.append(self._parsear_item_ig(data))
            else:
                for valor in data.values():
                    self._buscar_posts_en_json(valor)
                    
        elif isinstance(data, list):
            for item in data:
                self._buscar_posts_en_json(item)

    async def _interceptar_trafico_instagram(self, response):
        if response.request.resource_type in ["fetch", "xhr"]:
            if "graphql" in response.url or "api/v1" in response.url:
                try:
                    data = await response.json()
                    self._buscar_posts_en_json(data)
                except:
                    pass

    async def _extraer_perfil_instagram(self, pagina, creador_nombre: str, nick_original: str, scrolls=50, flag=True):
        self.posts_interceptados = []
        self.current_followers_ig = 0
        self.current_creador_ig = creador_nombre
        self.current_nick_ig_original = nick_original # <- GUARDAMOS EL ORIGINAL
        self.current_nick_ig_limpio = str(nick_original).strip('/').split('?')[0] # <- LIMPIO SOLO PARA NAVEGAR

        url = f"https://www.instagram.com/{self.current_nick_ig_limpio}/"
        print(f"\n🌍 Entrando a @{self.current_nick_ig_limpio} ({creador_nombre})...")
        
        await pagina.goto(url)

        if flag:
            print("   ⏳ ATENCIÓN: Esperando 30 segundos.")
            print("   👉 Si Instagram te pide Iniciar Sesión, hazlo AHORA en la ventana del navegador.")
            await pagina.wait_for_timeout(30000) 
        
        try:
            await pagina.wait_for_selector("header", timeout=20000)
            self.current_followers_ig = 0
            texto_crudo = ""
            
            selector_enlace = f'a[href="/{self.current_nick_ig_limpio}/followers/"]'
            enlace_seguidores = pagina.locator(selector_enlace)
            
            if await enlace_seguidores.count() > 0:
                span_con_title = enlace_seguidores.locator("span[title]")
                if await span_con_title.count() > 0:
                    texto_crudo = await span_con_title.first.get_attribute("title")
                else:
                    texto_crudo = await enlace_seguidores.inner_text()
            
            if not texto_crudo:
                texto_cabecera = await pagina.locator("header").inner_text()
                match = re.search(r'([\d\.,MKmk]+)\s*(seguidores|followers|mil seguidores)', texto_cabecera.replace('\n', ' '), re.IGNORECASE)
                if match:
                    texto_crudo = match.group(1)
            
            if not texto_crudo:
                meta_desc = await pagina.locator('meta[name="description"]').get_attribute("content")
                match_meta = re.search(r'([\d\.,MKmk]+)\s*(Seguidores|Followers)', str(meta_desc), re.IGNORECASE)
                if match_meta:
                    texto_crudo = match_meta.group(1)

            if texto_crudo:
                texto_limpio = str(texto_crudo).upper().replace(' ', '')
                
                if 'M' in texto_limpio:
                    num = float(texto_limpio.replace('M', '').replace(',', '.'))
                    self.current_followers_ig = int(num * 1000000)
                elif 'K' in texto_limpio:
                    num = float(texto_limpio.replace('K', '').replace(',', '.'))
                    self.current_followers_ig = int(num * 1000)
                else:
                    numero_solo = ''.join(c for c in texto_limpio if c.isdigit())
                    self.current_followers_ig = int(numero_solo) if numero_solo else 0
                    
                print(f"   👥 Seguidores detectados: {self.current_followers_ig}")
            else:
                print("   ⚠️ No se encontró la métrica de seguidores en el DOM ni en los Metadatos.")

        except Exception as e:
            print(f"   ⚠️ Fallo técnico leyendo seguidores: {e}")
            self.current_followers_ig = 0

        print(f"   🖱️ Iniciando hasta {scrolls} scrolls...")
        altura_anterior = await pagina.evaluate("document.body.scrollHeight")
        intentos_sin_bajar = 0

        for i in range(scrolls):
            await pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await pagina.wait_for_timeout(3000) 
            
            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 3:
                    print(f"   🛑 Fondo del perfil alcanzado en el scroll {i+1}.")
                    break
            else:
                intentos_sin_bajar = 0
                altura_anterior = altura_actual
        
        df = pd.DataFrame(self.posts_interceptados)
        
        if not df.empty:
            df = df.dropna(subset=['post_id'])
            df['seguidores'] = self.current_followers_ig
            df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0)
            df['comentarios'] = pd.to_numeric(df['comentarios'], errors='coerce').fillna(0)
            
            df = df.sort_values(by=['likes', 'comentarios'], ascending=[False, False])
            df = df.drop_duplicates(subset=['post_id'], keep='first')
            
            print(f"   ✅ ¡Conseguidos {len(df)} posts ÚNICOS y LIMPIOS de {creador_nombre}!")
        
        return df

    async def _ejecutar_extraccion_instagram(self):
        print(f"\n🚀 Iniciando extracción masiva para {len(self.df_instagram)} creadores de Instagram...\n")

        directorio_sesion = "./instagram_session"
        todos_los_datos = []
        
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
            
            pagina.on("response", self._interceptar_trafico_instagram)
            
            flag = True
            for index, fila in self.df_instagram.iterrows():
                creador_nombre = fila['creador']
                nick_ig = fila['nick_instagram']
                
                df_resultados = await self._extraer_perfil_instagram(pagina, creador_nombre, nick_ig, flag=flag)
                flag = False
                
                if not df_resultados.empty:
                    todos_los_datos.append(df_resultados)
                else:
                    print(f"   ⚠️ No se obtuvieron datos para {creador_nombre}.")
                    
                print("   ⏳ Esperando 5 segundos antes de cambiar de perfil...")
                await pagina.wait_for_timeout(5000)
                
            print("Cerrando navegador de Instagram... Extracción finalizada.")
            await contexto.close()

        if todos_los_datos:
            self.df_instagram = pd.concat(todos_los_datos, ignore_index=True)
            print(f"\n🎉 EXTRACCIÓN DE INSTAGRAM COMPLETADA 🎉")
            print(f"📊 Total histórico de posts en memoria: {len(self.df_instagram)}")
        else:
            print("\n⚠️ No se logró extraer ningún dato de Instagram.")
            self.df_instagram = pd.DataFrame()

    def _extraccion_instagram(self):
        asyncio.run(self._ejecutar_extraccion_instagram())
    

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
                        
                        if autor_video == self.current_nick_tiktok_limpio:
                            stats = video.get('stats', {})
                            autor_stats = video.get('authorStats', {})
                            
                            self.videos_interceptados.append({
                                "creador": self.current_creador_tiktok, 
                                "nick_tiktok": self.current_nick_tiktok_original, 
                                "seguidores": autor_stats.get('followerCount', 0), 
                                "fecha_publicacion": pd.to_datetime(video.get('createTime'), unit='s'),
                                "vistas": stats.get('playCount', 0),
                                "likes": stats.get('diggCount', 0),
                                "video_id": video.get('id') 
                            })
            except:
                pass
    

    async def _extraer_perfil(self, pagina, creador_nombre: str, nick_original: str, scrolls=50, flag=True):
        self.videos_interceptados = [] 
        self.current_creador_tiktok = creador_nombre
        self.current_nick_tiktok_original = nick_original # <- GUARDAMOS EL ORIGINAL
        self.current_nick_tiktok_limpio = str(nick_original).strip('/').replace('@', '').split('?')[0]
        
        url = f"https://www.tiktok.com/@{self.current_nick_tiktok_limpio}"
        
        print(f"\n🌍 Entrando a @{self.current_nick_tiktok_limpio} ({creador_nombre})...")
        await pagina.goto(url)
        
        if flag:
            print("   ⏳ Esperando carga... (Si sale Captcha, resuélvelo)")
            await pagina.wait_for_timeout(20000) 
        
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
        print(f"\n🚀 Iniciando extracción masiva para {len(self.df_tiktok)} creadores de TikTok...\n")

        directorio_sesion = "./tiktok_session"
        todos_los_datos = []
        
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
            
            pagina.on("response", self._interceptar_trafico)
            
            flag = True
            for index, fila in self.df_tiktok.iterrows():
                creador_nombre = fila['creador']
                nick_tiktok = fila['nick_tiktok'] 
                
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

        if todos_los_datos:
            self.df_tiktok = pd.concat(todos_los_datos, ignore_index=True)
            print(f"\n🎉 EXTRACCIÓN DE TIKTOK COMPLETADA 🎉")
            print(f"📊 Total histórico de vídeos en memoria: {len(self.df_tiktok)}")
        else:
            print("\n⚠️ No se logró extraer ningún dato de TikTok.")
            self.df_tiktok = pd.DataFrame()

    def _extraccion_tiktok(self):
        asyncio.run(self._ejecutar_extraccion_tiktok())
    

    # ----------------------------------------------
    # EXTRACCIÓN DE YOUTUBE
    # ----------------------------------------------

    def _obtener_todos_los_videos(self, creador_nombre: str, nick_original: str) -> list:
        handle_str = str(nick_original).strip()
        if not handle_str.startswith('@'):
            handle_str = f"@{handle_str}"
            
        url = f"https://www.youtube.com/{handle_str}/videos"
        videos_extraidos = []

        ydl_opts_rapido = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
        }
        
        print(f"  -> Buscando cuántos vídeos tiene {handle_str}...")
        try:
            with yt_dlp.YoutubeDL(ydl_opts_rapido) as ydl:
                info_canal = ydl.extract_info(url, download=False)
                if not info_canal or 'entries' not in info_canal:
                    return []
                
                lista_videos = info_canal['entries']
                total_videos = len(lista_videos)
                
                subscriptores = info_canal.get('channel_follower_count') 
                if subscriptores is None:
                    subscriptores = info_canal.get('subscriber_count', 0)

                print(f"  -> ¡Encontrados {total_videos} vídeos y {subscriptores} subs! Empezando extracción profunda...")
        
        except Exception as e:
            print(f"Error al obtener la lista de {handle_str}: {e}")
            return []

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
                    video = ydl.extract_info(url_video, download=False)
                    
                    if video:
                        video_data = {
                            "creador": creador_nombre,
                            "nick_youtube": nick_original,
                            "seguidores": subscriptores or 0,
                            "id_video": video.get('id'),
                            "titulo": video.get('title'),
                            "url_video": video.get('webpage_url'),
                            "fecha_publicacion": video.get('upload_date'), 
                            "visualizaciones": video.get('view_count', 0) or 0,
                            "likes": video.get('like_count', 0) or 0,
                            "duracion_segundos": video.get('duration')
                        }
                        videos_extraidos.append(video_data)
                        
                        if (i + 1) % 25 == 0 or (i + 1) == total_videos:
                            print(f"     ... procesados {i + 1}/{total_videos} vídeos")
                            time.sleep(5)
                            
                except Exception as e:
                    pass 
                    
        return videos_extraidos
    
    def _extraccion_youtube(self):
        print(f"\n▶️ Encontrados {len(self.df_youtube)} creadores. Empezando la extracción de YouTube...\n")

        historico_videos = []
        c = 1

        for _, row in self.df_youtube.iterrows():
            creador_nombre = row.get('creador')
            usuario_yt = row.get('nick_youtube') 
            
            if pd.isna(usuario_yt) or usuario_yt == '':
                continue
                
            print(f"[{c}/{len(self.df_youtube)}] Extrayendo vídeos de: {usuario_yt}...")
            c += 1

            videos = self._obtener_todos_los_videos(creador_nombre, usuario_yt)
            
            if videos:
                historico_videos.extend(videos)
                print(f"   -> {len(videos)} vídeos extraídos de {usuario_yt}.")
            
            time.sleep(5)

        if historico_videos:
            self.df_youtube = pd.DataFrame(historico_videos)
            print(f"\n✅ Extracción de YouTube completada. {len(self.df_youtube)} vídeos guardados en self.df_youtube.")
        else:
            print("\n⚠️ No se extrajo ningún dato de YouTube.")
    
    # ----------------------------------------------
    # EXTRACCIÓN DE TWITCH
    # ----------------------------------------------
    def _obtener_seguidores_twitch(self, token: str, client_id: str, user_id: str) -> int:
        url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}"
        headers = {
            'Client-ID': client_id,
            'Authorization': f'Bearer {token}'
        }
        try:
            response = requests.get(url, headers=headers)
            return response.json().get('total', 0)
        except:
            return 0
        
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
    
    def _obtener_todos_los_videos_twitch(self, token: str, client_id: str, user_id: str, creador_nombre: str, nick_original: str, seguidores: int) -> list:
        videos_lista = []
        cursor = None
        
        headers = {
            'Client-ID': client_id,
            'Authorization': f'Bearer {token}'
        }
        
        while True:
            url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first=100"
            
            if cursor:
                url += f"&after={cursor}"
                
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"   ! Error API Twitch ({response.status_code})")
                break
                
            data = response.json()
            
            if 'data' in data and len(data['data']) > 0:
                for v in data['data']:
                    videos_lista.append({
                        "creador": creador_nombre,
                        "nick_twitch": nick_original, 
                        "seguidores": seguidores,
                        "id_video": v['id'],
                        "titulo": v['title'],
                        "fecha_publicacion": v['created_at'],
                        "visualizaciones": v['view_count'],
                        "duracion": v['duration'],
                        "tipo": v['type'] 
                    })
                
                cursor = data.get('pagination', {}).get('cursor')
                
                if not cursor:
                    break
                
                time.sleep(0.1)
            else:
                break
                
        return videos_lista
    
    def _extraccion_twitch(self):
        print(f"\n🎮 Encontrados {len(self.df_twitch)} creadores. Empezando la extracción de Twitch...\n")

        historico_videos = []
        c = 1

        for _, row in self.df_twitch.iterrows():
            creador_nombre = row.get('creador')
            usuario_twitch = row.get('nick_twitch') 
            
            if pd.isna(usuario_twitch) or usuario_twitch == '':
                continue
                
            print(f"[{c}/{len(self.df_twitch)}] Extrayendo vídeos de: {usuario_twitch}...")
            c += 1

            user_info = self._obtener_usuario_info(self.twitch_token, self.TWITCH_CLIENT_ID, usuario_twitch)
            
            if user_info:
                user_id = user_info.get('id')

                seguidores = self._obtener_seguidores_twitch(self.twitch_token, self.TWITCH_CLIENT_ID, user_id)
                videos = self._obtener_todos_los_videos_twitch(self.twitch_token, self.TWITCH_CLIENT_ID, user_id, creador_nombre, usuario_twitch, seguidores)
                
                if videos:
                    historico_videos.extend(videos)
                    print(f"   -> {len(videos)} vídeos extraídos de {usuario_twitch}.")
                else:
                    print(f"   ⚠️ No se obtuvieron vídeos para {usuario_twitch}.")
            else:
                print(f"   ⚠️ No se encontró información para el usuario {usuario_twitch}.")
            
            time.sleep(2)

        if historico_videos:
            self.df_twitch = pd.DataFrame(historico_videos)
            print(f"\n✅ Extracción de Twitch completada. {len(self.df_twitch)} vídeos guardados en self.df_twitch.")
        else:
            print("\n⚠️ No se extrajo ningún dato de Twitch.")

    '''
    # ----------------------------------------------
    # EXTRACCIÓN DE PODCASTS
    # ----------------------------------------------

    def _obtener_todos_los_episodios(self, token: str, creador_nombre: str, nick_original: str) -> list:
        episodios_lista = []
        
        id_limpio = str(nick_original).split('/')[-1].split('?')[0].strip()
        
        url = f"https://api.spotify.com/v1/shows/{id_limpio}/episodes?limit=50"
        headers = {"Authorization": f"Bearer {token}"}
        
        while url:
            try:
                response = requests.get(url, headers=headers)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print(f"   ! Rate limit alcanzado. Esperando {retry_after} segundos...")
                    time.sleep(retry_after)
                    continue 
                    
                if response.status_code != 200:
                    print(f"   ! Error API Spotify ({response.status_code}) en ID: {id_limpio}")
                    break
                    
                data = response.json()
                
                items = data.get('items', [])
                for ep in items:
                    if ep is not None and isinstance(ep, dict) and 'id' in ep:
                        episodios_lista.append({
                            "creador": creador_nombre,
                            "nick_podcast": nick_original,
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
        c = 1

        for index, row in self.df_podcast.iterrows():
            creador_nombre = row.get('creador')
            show_id = row.get('nick_podcast')

            if pd.isna(show_id) or show_id == '':
                continue
                
            print(f"[{c}/{len(self.df_podcast)}] Extrayendo episodios de: {show_id}...")
            c += 1

            episodios = self._obtener_todos_los_episodios(self.spotify_token, creador_nombre, show_id)
            
            if episodios:
                historico_episodios.extend(episodios)
                print(f"   -> {len(episodios)} episodios extraídos de {show_id}.")
            else:
                print(f"   ⚠️ No se obtuvieron episodios para {show_id}.")
            
            time.sleep(5)

        if historico_episodios:
            self.df_podcast = pd.DataFrame(historico_episodios)
            print(f"\n✅ Extracción de Podcasts completada. {len(self.df_podcast)} episodios guardados en self.df_podcast.")
        else:
            print("\n⚠️ No se extrajo ningún dato de Podcasts.")
    '''

    # ----------------------------------------------
    # EXTRACCIÓN GENERAL
    # ----------------------------------------------
    def extraction(self):
        self._extraccion_tiktok() 
        self._extraccion_instagram()
        self._extraccion_youtube()
        self._extraccion_twitch()
        #self._extraccion_podcast() De momento podcasts no, no podemos extraer oyentes

        print("\n✅ Extracción de redes sociales completada.")
        print("Guardando resultados en archivos parquet...")

        self.df_instagram.to_parquet(self.data_path / "historico_instagram.parquet", index=False)
        self.df_tiktok.to_parquet(self.data_path / "historico_tiktok.parquet", index=False)
        self.df_youtube.to_parquet(self.data_path / "historico_youtube.parquet", index=False)
        self.df_twitch.to_parquet(self.data_path / "historico_twitch.parquet", index=False)
        #self.df_podcast.to_parquet(self.data_path / "historico_podcast.parquet", index=False)

        print("✅ Resultados guardados en archivos parquet.")