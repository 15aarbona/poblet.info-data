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
        
        # RUTA DE DATOS
        self.data_path = Path().cwd() / "data"

        # OBTENEMOS TOKENS DE CONFIGURACIÓN
        with open(Path().cwd() / config_path) as f:
            config = json.load(f)

            self.TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
            TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
            self.twitch_token = obtener_token_twitch(self.TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)

        # DATAFRAMES DE CREADORES POR RED SOCIAL
        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")
        self.df_instagram = self._obtener_columna_red_social("instagram")
        self.df_tiktok = self._obtener_columna_red_social("tiktok")
        self.df_youtube = self._obtener_columna_red_social("youtube")
        self.df_twitch = self._obtener_columna_red_social("twitch")

        self.df_instagram = self.df_instagram.iloc[15:16]
        self.df_tiktok = self.df_tiktok.iloc[15:16]
        self.df_youtube = self.df_youtube.iloc[3:4]
        self.df_twitch = self.df_twitch.iloc[0:1]

        # ----------------------------------------------------
        # LECTURA DE CACHÉ (Última fecha por creador)
        # ----------------------------------------------------
        self.fechas_cache_ig = self._obtener_fechas_cache("instagram", "nick_instagram")
        self.fechas_cache_tk = self._obtener_fechas_cache("tiktok", "nick_tiktok")
        self.fechas_cache_yt = self._obtener_fechas_cache("youtube", "nick_youtube")
        self.fechas_cache_tw = self._obtener_fechas_cache("twitch", "nick_twitch")

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
        df = self.df_creadors[["creador", f"nick_{red_social}"]].copy()
        df = df.dropna(subset=[f"nick_{red_social}"])
        return df

    def _obtener_fechas_cache(self, red_social: str, col_nick: str) -> dict:
        """Lee el parquet 'clean_' para saber la fecha del último post extraído por creador"""
        archivo = self.data_path / f"clean_{red_social}.parquet"
        if archivo.exists():
            try:
                df = pd.read_parquet(archivo)
                if not df.empty and col_nick in df.columns and 'fecha_publicacion' in df.columns:
                    # Convertimos a formato sin zona horaria para comparaciones seguras
                    df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True, errors='coerce').dt.tz_localize(None)
                    return df.groupby(col_nick)['fecha_publicacion'].max().to_dict()
            except Exception as e:
                pass
        return {}
    
 # ----------------------------------------------
    # EXTRACCIÓN DE INSTAGRAM
    # ----------------------------------------------
    def _registrar_anuncio(self, data):
        """Rastrea el bloque bloqueado para extraer su ID y meterlo en la Lista Negra."""
        if isinstance(data, dict):
            if 'shortcode' in data and data['shortcode']: self.blacklist_anuncios.add(data['shortcode'])
            if 'code' in data and data['code']: self.blacklist_anuncios.add(data['code'])
            for v in data.values(): self._registrar_anuncio(v)
        elif isinstance(data, list):
            for item in data: self._registrar_anuncio(item)

    def _parsear_nodo_ig(self, node):
        likes = node.get("edge_media_preview_like", {})
        likes_count = likes.get("count", 0) if isinstance(likes, dict) else 0
        comentarios = node.get("edge_media_to_comment", {})
        comentarios_count = comentarios.get("count", 0) if isinstance(comentarios, dict) else 0

        return {
            "creador": self.current_creador_ig,
            "nick_instagram": self.current_nick_ig_original, 
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
            "nick_instagram": self.current_nick_ig_original, 
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

            # 🛑 CORTAFUEGOS ESTRUCTURAL
            ramas_prohibidas = ['injected', 'injected_items', 'ad_items', 'sponsored_items', 'xdt_injected_items', 'sponsored_feed_item']
            for clave in list(data.keys()):
                if str(clave).lower() in ramas_prohibidas:
                    self._registrar_anuncio(data[clave]) # ¡A la lista negra!
                    del data[clave]

            # 🛑 ESCUDO DE METADATOS AVANZADO
            es_anuncio = False
            if str(data.get('is_ad')).lower() == 'true': es_anuncio = True
            if str(data.get('is_sponsored')).lower() == 'true': es_anuncio = True
            
            campos_sospechosos = ['ad_id', 'sponsored_label_info', 'ad_action', 'tracker_token', 'ad_metadata', 'about_ad_params', 'hide_reasons_v2']
            for campo in campos_sospechosos:
                if data.get(campo) is not None: 
                    es_anuncio = True
                    break
                    
            if data.get('__typename') in ['GraphAd', 'GraphSponsoredMedia', 'XDTGraphAd']: 
                es_anuncio = True

            if es_anuncio:
                self._registrar_anuncio(data) # ¡A la lista negra!
                return

            nick_limpio = str(self.current_nick_ig_limpio).strip().lower()
            
            # 💡 ESCUDO DE NICK / URL
            es_formato_1 = 'shortcode' in data and 'edge_media_preview_like' in data
            es_formato_2 = 'code' in data and 'like_count' in data
            
            if es_formato_1 or es_formato_2:
                nicks_en_post = []
                
                # A) Buscamos nicks en campos de usuario principal
                if isinstance(data.get('owner'), dict): nicks_en_post.append(data['owner'].get('username'))
                if isinstance(data.get('user'), dict): nicks_en_post.append(data['user'].get('username'))
                if isinstance(data.get('owner'), str): nicks_en_post.append(data.get('owner'))
                if isinstance(data.get('user'), str): nicks_en_post.append(data.get('user'))
                
                # B) Buscamos el nick en el autor de la descripción (caption)
                try: nicks_en_post.append(data['edge_media_to_caption']['edges'][0]['node']['user']['username'])
                except: pass
                try: nicks_en_post.append(data['caption']['user']['username'])
                except: pass
                
                # C) Buscamos en enlaces o URLs internas
                permalink = data.get('permalink')
                if isinstance(permalink, str) and '/' in permalink:
                    partes = [p for p in permalink.strip('/').split('/') if p]
                    if len(partes) > 1 and partes[1] == 'p': 
                        nicks_en_post.append(partes[0])

                # 🤝 D) SALVAVIDAS DE COLABORACIONES (Shared Posts)
                if 'coauthor_producers' in data and isinstance(data['coauthor_producers'], list):
                    for coautor in data['coauthor_producers']:
                        if isinstance(coautor, dict) and 'username' in coautor:
                            nicks_en_post.append(coautor['username'])

                # Limpiamos la lista quitando vacíos y asegurando minúsculas
                nicks_en_post = [str(n).lower().strip() for n in nicks_en_post if n]
                
                # ⚔️ LA PRUEBA DE FUEGO
                if nicks_en_post and nick_limpio not in nicks_en_post:
                    self._registrar_anuncio(data) # ¡A la lista negra!
                    print(f"   🛡️ [ESCUDO NICK] Anuncio cazado (Pertenece a {nicks_en_post[0]}). Añadido a Lista Negra.")
                    return
                
                if es_formato_1:
                    self.posts_interceptados.append(self._parsear_nodo_ig(data))
                else:
                    self.posts_interceptados.append(self._parsear_item_ig(data))
                return 

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

    async def _extraer_perfil_instagram(self, pagina, creador_nombre: str, nick_original: str, fecha_limite=None, scrolls=50, flag=True):
        self.posts_interceptados = []
        self.blacklist_anuncios = set() # 📋 NUEVO: Inicializamos la lista negra
        self.current_followers_ig = 0
        self.current_creador_ig = creador_nombre
        self.current_nick_ig_original = nick_original 
        self.current_nick_ig_limpio = str(nick_original).strip('/').split('?')[0] 

        url = f"https://www.instagram.com/{self.current_nick_ig_limpio}/"
        print(f"\n🌍 Entrando a @{self.current_nick_ig_limpio} ({creador_nombre})...")
        if fecha_limite: print(f"   ⏱️ Caché detectada: Extrayendo hasta {fecha_limite.date()}")
        
        await pagina.goto(url)
        await pagina.wait_for_timeout(3000)

        if flag:
            print("   ⏳ ATENCIÓN: Esperando 30 segundos (Primer inicio).")
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
                if match: texto_crudo = match.group(1)
            
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
        except Exception as e:
            self.current_followers_ig = 0

        print(f"   🖱️ Iniciando hasta {scrolls} scrolls...")
        altura_anterior = await pagina.evaluate("document.body.scrollHeight")
        intentos_sin_bajar = 0

        for i in range(scrolls):
            await pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await pagina.wait_for_timeout(3000) 
            
            if fecha_limite and self.posts_interceptados:
                fechas = [pd.to_datetime(p['fecha_publicacion'], utc=True).tz_localize(None) for p in self.posts_interceptados if pd.notna(p['fecha_publicacion'])]
                if fechas and min(fechas) <= fecha_limite:
                    print(f"   🛑 Caché alcanzada ({min(fechas).date()}). Deteniendo scroll.")
                    break

            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 3: break
            else:
                intentos_sin_bajar = 0
                altura_anterior = altura_actual
        
        df = pd.DataFrame(self.posts_interceptados)
        if not df.empty:
            df = df.dropna(subset=['post_id'])
            
            # 🛑 LA PURGA DEFINITIVA: ELIMINAR LOS FICHADOS EN LA LISTA NEGRA
            total_antes = len(df)
            df = df[~df['post_id'].isin(self.blacklist_anuncios)]
            total_despues = len(df)
            
            if total_antes > total_despues:
                print(f"   🗑️ [PURGA] Se han eliminado definitivamente {total_antes - total_despues} anuncios de los datos guardados.")

            df['seguidores'] = self.current_followers_ig
            df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0)
            df['comentarios'] = pd.to_numeric(df['comentarios'], errors='coerce').fillna(0)
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
            
            if fecha_limite:
                df = df[df['fecha_publicacion'] > fecha_limite] 
            
            df = df.sort_values(by=['likes', 'comentarios'], ascending=[False, False])
            df = df.drop_duplicates(subset=['post_id'], keep='first')
            
            print(f"   ✅ ¡Conseguidos {len(df)} posts PURAMENTE NUEVOS de {creador_nombre}!")
        
        return df

    async def _bloquear_recursos_innecesarios(self, route):
        url = route.request.url.lower()
        tipo = route.request.resource_type
        trackers = ["googleads", "doubleclick", "analytics", "tiktok.com/log/", "facebook.com/tr/", "pixel", "adsystem"]
        if any(t in url for t in trackers):
            await route.abort()
            return
        if tipo in ["font", "stylesheet"]:
            await route.abort()
            return
        await route.continue_()

    async def _ejecutar_extraccion_instagram(self):
        print(f"\n🚀 Iniciando extracción de Instagram...\n")
        directorio_sesion = "./instagram_session"
        todos_los_datos = []
        
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir=directorio_sesion,
                headless=False,
                viewport={'width': 1920, 'height': 1080},
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            pagina = contexto.pages[0]
            await pagina.route("**/*", self._bloquear_recursos_innecesarios)
            pagina.on("response", self._interceptar_trafico_instagram)
            
            flag = True
            for index, fila in self.df_instagram.iterrows():
                creador_nombre = fila['creador']
                nick_ig = fila['nick_instagram'] 
                fecha_limite = self.fechas_cache_ig.get(nick_ig)
                
                df_resultados = await self._extraer_perfil_instagram(pagina, creador_nombre, nick_ig, fecha_limite, flag=flag)
                flag = False
                
                if not df_resultados.empty:
                    todos_los_datos.append(df_resultados)
                
                print("   ⏳ Esperando 5 segundos antes de cambiar de perfil...")
                await pagina.wait_for_timeout(5000)
                
            await contexto.close()

        if todos_los_datos:
            self.df_instagram = pd.concat(todos_los_datos, ignore_index=True)
            print(f"\n🎉 INSTAGRAM: Extraídas {len(self.df_instagram)} publicaciones nuevas.")
        else:
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
                        
                        # 🛡️ ESCUDO ANTI-PUBLICIDAD: Solo guardamos si el autor es EXACTAMENTE el nuestro
                        if autor_video and str(autor_video).lower() == str(self.current_nick_tiktok_limpio).lower():
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

    async def _extraer_perfil(self, pagina, creador_nombre: str, nick_original: str, fecha_limite=None, scrolls=50, flag=True):
        self.videos_interceptados = [] 
        self.current_creador_tiktok = creador_nombre
        self.current_nick_tiktok_original = nick_original 
        self.current_nick_tiktok_limpio = str(nick_original).strip('/').replace('@', '').split('?')[0]
        
        url = f"https://www.tiktok.com/@{self.current_nick_tiktok_limpio}"
        
        print(f"\n🌍 Entrando a @{self.current_nick_tiktok_limpio} ({creador_nombre})...")
        if fecha_limite: print(f"   ⏱️ Caché detectada: Extrayendo hasta {fecha_limite.date()}")
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
            
            # --- CACHE EARLY STOP ---
            if fecha_limite and self.videos_interceptados:
                fechas = [pd.to_datetime(v['fecha_publicacion'], utc=True).tz_localize(None) for v in self.videos_interceptados if pd.notna(v['fecha_publicacion'])]
                if fechas and min(fechas) <= fecha_limite:
                    print(f"   🛑 Caché alcanzada ({min(fechas).date()}). Deteniendo scroll.")
                    break

            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 4: break 
            else:
                intentos_sin_bajar = 0 
                altura_anterior = altura_actual
                
        df = pd.DataFrame(self.videos_interceptados)
        if not df.empty:
            df = df.dropna(subset=['video_id'])
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
            
            if fecha_limite:
                df = df[df['fecha_publicacion'] > fecha_limite]
                
            df = df.drop_duplicates(subset=['video_id'])
            print(f"   ✅ ¡Conseguidos {len(df)} vídeos NUEVOS de {creador_nombre}!")
            
        return df

    async def _ejecutar_extraccion_tiktok(self):
        print(f"\n🚀 Iniciando extracción de TikTok...\n")
        directorio_sesion = "./tiktok_session"
        todos_los_datos = []
        
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir=directorio_sesion,
                headless=False,
                viewport={'width': 1920, 'height': 1080},
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            pagina = contexto.pages[0]

            await pagina.route("**/*", self._bloquear_recursos_innecesarios)

            pagina.on("response", self._interceptar_trafico)
            
            flag = True
            for index, fila in self.df_tiktok.iterrows():
                creador_nombre = fila['creador']
                nick_tiktok = fila['nick_tiktok'] 
                fecha_limite = self.fechas_cache_tk.get(nick_tiktok)
                
                df_resultados = await self._extraer_perfil(pagina, creador_nombre, nick_tiktok, fecha_limite, flag=flag)
                flag = False
                
                if not df_resultados.empty:
                    todos_los_datos.append(df_resultados)
                
                print("   ⏳ Esperando 5 segundos antes de cambiar de perfil...")
                await pagina.wait_for_timeout(5000)
                
            await contexto.close()

        if todos_los_datos:
            self.df_tiktok = pd.concat(todos_los_datos, ignore_index=True)
            print(f"\n🎉 TIKTOK: Extraídos {len(self.df_tiktok)} vídeos nuevos.")
        else:
            self.df_tiktok = pd.DataFrame()

    def _extraccion_tiktok(self):
        asyncio.run(self._ejecutar_extraccion_tiktok())
    
    # ----------------------------------------------
    # EXTRACCIÓN DE YOUTUBE
    # ----------------------------------------------
    def _obtener_todos_los_videos(self, creador_nombre: str, nick_original: str, fecha_limite=None) -> list:
        handle_str = str(nick_original).strip()
        if not handle_str.startswith('@'):
            handle_str = f"@{handle_str}"
            
        url = f"https://www.youtube.com/{handle_str}/videos"
        videos_extraidos = []

        ydl_opts_rapido = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts_rapido) as ydl:
                info_canal = ydl.extract_info(url, download=False)
                if not info_canal or 'entries' not in info_canal:
                    return []
                
                lista_videos = info_canal['entries']
                subscriptores = info_canal.get('channel_follower_count') or info_canal.get('subscriber_count', 0)
        except Exception:
            return []

        ydl_opts_profundo = {'quiet': True, 'no_warnings': True, 'ignoreerrors': True}
        
        with yt_dlp.YoutubeDL(ydl_opts_profundo) as ydl:
            for i, video_breve in enumerate(lista_videos):
                url_video = video_breve.get('url')
                if not url_video: continue
                    
                try:
                    video = ydl.extract_info(url_video, download=False)
                    if video:
                        # YT devuelve string '20231225'
                        fecha_pub_str = video.get('upload_date')
                        if fecha_pub_str:
                            fecha_pub = pd.to_datetime(fecha_pub_str, format='%Y%m%d', errors='coerce')
                            # --- CACHE EARLY STOP ---
                            if fecha_limite and pd.notna(fecha_pub) and fecha_pub <= fecha_limite:
                                print(f"     🛑 Caché alcanzada. Ignorando vídeos más antiguos.")
                                break

                        videos_extraidos.append({
                            "creador": creador_nombre,
                            "nick_youtube": nick_original,
                            "seguidores": subscriptores or 0,
                            "id_video": video.get('id'),
                            "titulo": video.get('title'),
                            "url_video": video.get('webpage_url'),
                            "fecha_publicacion": fecha_pub_str, 
                            "visualizaciones": video.get('view_count', 0) or 0,
                            "likes": video.get('like_count', 0) or 0,
                            "duracion_segundos": video.get('duration')
                        })
                except Exception:
                    pass 
        return videos_extraidos
    
    def _extraccion_youtube(self):
        print(f"\n▶️ Iniciando extracción de YouTube...\n")
        historico_videos = []
        for c, row in self.df_youtube.iterrows():
            creador_nombre = row.get('creador')
            usuario_yt = row.get('nick_youtube') 
            fecha_limite = self.fechas_cache_yt.get(usuario_yt)
            
            print(f"[{c+1}/{len(self.df_youtube)}] Extrayendo: {usuario_yt}...")
            if fecha_limite: print(f"   ⏱️ Caché detectada: Extrayendo hasta {fecha_limite.date()}")

            videos = self._obtener_todos_los_videos(creador_nombre, usuario_yt, fecha_limite)
            if videos:
                historico_videos.extend(videos)
                print(f"   -> {len(videos)} vídeos extraídos.")
            
        if historico_videos:
            self.df_youtube = pd.DataFrame(historico_videos)
        else:
            self.df_youtube = pd.DataFrame()
    
    # ----------------------------------------------
    # EXTRACCIÓN DE TWITCH
    # ----------------------------------------------
    def _obtener_seguidores_twitch(self, token: str, client_id: str, user_id: str) -> int:
        url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}"
        headers = {'Client-ID': client_id, 'Authorization': f'Bearer {token}'}
        try:
            return requests.get(url, headers=headers).json().get('total', 0)
        except:
            return 0
        
    def _obtener_usuario_info(self, token: str, client_id: str, login_name: str) -> dict:
        login_clean = str(login_name).strip().replace('@', '')
        url = f"https://api.twitch.tv/helix/users?login={login_clean}"
        headers = {'Client-ID': client_id, 'Authorization': f'Bearer {token}'}
        try:
            data = requests.get(url, headers=headers).json()
            if 'data' in data and len(data['data']) > 0: return data['data'][0]
        except Exception:
            return None
    
    def _obtener_todos_los_videos_twitch(self, token: str, client_id: str, user_id: str, creador_nombre: str, nick_original: str, seguidores: int, fecha_limite=None) -> list:
        videos_lista = []
        cursor = None
        headers = {'Client-ID': client_id, 'Authorization': f'Bearer {token}'}
        detener_paginacion = False

        while not detener_paginacion:
            url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first=100"
            if cursor: url += f"&after={cursor}"
                
            response = requests.get(url, headers=headers)
            if response.status_code != 200: break
                
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                for v in data['data']:
                    fecha_pub = pd.to_datetime(v['created_at'], utc=True).tz_localize(None)
                    
                    # --- CACHE EARLY STOP ---
                    if fecha_limite and fecha_pub <= fecha_limite:
                        print(f"     🛑 Caché alcanzada. Ignorando vídeos más antiguos.")
                        detener_paginacion = True
                        break

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
                if not cursor: break
            else:
                break
                
        return videos_lista
    
    def _extraccion_twitch(self):
        print(f"\n🎮 Iniciando extracción de Twitch...\n")
        historico_videos = []
        for c, row in self.df_twitch.iterrows():
            creador_nombre = row.get('creador')
            usuario_twitch = row.get('nick_twitch') 
            fecha_limite = self.fechas_cache_tw.get(usuario_twitch)
            
            print(f"[{c+1}/{len(self.df_twitch)}] Extrayendo: {usuario_twitch}...")
            user_info = self._obtener_usuario_info(self.twitch_token, self.TWITCH_CLIENT_ID, usuario_twitch)
            
            if user_info:
                user_id = user_info.get('id')
                seguidores = self._obtener_seguidores_twitch(self.twitch_token, self.TWITCH_CLIENT_ID, user_id)
                videos = self._obtener_todos_los_videos_twitch(self.twitch_token, self.TWITCH_CLIENT_ID, user_id, creador_nombre, usuario_twitch, seguidores, fecha_limite)
                if videos: historico_videos.extend(videos)
            
        if historico_videos:
            self.df_twitch = pd.DataFrame(historico_videos)
        else:
            self.df_twitch = pd.DataFrame()

    # ----------------------------------------------
    # EXTRACCIÓN GENERAL
    # ----------------------------------------------
    def extraction(self):
        self._extraccion_tiktok() 
        self._extraccion_instagram()
        self._extraccion_youtube()
        self._extraccion_twitch()

        print("\n✅ Extracción de redes sociales completada.")
        print("Guardando archivos temporales (Novedades)...")

        # Guardamos solo lo nuevo extraído en los archivos históricos (ahora temporales)
        if not self.df_instagram.empty: self.df_instagram.to_parquet(self.data_path / "historico_instagram.parquet", index=False)
        if not self.df_tiktok.empty: self.df_tiktok.to_parquet(self.data_path / "historico_tiktok.parquet", index=False)
        if not self.df_youtube.empty: self.df_youtube.to_parquet(self.data_path / "historico_youtube.parquet", index=False)
        if not self.df_twitch.empty: self.df_twitch.to_parquet(self.data_path / "historico_twitch.parquet", index=False)

        print("✅ Novedades temporales guardadas.")