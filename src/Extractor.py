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
    def __init__(self, config_path="tokens.json", ui_callback=None):
        self.ui_callback = ui_callback
        
        def obtener_token_twitch(client_id: str, client_secret: str) -> str:
            url = "https://id.twitch.tv/oauth2/token"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'
            }
            response = requests.post(url, params=params)
            return response.json().get('access_token')
        
        self.data_path = Path().cwd() / "data"

        with open(Path().cwd() / config_path) as f:
            config = json.load(f)
            self.TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
            TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
            self.twitch_token = obtener_token_twitch(self.TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)

        self.df_creadors = pd.read_parquet(self.data_path / "creadors_poblet.parquet")
        self.df_instagram = self._obtener_columna_red_social("instagram")
        self.df_tiktok = self._obtener_columna_red_social("tiktok")
        self.df_youtube = self._obtener_columna_red_social("youtube")
        self.df_twitch = self._obtener_columna_red_social("twitch")

        self.df_instagram = self.df_instagram.iloc[15:16]
        self.df_tiktok = self.df_tiktok.iloc[15:16]
        self.df_youtube = self.df_youtube.iloc[3:4]
        self.df_twitch = self.df_twitch.iloc[0:1]

        self.fechas_cache_ig = self._obtener_fechas_cache("instagram", "nick_instagram")
        self.fechas_cache_tk = self._obtener_fechas_cache("tiktok", "nick_tiktok")
        self.fechas_cache_yt = self._obtener_fechas_cache("youtube", "nick_youtube")
        self.fechas_cache_tw = self._obtener_fechas_cache("twitch", "nick_twitch")

    def _reportar_estado(self, red, creador, accion):
        if self.ui_callback:
            self.ui_callback(red, creador, accion)

    def _obtener_columna_red_social(self, red_social: str) -> pd.DataFrame:
        df = self.df_creadors[["creador", f"nick_{red_social}"]].copy()
        df = df.dropna(subset=[f"nick_{red_social}"])
        return df

    def _obtener_fechas_cache(self, red_social: str, col_nick: str) -> dict:
        archivo = self.data_path / f"clean_{red_social}.parquet"
        if archivo.exists():
            try:
                df = pd.read_parquet(archivo)
                if not df.empty and col_nick in df.columns and 'fecha_publicacion' in df.columns:
                    df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True, errors='coerce').dt.tz_localize(None)
                    return df.groupby(col_nick)['fecha_publicacion'].max().to_dict()
            except Exception:
                pass
        return {}

    async def _bloquear_recursos_innecesarios(self, route):
        url = route.request.url.lower()
        tipo = route.request.resource_type
        trackers = ["googleads", "doubleclick", "analytics", "tiktok.com/log/", "facebook.com/tr/", "pixel", "adsystem"]
        if any(t in url for t in trackers):
            await route.abort()
            return
        if tipo in ["font", "stylesheet", "image", "media", "imageset"]:
            await route.abort()
            return
        await route.continue_()

    def _registrar_anuncio(self, data, state):
        if isinstance(data, dict):
            if 'shortcode' in data and data['shortcode']: state['blacklist'].add(data['shortcode'])
            if 'code' in data and data['code']: state['blacklist'].add(data['code'])
            for v in data.values(): self._registrar_anuncio(v, state)
        elif isinstance(data, list):
            for item in data: self._registrar_anuncio(item, state)

    def _parsear_nodo_ig(self, node, state):
        likes = node.get("edge_media_preview_like", {}).get("count", 0) if isinstance(node.get("edge_media_preview_like", {}), dict) else 0
        comentarios = node.get("edge_media_to_comment", {}).get("count", 0) if isinstance(node.get("edge_media_to_comment", {}), dict) else 0
        return {
            "creador": state['creador'], "nick_instagram": state['nick_orig'], "seguidores": state['followers'],
            "post_id": node.get("shortcode"), "fecha_publicacion": pd.to_datetime(node.get("taken_at_timestamp"), unit='s') if node.get("taken_at_timestamp") else None,
            "tipo_publicacion": node.get("__typename"), "likes": likes, "comentarios": comentarios,
            "url_post": f"https://www.instagram.com/p/{node.get('shortcode')}/" if node.get("shortcode") else None
        }

    def _parsear_item_ig(self, item, state):
        return {
            "creador": state['creador'], "nick_instagram": state['nick_orig'], "seguidores": state['followers'],
            "post_id": item.get("code"), "fecha_publicacion": pd.to_datetime(item.get("taken_at"), unit='s') if item.get("taken_at") else None,
            "tipo_publicacion": item.get("media_type"), "likes": item.get("like_count", 0), "comentarios": item.get("comment_count", 0),
            "url_post": f"https://www.instagram.com/p/{item.get('code')}/" if item.get("code") else None
        }

    def _buscar_posts_en_json(self, data, state):
        if isinstance(data, dict):
            if 'edge_followed_by' in data and isinstance(data['edge_followed_by'], dict) and 'count' in data['edge_followed_by']:
                state['followers'] = data['edge_followed_by']['count']

            ramas_prohibidas = ['injected', 'injected_items', 'ad_items', 'sponsored_items', 'xdt_injected_items', 'sponsored_feed_item']
            for clave in list(data.keys()):
                if str(clave).lower() in ramas_prohibidas:
                    self._registrar_anuncio(data[clave], state)
                    del data[clave]

            es_anuncio = False
            if str(data.get('is_ad')).lower() == 'true': es_anuncio = True
            if str(data.get('is_sponsored')).lower() == 'true': es_anuncio = True
            campos_sospechosos = ['ad_id', 'sponsored_label_info', 'ad_action', 'tracker_token', 'ad_metadata', 'about_ad_params', 'hide_reasons_v2']
            if any(data.get(c) is not None for c in campos_sospechosos) or data.get('__typename') in ['GraphAd', 'GraphSponsoredMedia', 'XDTGraphAd']:
                es_anuncio = True

            if es_anuncio:
                self._registrar_anuncio(data, state)
                return

            es_formato_1 = 'shortcode' in data and 'edge_media_preview_like' in data
            es_formato_2 = 'code' in data and 'like_count' in data
            
            if es_formato_1 or es_formato_2:
                nicks = []
                for k in ['owner', 'user']:
                    if isinstance(data.get(k), dict): nicks.append(data[k].get('username'))
                    elif isinstance(data.get(k), str): nicks.append(data.get(k))
                try: nicks.append(data['edge_media_to_caption']['edges'][0]['node']['user']['username'])
                except: pass
                try: nicks.append(data['caption']['user']['username'])
                except: pass
                permalink = data.get('permalink')
                if isinstance(permalink, str) and '/' in permalink:
                    p = [x for x in permalink.strip('/').split('/') if x]
                    if len(p) > 1 and p[1] == 'p': nicks.append(p[0])
                if 'coauthor_producers' in data and isinstance(data['coauthor_producers'], list):
                    for c in data['coauthor_producers']:
                        if isinstance(c, dict) and 'username' in c: nicks.append(c['username'])

                nicks = [str(n).lower().strip() for n in nicks if n]
                if nicks and state['nick_limpio'] not in nicks:
                    self._registrar_anuncio(data, state)
                    print(f"   🛡️ [ESCUT NICK] Anunci caçat (Pertany a {nicks[0] if nicks else 'desconegut'}). Afegit a Llista Negra.")
                    return 
                
                if es_formato_1: state['posts'].append(self._parsear_nodo_ig(data, state))
                else: state['posts'].append(self._parsear_item_ig(data, state))
                return 

            for valor in data.values(): self._buscar_posts_en_json(valor, state)
        elif isinstance(data, list):
            for item in data: self._buscar_posts_en_json(item, state)

    def _crear_interceptor_ig(self, state):
        async def interceptor(response):
            if response.request.resource_type in ["fetch", "xhr"]:
                if "graphql" in response.url or "api/v1" in response.url:
                    try: data = await response.json(); self._buscar_posts_en_json(data, state)
                    except: pass
        return interceptor

    async def _extraer_perfil_instagram_con_contexto(self, contexto, fila):
        creador_nombre = fila['creador']
        nick_original = fila['nick_instagram']
        nick_limpio = str(nick_original).strip('/').split('?')[0]
        fecha_limite = self.fechas_cache_ig.get(nick_original)
        
        self._reportar_estado("IG", creador_nombre, "start")
        
        state = {
            'posts': [], 'blacklist': set(), 'followers': 0, 
            'creador': creador_nombre, 'nick_orig': nick_original, 'nick_limpio': nick_limpio.lower()
        }

        pagina = await contexto.new_page()
        await pagina.route("**/*", self._bloquear_recursos_innecesarios)
        pagina.on("response", self._crear_interceptor_ig(state))

        url = f"https://www.instagram.com/{nick_limpio}/"
        print(f"🌍 [IG] Entrant a @{nick_limpio}...")
        try: 
            await pagina.goto(url, timeout=20000)
            await pagina.wait_for_selector("header", timeout=10000)
            
            texto_cabecera = await pagina.locator("header").inner_text()
            match = re.search(r'([\d\.,MKmk]+)\s*(seguidores|followers|mil seguidores)', texto_cabecera.replace('\n', ' '), re.IGNORECASE)
            if match and state['followers'] == 0:
                tl = str(match.group(1)).upper().replace(' ', '')
                if 'M' in tl: state['followers'] = int(float(tl.replace('M', '').replace(',', '.')) * 1000000)
                elif 'K' in tl: state['followers'] = int(float(tl.replace('K', '').replace(',', '.')) * 1000)
                else: state['followers'] = int(''.join(c for c in tl if c.isdigit()) or 0)
        except: pass

        altura_anterior = await pagina.evaluate("document.body.scrollHeight")
        intentos_sin_bajar = 0

        for i in range(50):
            await pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            posts_antes = len(state['posts'])
            for _ in range(25):
                await asyncio.sleep(0.1)
                if len(state['posts']) > posts_antes: break
            
            if fecha_limite and state['posts']:
                fechas = [pd.to_datetime(p['fecha_publicacion'], utc=True).tz_localize(None) for p in state['posts'] if pd.notna(p['fecha_publicacion'])]
                if fechas and min(fechas) <= fecha_limite: break

            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 3: break
            else:
                intentos_sin_bajar = 0
                altura_anterior = altura_actual
        
        await pagina.close()
        
        df = pd.DataFrame(state['posts'])
        if not df.empty:
            df = df.dropna(subset=['post_id'])
            df = df[~df['post_id'].isin(state['blacklist'])]
            df['seguidores'] = state['followers']
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
            if fecha_limite: df = df[df['fecha_publicacion'] > fecha_limite] 
            df = df.sort_values(by=['likes', 'comentarios'], ascending=[False, False]).drop_duplicates(subset=['post_id'], keep='first')
            print(f"   ✅ [IG] {len(df)} posts NOUS de {creador_nombre}")
            
        self._reportar_estado("IG", creador_nombre, "done")
        return df

    async def _ejecutar_extraccion_instagram(self):
        print(f"\n🚀 Iniciant extracció d'Instagram...\n")
        todos_los_datos = []
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir="./instagram_session", headless=False, viewport={'width': 1920, 'height': 1080},
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            
            chunk_size = 3
            for i in range(0, len(self.df_instagram), chunk_size):
                chunk = self.df_instagram.iloc[i:i+chunk_size]
                tareas = [self._extraer_perfil_instagram_con_contexto(contexto, fila) for _, fila in chunk.iterrows()]
                resultados = await asyncio.gather(*tareas)
                for df_res in resultados:
                    if not df_res.empty: todos_los_datos.append(df_res)
                
            await contexto.close()

        self.df_instagram = pd.concat(todos_los_datos, ignore_index=True) if todos_los_datos else pd.DataFrame()
        print(f"\n🎉 INSTAGRAM COMPLETAT.")

    def _crear_interceptor_tk(self, state):
        async def interceptor(response):
            if response.request.resource_type in ["fetch", "xhr"]:
                try:
                    data = await response.json()
                    if 'itemList' in data:
                        for video in data['itemList']:
                            autor = video.get('author', {}).get('uniqueId')
                            if autor and str(autor).lower() == state['nick_limpio']:
                                stats = video.get('stats', {})
                                autor_stats = video.get('authorStats', {})
                                state['videos'].append({
                                    "creador": state['creador'], "nick_tiktok": state['nick_orig'],
                                    "seguidores": autor_stats.get('followerCount', 0), 
                                    "fecha_publicacion": pd.to_datetime(video.get('createTime'), unit='s'),
                                    "vistas": stats.get('playCount', 0), "likes": stats.get('diggCount', 0),
                                    "video_id": video.get('id') 
                                })
                except: pass
        return interceptor

    async def _extraer_perfil_tiktok_con_contexto(self, contexto, fila):
        creador_nombre = fila['creador']
        nick_original = fila['nick_tiktok'] 
        nick_limpio = str(nick_original).strip('/').replace('@', '').split('?')[0].lower()
        fecha_limite = self.fechas_cache_tk.get(nick_original)
        
        self._reportar_estado("TT", creador_nombre, "start")
        
        state = {'videos': [], 'creador': creador_nombre, 'nick_orig': nick_original, 'nick_limpio': nick_limpio}

        pagina = await contexto.new_page()
        await pagina.route("**/*", self._bloquear_recursos_innecesarios)
        pagina.on("response", self._crear_interceptor_tk(state))
        
        print(f"🌍 [TK] Entrant a @{nick_limpio}...")
        try: await pagina.goto(f"https://www.tiktok.com/@{nick_limpio}", timeout=15000)
        except: pass
        
        altura_anterior = await pagina.evaluate("document.body.scrollHeight")
        intentos_sin_bajar = 0

        for i in range(50):
            await pagina.evaluate("window.scrollBy(0, 1500)")
            
            videos_antes = len(state['videos'])
            for _ in range(30):
                await asyncio.sleep(0.1)
                if len(state['videos']) > videos_antes: break

            if fecha_limite and state['videos']:
                fechas = [pd.to_datetime(v['fecha_publicacion'], utc=True).tz_localize(None) for v in state['videos'] if pd.notna(v['fecha_publicacion'])]
                if fechas and min(fechas) <= fecha_limite: break

            altura_actual = await pagina.evaluate("document.body.scrollHeight")
            if altura_actual == altura_anterior:
                intentos_sin_bajar += 1
                if intentos_sin_bajar >= 4: break 
            else:
                intentos_sin_bajar = 0 
                altura_anterior = altura_actual
                
        await pagina.close()
        
        df = pd.DataFrame(state['videos'])
        if not df.empty:
            df = df.dropna(subset=['video_id']).drop_duplicates(subset=['video_id'])
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], utc=True).dt.tz_localize(None)
            if fecha_limite: df = df[df['fecha_publicacion'] > fecha_limite]
            print(f"   ✅ [TK] {len(df)} vídeos NOUS de {creador_nombre}")
            
        self._reportar_estado("TT", creador_nombre, "done")
        return df

    async def _ejecutar_extraccion_tiktok(self):
        print(f"\n🚀 Iniciant extracció de TikTok...\n")
        todos_los_datos = []
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir="./tiktok_session", headless=False, viewport={'width': 1920, 'height': 1080},
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            
            chunk_size = 3
            for i in range(0, len(self.df_tiktok), chunk_size):
                chunk = self.df_tiktok.iloc[i:i+chunk_size]
                tareas = [self._extraer_perfil_tiktok_con_contexto(contexto, fila) for _, fila in chunk.iterrows()]
                resultados = await asyncio.gather(*tareas)
                for df_res in resultados:
                    if not df_res.empty: todos_los_datos.append(df_res)
                
            await contexto.close()

        self.df_tiktok = pd.concat(todos_los_datos, ignore_index=True) if todos_los_datos else pd.DataFrame()
        print(f"\n🎉 TIKTOK COMPLETAT.")

    def _extraccion_youtube(self):
        print(f"\n▶️ Iniciant extracció de YouTube...\n")
        historico_videos = []
        for c, row in self.df_youtube.iterrows():
            creador_nombre = row.get('creador')
            usuario_yt = row.get('nick_youtube') 
            fecha_limite = self.fechas_cache_yt.get(usuario_yt)
            
            self._reportar_estado("YT", creador_nombre, "start")
            print(f"[YT] Extraient: {usuario_yt}...")
            handle_str = str(usuario_yt).strip()
            if not handle_str.startswith('@'): handle_str = f"@{handle_str}"
                
            ydl_opts_rapido = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
            try:
                with yt_dlp.YoutubeDL(ydl_opts_rapido) as ydl:
                    info_canal = ydl.extract_info(f"https://www.youtube.com/{handle_str}/videos", download=False)
                    if not info_canal or 'entries' not in info_canal: continue
                    lista_videos = info_canal['entries']
                    subscriptores = info_canal.get('channel_follower_count') or info_canal.get('subscriber_count', 0)
            except Exception: 
                self._reportar_estado("YT", creador_nombre, "done")
                continue

            ydl_opts_profundo = {'quiet': True, 'no_warnings': True, 'ignoreerrors': True}
            with yt_dlp.YoutubeDL(ydl_opts_profundo) as ydl:
                for video_breve in lista_videos:
                    url_video = video_breve.get('url')
                    if not url_video: continue
                    try:
                        video = ydl.extract_info(url_video, download=False)
                        if video:
                            fecha_pub_str = video.get('upload_date')
                            if fecha_pub_str:
                                fecha_pub = pd.to_datetime(fecha_pub_str, format='%Y%m%d', errors='coerce')
                                if fecha_limite and pd.notna(fecha_pub) and fecha_pub <= fecha_limite: break

                            historico_videos.append({
                                "creador": creador_nombre, "nick_youtube": usuario_yt, "seguidores": subscriptores or 0,
                                "id_video": video.get('id'), "titulo": video.get('title'), "url_video": video.get('webpage_url'),
                                "fecha_publicacion": fecha_pub_str, "visualizaciones": video.get('view_count', 0) or 0,
                                "likes": video.get('like_count', 0) or 0, "duracion_segundos": video.get('duration')
                            })
                    except Exception: pass 
            
            self._reportar_estado("YT", creador_nombre, "done")
            
        self.df_youtube = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        print(f"\n🎉 YOUTUBE COMPLETAT.")
    
    def _extraccion_twitch(self):
        print(f"\n🎮 Iniciant extracció de Twitch...\n")
        historico_videos = []
        headers = {'Client-ID': self.TWITCH_CLIENT_ID, 'Authorization': f'Bearer {self.twitch_token}'}
        
        for c, row in self.df_twitch.iterrows():
            creador_nombre = row.get('creador')
            usuario_twitch = row.get('nick_twitch') 
            fecha_limite = self.fechas_cache_tw.get(usuario_twitch)
            
            self._reportar_estado("TW", creador_nombre, "start")
            print(f"[TW] Extraient: {usuario_twitch}...")
            login_clean = str(usuario_twitch).strip().replace('@', '')
            try:
                user_info = requests.get(f"https://api.twitch.tv/helix/users?login={login_clean}", headers=headers).json().get('data', [])
                if not user_info: 
                    self._reportar_estado("TW", creador_nombre, "done")
                    continue
                user_id = user_info[0].get('id')
                
                seguidores = requests.get(f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}", headers=headers).json().get('total', 0)
                
                cursor, detener = None, False
                while not detener:
                    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first=100"
                    if cursor: url += f"&after={cursor}"
                    response = requests.get(url, headers=headers)
                    if response.status_code != 200: break
                        
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        for v in data['data']:
                            fecha_pub = pd.to_datetime(v['created_at'], utc=True).tz_localize(None)
                            if fecha_limite and fecha_pub <= fecha_limite:
                                detener = True
                                break
                            historico_videos.append({
                                "creador": creador_nombre, "nick_twitch": usuario_twitch, "seguidores": seguidores,
                                "id_video": v['id'], "titulo": v['title'], "fecha_publicacion": v['created_at'],
                                "visualizaciones": v['view_count'], "duracion": v['duration'], "tipo": v['type'] 
                            })
                        cursor = data.get('pagination', {}).get('cursor')
                        if not cursor: break
                    else: break
            except Exception: pass
            
            self._reportar_estado("TW", creador_nombre, "done")
            
        self.df_twitch = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        print(f"\n🎉 TWITCH COMPLETAT.")

    def _extraccion_instagram(self):
        if not self.df_instagram.empty:
            asyncio.run(self._ejecutar_extraccion_instagram())
            print("\n✅ Novetats temporals guardades.")
            self.df_instagram.to_parquet(self.data_path / "historico_instagram.parquet", index=False)

    def _extraccion_tiktok(self):
        if not self.df_tiktok.empty:
            asyncio.run(self._ejecutar_extraccion_tiktok())
            print("\n✅ Novetats temporals guardades.")
            self.df_tiktok.to_parquet(self.data_path / "historico_tiktok.parquet", index=False)

    def extraction(self):
        async def main_pipeline():
            print("\n=======================================================")
            print("🚀 EXECUCIÓ DINÀMICA EN PARAL·LEL (Màx 2 tasques simultànies)")
            print("=======================================================")
            
            pendientes = []
            if not self.df_instagram.empty:
                pendientes.append(("Instagram", self._ejecutar_extraccion_instagram()))
            if not self.df_youtube.empty:
                pendientes.append(("YouTube", asyncio.to_thread(self._extraccion_youtube)))
            if not self.df_tiktok.empty:
                pendientes.append(("TikTok", self._ejecutar_extraccion_tiktok()))
            if not self.df_twitch.empty:
                pendientes.append(("Twitch", asyncio.to_thread(self._extraccion_twitch)))
                
            tareas_activas = set()
            
            while pendientes and len(tareas_activas) < 2:
                nombre, corrutina = pendientes.pop(0)
                tarea = asyncio.create_task(corrutina, name=nombre)
                tareas_activas.add(tarea)
                print(f"▶️ [SISTEMA] Arrancant extracció de {nombre}...")

            while tareas_activas:
                hechas, tareas_activas = await asyncio.wait(tareas_activas, return_when=asyncio.FIRST_COMPLETED)
                
                for tarea in hechas:
                    try:
                        await tarea
                        print(f"✅ [SISTEMA] Extracció de {tarea.get_name()} finalitzada. Espai alliberat.")
                    except Exception as e:
                        print(f"❌ [SISTEMA] Error en {tarea.get_name()}: {e}")
                
                while pendientes and len(tareas_activas) < 2:
                    nombre, corrutina = pendientes.pop(0)
                    nueva_tarea = asyncio.create_task(corrutina, name=nombre)
                    tareas_activas.add(nueva_tarea)
                    print(f"▶️ [SISTEMA] Espai lliure ocupat. Arrancant extracció de {nombre}...")

        asyncio.run(main_pipeline())

        print("\n✅ Extracció dinàmica de xarxes socials completada.")
        print("Guardant arxius temporals (Novetats)...")

        if not self.df_instagram.empty: self.df_instagram.to_parquet(self.data_path / "historico_instagram.parquet", index=False)
        if not self.df_tiktok.empty: self.df_tiktok.to_parquet(self.data_path / "historico_tiktok.parquet", index=False)
        if not self.df_youtube.empty: self.df_youtube.to_parquet(self.data_path / "historico_youtube.parquet", index=False)
        if not self.df_twitch.empty: self.df_twitch.to_parquet(self.data_path / "historico_twitch.parquet", index=False)

        print("✅ Novetats temporals guardades.")