import pandas as pd
import requests
import time
from pathlib import Path
import json
import re
import asyncio
from playwright.async_api import async_playwright

from src.extractors.Extractor import Extractor

class InstagramExtractor(Extractor):
    def __init__(self, config_path="tokens.json", ui_callback=None):
        super().__init__(config_path, ui_callback)

        # Obtener datos de Instagram
        self.df = self._obtener_columna_red_social("instagram")

    
    def _registrar_anuncio(self, data, state):
        if isinstance(data, dict):
            if 'shortcode' in data and data['shortcode']: state['blacklist'].add(data['shortcode'])
            if 'code' in data and data['code']: state['blacklist'].add(data['code'])
            for v in data.values(): self._registrar_anuncio(v, state)
        elif isinstance(data, list):
            for item in data: self._registrar_anuncio(item, state)

    
    def _parsear_nodo(self, node, state):
        likes = node.get("edge_media_preview_like", {}).get("count", 0) if isinstance(node.get("edge_media_preview_like", {}), dict) else 0
        comentarios = node.get("edge_media_to_comment", {}).get("count", 0) if isinstance(node.get("edge_media_to_comment", {}), dict) else 0
        return {
            "creador": state['creador'], "nick_instagram": state['nick_orig'], "seguidores": state['followers'],
            "post_id": node.get("shortcode"), "fecha_publicacion": pd.to_datetime(node.get("taken_at_timestamp"), unit='s') if node.get("taken_at_timestamp") else None,
            "tipo_publicacion": node.get("__typename"), "likes": likes, "comentarios": comentarios,
            "url_post": f"https://www.instagram.com/p/{node.get('shortcode')}/" if node.get("shortcode") else None
        }
    
    
    def _parsear_item(self, item, state):
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
                    return 
                
                if es_formato_1: state['posts'].append(self._parsear_nodo(data, state))
                else: state['posts'].append(self._parsear_item(data, state))
                return 

            for valor in data.values(): self._buscar_posts_en_json(valor, state)
        elif isinstance(data, list):
            for item in data: self._buscar_posts_en_json(item, state)

    
    def _crear_interceptor(self, state):
        async def interceptor(response):
            if response.request.resource_type in ["fetch", "xhr"]:
                if "graphql" in response.url or "api/v1" in response.url:
                    try: data = await response.json(); self._buscar_posts_en_json(data, state)
                    except: pass
        return interceptor
    

    async def _extraer_perfil_con_contexto(self, contexto, fila):
        creador_nombre = fila['creador']
        nick_original = fila['nick_instagram']
        nick_limpio = str(nick_original).strip('/').split('?')[0]
        ids_conocidos = self.ids_cache_ig.get(nick_original, set())
        
        self._reportar_estado("IG", creador_nombre, "start")
        
        state = {
            'posts': [], 'blacklist': set(), 'followers': 0, 
            'creador': creador_nombre, 'nick_orig': nick_original, 'nick_limpio': nick_limpio.lower()
        }

        try:
            pagina = await contexto.new_page()
            await pagina.route("**/*", self._bloquear_recursos_innecesarios)
            pagina.on("response", self._crear_interceptor(state))

            url = f"https://www.instagram.com/{nick_limpio}/"
            print(f"[IG] Entrant a @{nick_limpio}...")
            
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
            except Exception as e:
                print(f"[IG] Error al entrar a {nick_limpio}: {e}")

            altura_anterior = await pagina.evaluate("document.body.scrollHeight")
            intentos_sin_bajar = 0

            for i in range(100):
                await pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                
                posts_antes = len(state['posts'])
                for _ in range(25):
                    await asyncio.sleep(0.1)
                    if len(state['posts']) > posts_antes: break
                
                if ids_conocidos and state['posts']:
                    ids_actuales = {str(p.get('post_id')) for p in state['posts'] if p.get('post_id')}
                    if ids_actuales.intersection(ids_conocidos): 
                        break 

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
                if ids_conocidos: df = df[~df['post_id'].astype(str).isin(ids_conocidos)]                
                df = df.sort_values(by=['likes', 'comentarios'], ascending=[False, False]).drop_duplicates(subset=['post_id'], keep='first')
                print(f"\t[IG] {len(df)} posts NOUS de {creador_nombre}")
                
            self._reportar_estado("IG", creador_nombre, "done")
            return df

        except Exception as e:
            print(f"\t[IG] Error extraient a {creador_nombre}: {str(e)}")
            
            try:
                await pagina.close()
            except:
                pass
                
            self._reportar_estado("IG", creador_nombre, "done")
            return pd.DataFrame()
    

    async def _extraccion(self):
        print(f"\n🚀 Iniciant extracció d'Instagram...\n")
        todos_los_datos = []
        
        self._reportar_estado("IG", "", "init", total=len(self.df_instagram))
        
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir="./instagram_session", headless=False, viewport={'width': 1920, 'height': 1080},
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            
            semaforo = asyncio.Semaphore(4)
            
            async def procesar_con_semaforo(fila):
                async with semaforo:
                    df_res = await self._extraer_perfil_con_contexto(contexto, fila)
                    if df_res is not None and not df_res.empty:
                        todos_los_datos.append(df_res)
                        try: pd.concat(todos_los_datos, ignore_index=True).to_parquet(self.data_path / "historico_instagram.parquet", index=False)
                        except: pass
                    return df_res

            tareas = [asyncio.create_task(procesar_con_semaforo(fila)) for _, fila in self.df_instagram.iterrows()]
            resultados = await asyncio.gather(*tareas)
            
            for df_res in resultados:
                if df_res is not None and not df_res.empty:
                    todos_los_datos.append(df_res)
                
            await contexto.close()

        self.df_instagram = pd.concat(todos_los_datos, ignore_index=True) if todos_los_datos else pd.DataFrame()
        print(f"\n🎉 INSTAGRAM COMPLETAT.")