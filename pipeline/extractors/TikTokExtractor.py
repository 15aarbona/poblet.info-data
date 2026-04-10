import pandas as pd
import asyncio
from playwright.async_api import async_playwright

from extractors.Extractor import Extractor

class TikTokExtractor(Extractor):
    def __init__(self, config_path="tokens.json", ui_callback=None):
        super().__init__(config_path, ui_callback)

        # Obtener datos de TikTok
        self.df = self._obtener_columna_red_social("tiktok")
        self.df = self.df[10:15]  # Para pruebas rápidas, eliminar en producción

    
    def _crear_interceptor(self, state):
        async def interceptor(response):
            if response.request.resource_type in ["fetch", "xhr"]:
                # TikTok suele usar rutas que contienen /api/item_list/ o /api/post/item_list/
                if "item_list" in response.url or "post/item_list" in response.url:
                    try:
                        data = await response.json()
                        if 'itemList' in data:
                            for video in data['itemList']:
                                autor = video.get('author', {}).get('uniqueId')
                                if autor and str(autor).lower() == state['nick_limpio']:
                                    stats = video.get('stats', {})
                                    autor_stats = video.get('authorStats', {})
                                    
                                    state['videos'].append({
                                        "creador": state['creador'], 
                                        "nick_tiktok": state['nick_orig'],
                                        "seguidores": autor_stats.get('followerCount', 0), 
                                        "fecha_publicacion": pd.to_datetime(video.get('createTime'), unit='s'),
                                        "visualizaciones": stats.get('playCount', 0), # Renombrado para consistencia
                                        "likes": stats.get('diggCount', 0),
                                        "comentarios": stats.get('commentCount', 0), # NUEVO CAMPO
                                        "url_post": f"https://www.tiktok.com/@{state['nick_limpio']}/video/{video.get('id')}", # NUEVO CAMPO
                                        "video_id": video.get('id') 
                                    })
                    except: pass
        return interceptor
    

    async def _extraer_perfil_con_contexto(self, contexto, fila):
        import random
        
        creador_nombre = fila['creador']
        nick_original = fila['nick_tiktok'] 
        nick_limpio = str(nick_original).strip('/').replace('@', '').split('?')[0].lower()
        
        self._reportar_estado("TT", creador_nombre, "start")
        
        state = {'videos': [], 'creador': creador_nombre, 'nick_orig': nick_original, 'nick_limpio': nick_limpio}

        pagina = await contexto.new_page()
        
        await pagina.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.navigator.chrome = { runtime: {} };
        """)
        
        await pagina.route("**/*", self._bloquear_recursos_innecesarios)
        pagina.on("response", self._crear_interceptor(state))
        
        url = f"https://www.tiktok.com/@{nick_limpio}"
        print(f"[TK] Entrant a @{nick_limpio}...")
        
        max_intentos = 3
        cargado_con_exito = False

        await asyncio.sleep(random.uniform(1.0, 3.0))
        try:
            await pagina.goto(url, timeout=15000, wait_until="domcontentloaded")
        except:
            pass 
            
        for intento in range(max_intentos):
            try:
                await pagina.wait_for_selector('h2[data-e2e="user-title"], div[data-e2e="user-post-item"]', timeout=8000)
                
                cargado_con_exito = True
                break 
                
            except Exception:
                if intento < max_intentos - 1:
                    print(f"\t[TK] {creador_nombre} encallat. Intentant saltar bloqueig (Intent {intento + 2}/{max_intentos})...")
                    try:
                        clic_exitoso = await pagina.evaluate('''() => {
                            let botones = Array.from(document.querySelectorAll('button'));
                            let btn = botones.find(b => 
                                b.innerText.includes('Actualizar') || 
                                b.innerText.includes('Try again') || 
                                b.innerText.includes('Reintentar') ||
                                b.innerText.includes('Reload')
                            );
                            if (btn) { 
                                btn.click(); 
                                return true; 
                            }
                            return false;
                        }''')
                        
                        if clic_exitoso:
                            pass
                        else:
                            await pagina.reload(timeout=15000, wait_until="domcontentloaded")
                    except Exception:
                        pass
                    
                    temps_espera = random.uniform(4.5, 8.5)
                    await asyncio.sleep(temps_espera)
                    
                else:
                    print(f"\t[TK] Impossible carregar a {creador_nombre} després de {max_intentos} intents. Botant...")

        if not cargado_con_exito:
            await pagina.close()
            self._reportar_estado("TT", creador_nombre, "done")
            return pd.DataFrame()
        
        altura_anterior = await pagina.evaluate("document.body.scrollHeight")
        intentos_sin_bajar = 0

        for i in range(100):
            await asyncio.sleep(random.uniform(0.5, 1.5))
            await pagina.evaluate("window.scrollBy(0, 1500)")
            
            videos_antes = len(state['videos'])
            for _ in range(30):
                await asyncio.sleep(0.1)
                if len(state['videos']) > videos_antes: break

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
            print(f"\t[TK] {len(df)} vídeos NOUS de {creador_nombre}")
            
        self._reportar_estado("TT", creador_nombre, "done")
        return df
    

    async def _extraccion(self):
        print(f"\nIniciant extracció de TikTok...\n")
        todos_los_datos = []
        
        self._reportar_estado("TT", "", "init", total=len(self.df))
        
        async with async_playwright() as p:
            contexto = await p.chromium.launch_persistent_context(
                user_data_dir=str(self.repo_root / "tiktok_session"), 
                headless=False, 
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            
            for _, fila in self.df.iterrows():
                df_res = await self._extraer_perfil_con_contexto(contexto, fila)
                
                if df_res is not None and not df_res.empty: 
                    todos_los_datos.append(df_res)
                    try: pd.concat(todos_los_datos, ignore_index=True).to_parquet(self.data_path / "historico_tiktok.parquet", index=False)
                    except: pass
                
                import random
                await asyncio.sleep(random.uniform(2.5, 5.0))
                
            await contexto.close()
        
        # Suponiendo que tu dataframe final se llama self.df o lo tienes en una variable
        if not self.df.empty:
            # Convertimos el timestamp UNIX (segundos) a fecha normal
            self.df['fecha_publicacion'] = pd.to_datetime(self.df['fecha_publicacion'], unit='s', errors='coerce')
            
            # Opcional: quitar la zona horaria para que cuadre con el resto
            self.df['fecha_publicacion'] = self.df['fecha_publicacion'].dt.tz_localize(None)


        self.df = pd.concat(todos_los_datos, ignore_index=True) if todos_los_datos else pd.DataFrame()
        print(f"\n🎉 TIKTOK COMPLETAT.")