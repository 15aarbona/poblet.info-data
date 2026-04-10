import pandas as pd
import requests
import asyncio

from extractors.Extractor import Extractor

class TwitchExtractor(Extractor):
    def __init__(self, config_path="tokens.json", ui_callback=None):
        super().__init__(config_path, ui_callback)

        # Obtener datos de Twitch
        self.df = self._obtener_columna_red_social("twitch")
        
        # Función auxiliar para obtener el token de Twitch
        def obtener_token_twitch(client_id: str, client_secret: str) -> str:
            url = "https://id.twitch.tv/oauth2/token"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'
            }
            response = requests.post(url, params=params)
            return response.json().get('access_token')
        
        # Obtener el token de Twitch
        self.TWITCH_CLIENT_ID = self.config.get('TWITCH_CLIENT_ID')
        TWITCH_CLIENT_SECRET = self.config.get('TWITCH_CLIENT_SECRET')
        self.twitch_token = obtener_token_twitch(self.TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)


    def _extraccion_sync(self):
        print(f"\nIniciant extracció de Twitch...\n")
        self._reportar_estado("TW", "", "init", total=len(self.df))

        historico_videos = []
        headers = {'Client-ID': self.TWITCH_CLIENT_ID, 'Authorization': f'Bearer {self.twitch_token}'}
        
        for c, row in self.df.iterrows():
            creador_nombre = row.get('creador')
            usuario_twitch = row.get('nick_twitch') 
            
            self._reportar_estado("TW", creador_nombre, "start")
            print(f"\t[TW] Extraient: {usuario_twitch}...")

            login_clean = str(usuario_twitch).strip().replace('@', '')
            try:
                # 1. Obtener ID del usuario
                user_info = requests.get(f"https://api.twitch.tv/helix/users?login={login_clean}", headers=headers).json().get('data', [])
                if not user_info: 
                    self._reportar_estado("TW", creador_nombre, "done")
                    continue

                user_id = user_info[0].get('id')
                
                # 2. Obtener Seguidores actuales
                seguidores = requests.get(f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}", headers=headers).json().get('total', 0)
                
                # === NUEVO: 3. Comprobar si está en DIRECTO ahora mismo ===
                url_stream = f"https://api.twitch.tv/helix/streams?user_id={user_id}"
                res_stream = requests.get(url_stream, headers=headers).json()
                
                if 'data' in res_stream and len(res_stream['data']) > 0:
                    stream_data = res_stream['data'][0]
                    historico_videos.append({
                        "creador": creador_nombre, 
                        "nick_twitch": usuario_twitch, 
                        "seguidores": seguidores,
                        "id_video": stream_data.get('id'), 
                        "titulo": stream_data.get('title'), 
                        "fecha_publicacion": stream_data.get('started_at'),
                        "tipo_publicacion": "DIRECTO", # Clasificación según tu esquema
                        "visualizaciones": stream_data.get('viewer_count', 0), # Espectadores concurrentes
                        "url_post": f"https://www.twitch.tv/{login_clean}"
                    })
                    print(f"\t[TW] 🔴 {usuario_twitch} està en DIRECTE amb {stream_data.get('viewer_count')} espectadors!")

                # 4. Obtener histórico de vídeos RESUBIDOS (VODs)
                cursor, detener = None, False
                while not detener:
                    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first=100"
                    if cursor: url += f"&after={cursor}"

                    response = requests.get(url, headers=headers)

                    if response.status_code != 200: break
                        
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        for v in data['data']:
                            historico_videos.append({
                                "creador": creador_nombre, 
                                "nick_twitch": usuario_twitch, 
                                "seguidores": seguidores,
                                "id_video": v['id'], 
                                "titulo": v['title'], 
                                "fecha_publicacion": v['created_at'],
                                "tipo_publicacion": "RESUBIDO", # Clasificación según tu esquema
                                "visualizaciones": v.get('view_count', 0), 
                                "url_post": v.get('url')
                            })

                        cursor = data.get('pagination', {}).get('cursor')
                        if not cursor: break
                    else: break

            except Exception as e: 
                print(f"\t[TW] Error amb {usuario_twitch}: {e}")

            if historico_videos:
                try: pd.DataFrame(historico_videos).to_parquet(self.data_path / "historico_twitch.parquet", index=False)
                except: pass
            
            self._reportar_estado("TW", creador_nombre, "done")
            
        self.df = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        
        # Limpieza de fechas para evitar problemas al guardar o exportar a Excel
        if not self.df.empty:
            self.df['fecha_publicacion'] = pd.to_datetime(self.df['fecha_publicacion'], utc=True).dt.tz_localize(None)

        print(f"\n🎉 TWITCH COMPLETAT.")
        
    async def _extraccion(self):
        """Envoltorio asíncrono para ejecutar la función síncrona sin bloquear la UI"""
        return await asyncio.to_thread(self._extraccion_sync)