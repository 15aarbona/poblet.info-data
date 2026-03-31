import pandas as pd
import requests

from src.extractors.Extractor import Extractor

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


    def _extraccion(self):
        print(f"\nIniciant extracció de Twitch...\n")
        self._reportar_estado("YT", "", "init", total=len(self.df_youtube))

        historico_videos = []
        headers = {'Client-ID': self.TWITCH_CLIENT_ID, 'Authorization': f'Bearer {self.twitch_token}'}
        
        for c, row in self.df_twitch.iterrows():
            creador_nombre = row.get('creador')
            usuario_twitch = row.get('nick_twitch') 
            ids_conocidos = self.ids_cache_tw.get(usuario_twitch, set())
            
            self._reportar_estado("TW", creador_nombre, "start")
            print(f"\t[TW] Extraient: {usuario_twitch}...")

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
                            if ids_conocidos and str(v['id']) in ids_conocidos:
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

            if historico_videos:
                try: pd.DataFrame(historico_videos).to_parquet(self.data_path / "historico_twitch.parquet", index=False)
                except: pass
            
            self._reportar_estado("TW", creador_nombre, "done")
            
        self.df_twitch = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        print(f"\n🎉 TWITCH COMPLETAT.")