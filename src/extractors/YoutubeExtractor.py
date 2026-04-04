import pandas as pd
import yt_dlp
import time 

from extractors.Extractor import Extractor

class YoutubeExtractor(Extractor):
    def __init__(self, config_path="tokens.json", ui_callback=None):
        super().__init__(config_path, ui_callback)

        # Obtener datos de YouTube
        self.df = self._obtener_columna_red_social("youtube")

    
    def _extraccion(self):
        print(f"\n▶Iniciant extracció de YouTube...\n")
        self._reportar_estado("YT", "", "init", total=len(self.df_youtube)) 
        
        historico_videos = []
        for c, row in self.df_youtube.iterrows():
            creador_nombre = row.get('creador')
            usuario_yt = row.get('nick_youtube') 
                        
            self._reportar_estado("YT", creador_nombre, "start")
            print(f"[YT] Extraient: {usuario_yt}...")

            handle_str = str(usuario_yt).strip()
            if not handle_str.startswith('@'): handle_str = f"@{handle_str}"
                
            ydl_opts_rapido = {
                'quiet': True, 'no_warnings': True, 'extract_flat': True,
                'sleep_interval_requests': 1,
                'sleep_interval': 2,
                'max_sleep_interval': 4
            }

            try:
                with yt_dlp.YoutubeDL(ydl_opts_rapido) as ydl:
                    info_canal = ydl.extract_info(f"https://www.youtube.com/{handle_str}/videos", download=False)
                    if not info_canal or 'entries' not in info_canal: continue
                    lista_videos = info_canal['entries']
                    subscriptores = info_canal.get('channel_follower_count') or info_canal.get('subscriber_count', 0)
            except Exception: 
                self._reportar_estado("YT", creador_nombre, "done")
                continue

            ydl_opts_profundo = {
                'quiet': True, 'no_warnings': True, 'ignoreerrors': True,
                'sleep_interval_requests': 2, 
                'sleep_interval': 4,          
                'max_sleep_interval': 8       
            }

            with yt_dlp.YoutubeDL(ydl_opts_profundo) as ydl:
                for (c, video_breve) in enumerate(lista_videos, start=1):         
                    url_video = video_breve.get('url')
                    if not url_video: continue
                    try:
                        video = ydl.extract_info(url_video, download=False)
                        if video:
                            fecha_pub_str = video.get('upload_date')

                            historico_videos.append({
                                "creador": creador_nombre, 
                                "nick_youtube": usuario_yt, 
                                "seguidores": subscriptores or 0,
                                "id_video": video.get('id'), 
                                "titulo": video.get('title'), 
                                "url_video": video.get('webpage_url'),
                                "fecha_publicacion": fecha_pub_str, 
                                "visualizaciones": video.get('view_count', 0) or 0,
                                "likes": video.get('like_count', 0) or 0, 
                                "duracion_segundos": video.get('duration')
                            })
                            time.sleep(0.2)
                    except Exception as e:
                        print(f"\t[YT] Error Extraient el video: {e}")
                    
                    if c % 10 == 0: print(f"\t[YT] Processat {c}/{len(lista_videos)} videos...")
            
            self._reportar_estado("YT", creador_nombre, "done")
            print(f"\t[YT] Finalitzat: {usuario_yt} - {len(lista_videos)} videos processats.")
            
            if historico_videos:
                try: 
                    pd.DataFrame(historico_videos).to_parquet(self.data_path / "historico_youtube.parquet", index=False)
                except Exception as e: 
                    print(f"\t[YT] Error guardant dades temporals de YT: {e}")
                    
            time.sleep(3)
            
        self.df_youtube = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        print(f"\n🎉 YOUTUBE COMPLETAT.")