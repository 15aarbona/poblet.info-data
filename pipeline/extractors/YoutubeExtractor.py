import pandas as pd
import yt_dlp
import time 
import concurrent.futures
import asyncio

from extractors.Extractor import Extractor

class YoutubeExtractor(Extractor):
    def __init__(self, config_path="tokens.json", ui_callback=None):
        super().__init__(config_path, ui_callback)

        # Obtener datos de YouTube
        self.df = self._obtener_columna_red_social("youtube")
        
        self.df = self.df[5:10]

    def _procesar_creador(self, row):
        creador_nombre = row.get('creador')
        usuario_yt = row.get('nick_youtube') 
                    
        self._reportar_estado("YT", creador_nombre, "start")
        
        handle_str = str(usuario_yt).strip()
        if not handle_str.startswith('@'): 
            handle_str = f"@{handle_str}"
            
        # Opciones ajustadas para ser más conservadores y evitar el Error 429
        ydl_opts_profundo = {
            'quiet': True, 
            'no_warnings': True, 
            'ignoreerrors': True, 
            'extract_flat': False,    # Necesario en False para obtener Likes y Comentarios
            'playlist_items': '1-15', # Mantenemos el límite de 15
            'sleep_interval_requests': 2,
            'sleep_interval': 4,      # 🛡️ Aumentamos un poco la pausa
            'max_sleep_interval': 7,
            'extractor_args': {'youtube': ['player_client=web']}
        }

        videos_creador = []
        
        # 🧩 Solución: Buscar explícitamente en ambas pestañas
        pestañas_a_extraer = [
            ("VIDEO", f"https://www.youtube.com/{handle_str}/videos"),
            ("SHORT", f"https://www.youtube.com/{handle_str}/shorts")
        ]

        try:
            with yt_dlp.YoutubeDL(ydl_opts_profundo) as ydl:
                
                for tipo_pub, url_tab in pestañas_a_extraer:
                    info_canal = ydl.extract_info(url_tab, download=False)
                    
                    if not info_canal or 'entries' not in info_canal:
                        continue
                    
                    entradas = info_canal.get('entries', [])
                    subscriptores = info_canal.get('channel_follower_count', 0)
                    
                    for video in entradas:
                        if not video: continue
                        
                        fecha_pub = video.get('upload_date')
                        fecha_pub_str = None
                        
                        if fecha_pub and len(fecha_pub) == 8:
                            fecha_pub_str = f"{fecha_pub[:4]}-{fecha_pub[4:6]}-{fecha_pub[6:]}"
                        elif video.get('timestamp'):
                            from datetime import datetime
                            fecha_pub_str = datetime.utcfromtimestamp(video.get('timestamp')).strftime('%Y-%m-%d')
                        
                        videos_creador.append({
                            "creador": creador_nombre, 
                            "nick_youtube": usuario_yt, 
                            "seguidores": subscriptores or 0,
                            "id_video": video.get('id'), 
                            "fecha_publicacion": fecha_pub_str, 
                            "tipo_publicacion": tipo_pub, # NUEVO: "VIDEO" o "SHORT"
                            "visualizaciones": video.get('view_count', 0) or 0,
                            "likes": video.get('like_count', 0) or 0, 
                            "comentarios": video.get('comment_count', 0) or 0, # NUEVO: Añadidos los comentarios
                            "url_video": video.get('webpage_url') if video.get('webpage_url') else video.get('url')
                        })
                        
        except Exception as e:
            print(f"\t[YT] ❌ Error Extraient el canal {usuario_yt}: {e}")
        
        self._reportar_estado("YT", creador_nombre, "done")
        return videos_creador

    def _extraccion_sync(self):
        self._reportar_estado("YT", "", "init", total=len(self.df)) 
        
        historico_videos = []
        
        # 🚀 PARALELIZACIÓN: Lanzamos hasta 3 canales a la vez. 
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Enviamos todos los creadores al pool de hilos
            futuros = [executor.submit(self._procesar_creador, row) for _, row in self.df.iterrows()]
            
            # as_completed nos permite recoger los datos según vayan terminando
            for futuro in concurrent.futures.as_completed(futuros):
                resultado = futuro.result()
                if resultado:
                    historico_videos.extend(resultado)
                    
                    # Guardado parcial de seguridad cada vez que un creador termina
                    try: 
                        pd.DataFrame(historico_videos).to_parquet(self.data_path / "historico_youtube.parquet", index=False)
                    except Exception as e: 
                        print(f"\t[YT] Error guardant dades temporals de YT: {e}")
        
        self.df = pd.DataFrame(historico_videos) if historico_videos else pd.DataFrame()
        print(f"\n🎉 YOUTUBE COMPLETAT.")

    async def _extraccion(self):
        """Envoltorio asíncrono para ejecutar la función síncrona sin bloquear la UI"""
        return await asyncio.to_thread(self._extraccion_sync)