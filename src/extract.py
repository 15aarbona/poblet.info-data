from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

# ==========================================
# CONFIGURACIÓN DE RUTAS (PATHS)
# ==========================================
# Si tus HTML están en otra carpeta, cambia el "." por tu ruta. 
# Ejemplo: carpeta_datos = Path("/home/usuario/proyectos/poblet/datos")
carpeta_datos = Path().cwd() / "data"

# Lista de archivos a leer (añadidos los de twitter y facebook por si los has descargado)
archivos_locales = [
    carpeta_datos / "html" / "poblet_instagram.html",
    carpeta_datos / "html" / "poblet_tiktok.html",
    carpeta_datos / "html" / "poblet_youtube.html",
    carpeta_datos / "html" / "poblet_twitch.html",
    carpeta_datos / "html" / "poblet_podcast.html",
    carpeta_datos / "html" / "poblet_twitter.html",
    carpeta_datos / "html" / "poblet_facebook.html"
]

# Diccionario para almacenar los datos (ahora con Twitter y Facebook)
creadores = defaultdict(lambda: {
    "nombre_mostrar": "",
    "nick_instagram": np.nan, 
    "nick_tiktok": np.nan, 
    "nick_youtube": np.nan, 
    "nick_twitch": np.nan,
    "nick_podcast": np.nan,
    "nick_twitter": np.nan,
    "nick_facebook": np.nan
})

def normalizar_nombre(nombre_raw):
    """Filtro para unificar a un creador si aparece en varios archivos"""
    n = nombre_raw.lower().strip().replace('@', '').replace(' ', '')
    return ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

def extraer_nick(url):
    """Detecta la red social por la URL y extrae el nick respetando mayúsculas/minúsculas"""
    url_lower = url.lower()
    
    if "instagram.com" in url_lower:
        match = re.search(r"instagram\.com/([^/?]+)", url, re.IGNORECASE)
        return "instagram", match.group(1) if match else url
        
    elif "tiktok.com" in url_lower:
        match = re.search(r"tiktok\.com/@([^/?]+)", url, re.IGNORECASE)
        return "tiktok", match.group(1) if match else url
        
    elif "youtube.com" in url_lower or "youtu.be" in url_lower:
        match = re.search(r"youtube\.com/(?:@|c/|channel/|user/)?([^/?]+)", url, re.IGNORECASE)
        if not match and "youtu.be" in url_lower:
            match = re.search(r"youtu\.be/([^/?]+)", url, re.IGNORECASE)
        return "youtube", match.group(1) if match else url
        
    elif "twitch.tv" in url_lower:
        match = re.search(r"twitch\.tv/([^/?]+)", url, re.IGNORECASE)
        return "twitch", match.group(1) if match else url
        
    # Novedad: Twitter / X
    elif "twitter.com" in url_lower or "x.com" in url_lower:
        match = re.search(r"(?:twitter\.com|x\.com)/([^/?]+)", url, re.IGNORECASE)
        return "twitter", match.group(1) if match else url
        
    # Novedad: Facebook
    elif "facebook.com" in url_lower or "fb.com" in url_lower:
        match = re.search(r"(?:facebook\.com|fb\.com)/([^/?]+)", url, re.IGNORECASE)
        return "facebook", match.group(1) if match else url
        
    elif any(x in url_lower for x in ["spotify.com", "ivoox.com", "apple.com", "podcasts"]):
        return "podcast", url
        
    return None, None

print("Iniciando escaneo inteligente de tarjetas de creadores...")

for archivo_path in archivos_locales:
    # Comprobamos si el archivo realmente existe antes de intentar leerlo
    if not archivo_path.exists():
        print(f"  ⏭️  Saltando: {archivo_path.name} (No se ha encontrado el archivo)")
        continue
        
    print(f"  📄 Leyendo: {archivo_path.name}...")
    try:
        with open(archivo_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        columnas = soup.find_all('div', class_=re.compile(r'wp-block-column'))
        
        for col in columnas:
            # 1. Sacar el nombre del creador
            etiquetas_texto = col.find_all(['strong', 'h2', 'h3'])
            nombre_crudo = ""
            for etiqueta in etiquetas_texto:
                texto = etiqueta.get_text(strip=True)
                if texto and len(texto) > 1 and "==" not in texto:
                    nombre_crudo = texto
                    break
                    
            if not nombre_crudo:
                continue 
                
            id_unico = normalizar_nombre(nombre_crudo)
            
            if not creadores[id_unico]["nombre_mostrar"]:
                creadores[id_unico]["nombre_mostrar"] = nombre_crudo.lstrip('@')
            
            # 2. Rastrear TODOS los enlaces dentro de su tarjeta
            enlaces = col.find_all('a', href=True)
            for a in enlaces:
                href = a['href']
                
                # Ignoramos enlaces internos a poblet.info
                if "poblet.info" in href:
                    continue
                    
                red, nick = extraer_nick(href)
                
                if red and nick:
                    creadores[id_unico][f"nick_{red}"] = nick

    except Exception as e:
        print(f"  ❌ Error procesando {archivo_path.name}: {e}")

# Exportación a Pandas y Parquet
datos_tabla = []
for id_unico, datos in creadores.items():
    fila = {"creador": datos["nombre_mostrar"]}
    fila.update({k: v for k, v in datos.items() if k.startswith('nick_')})
    datos_tabla.append(fila)

df = pd.DataFrame(datos_tabla)

if not df.empty:
    df = df.sort_values(by="creador").reset_index(drop=True)
    
    # Guardamos el archivo de salida en la misma carpeta que los datos
    archivo_salida = carpeta_datos / "creadors_poblet.csv"
    df.to_csv(archivo_salida, sep=";", index=False)
    
    print("\n" + "="*40)
    print("      ¡EXTRACCIÓN COMPLETADA!      ")
    print("="*40)
    print(f"✅ Total de creadores únicos: {len(df)}")
    print(df.head())
    print(f"\n📁 Archivo guardado en: {archivo_salida.absolute()}")
else:
    print("\nNo se ha extraído nada. Comprueba la estructura HTML y tus archivos.")