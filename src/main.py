from Extractor import Extractor
from DataClean import Cleaner

if __name__ == "__main__":
    print("🚀 Iniciando el proceso de extracción y limpieza de datos...")
    
    # # 1. Extracción de datos
    # extractor = Extractor()
    # extractor.extraction()

    # 2. Limpieza de datos
    cleaner = Cleaner()
    cleaner.clean_all()

    print("\n✅ Proceso de extracción y limpieza de datos completado.")