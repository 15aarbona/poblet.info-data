import customtkinter as ctk
import tkinter.messagebox as messagebox
import pandas as pd
from pathlib import Path
import threading
import sys

# Importamos tus clases
from Extractor import Extractor
from DataClean import Cleaner

# Configuración visual global corporativa
ctk.set_appearance_mode("System")  
ctk.set_default_color_theme("dark-blue")  

class TextRedirector(object):
    """Clase para redirigir los 'print' de la consola al CTkTextbox"""
    def __init__(self, widget):
        self.widget = widget

    def write(self, text):
        self.widget.configure(state='normal')
        self.widget.insert("end", text)
        self.widget.see("end")
        self.widget.configure(state='disabled')

    def flush(self):
        pass

class PipelineApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.title("Gestor de Pipeline de Datos - Redes Sociales")
        self.geometry("950x750")
        self.minsize(850, 650)
        
        self.data_path = Path().cwd() / "data"
        self.archivo_creadores = self.data_path / "creadors_poblet.parquet"
        self.df_creadores = self._cargar_creadores()

        # Fuentes estandarizadas
        self.font_title = ctk.CTkFont(family="Segoe UI", size=18, weight="bold")
        self.font_subtitle = ctk.CTkFont(family="Segoe UI", size=14, weight="bold")
        self.font_body = ctk.CTkFont(family="Segoe UI", size=13)
        self.font_button = ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        self.font_console = ctk.CTkFont(family="Consolas", size=12)

        # Sistema de pestañas moderno y limpio
        self.tabview = ctk.CTkTabview(self, command=self._al_cambiar_pestana, corner_radius=8)
        self.tabview.pack(fill='both', expand=True, padx=25, pady=25)

        self.tab_pipeline = self.tabview.add('Ejecución de Pipeline')
        self.tab_add = self.tabview.add('Alta de Creadores')
        self.tab_modify = self.tabview.add('Gestión de Perfiles')

        self._construir_tab_pipeline()
        self._construir_tab_add()
        self._construir_tab_modify()

    def _cargar_creadores(self):
        if self.archivo_creadores.exists():
            return pd.read_parquet(self.archivo_creadores)
        else:
            return pd.DataFrame(columns=["creador", "nick_instagram", "nick_tiktok", "nick_youtube", "nick_twitch", "nick_podcast"])

    def _guardar_creadores(self):
        self.df_creadores.to_parquet(self.archivo_creadores, index=False)

    # ==========================================
    # PESTAÑA 1: PIPELINE
    # ==========================================
    def _construir_tab_pipeline(self):
        ctk.CTkLabel(self.tab_pipeline, text="Panel de Control de Procesamiento", font=self.font_title).pack(pady=(15, 20))

        frame_opciones = ctk.CTkFrame(self.tab_pipeline, fg_color="transparent")
        frame_opciones.pack(fill='x', padx=20, pady=5)

        # 1. Bloque de Acciones
        frame_acciones = ctk.CTkFrame(frame_opciones, corner_radius=8)
        frame_acciones.pack(side='left', fill='both', expand=True, padx=(0, 10))
        
        ctk.CTkLabel(frame_acciones, text="Fases del Proceso", font=self.font_subtitle).pack(pady=(15, 10))

        self.var_extraer = ctk.BooleanVar(value=True)
        self.var_limpiar = ctk.BooleanVar(value=True)

        ctk.CTkSwitch(frame_acciones, text="Extracción de Datos (Scraping/API)", variable=self.var_extraer, font=self.font_body).pack(anchor='w', padx=30, pady=10)
        ctk.CTkSwitch(frame_acciones, text="Limpieza y Fusión de Datos", variable=self.var_limpiar, font=self.font_body).pack(anchor='w', padx=30, pady=10)

        # 2. Bloque de Redes Sociales
        frame_redes = ctk.CTkFrame(frame_opciones, corner_radius=8)
        frame_redes.pack(side='right', fill='both', expand=True, padx=(10, 0))
        
        ctk.CTkLabel(frame_redes, text="Fuentes de Datos", font=self.font_subtitle).pack(pady=(15, 10))

        self.redes_vars = {
            'instagram': ctk.BooleanVar(value=True),
            'tiktok': ctk.BooleanVar(value=True),
            'youtube': ctk.BooleanVar(value=True),
            'twitch': ctk.BooleanVar(value=True)
        }

        # Alineación recta vertical para los switches de redes (queda más limpio)
        frame_switches = ctk.CTkFrame(frame_redes, fg_color="transparent")
        frame_switches.pack(pady=5)
        
        for red, var in self.redes_vars.items():
            ctk.CTkSwitch(frame_switches, text=red.capitalize(), variable=var, font=self.font_body).pack(anchor='w', pady=8)

        # --- Botón de Ejecución ---
        self.btn_ejecutar = ctk.CTkButton(self.tab_pipeline, text="INICIAR PROCESAMIENTO", height=40, width=250, font=self.font_button, command=self._iniciar_pipeline)
        self.btn_ejecutar.pack(pady=25)

        # --- Barra de progreso ---
        self.progress = ctk.CTkProgressBar(self.tab_pipeline, mode="indeterminate", height=6)
        self.progress.set(0)

        # --- Consola virtual ---
        self.consola = ctk.CTkTextbox(self.tab_pipeline, height=220, font=self.font_console, text_color="#A9B7C6", fg_color="#1E1E1E", corner_radius=8)
        self.consola.pack(fill='both', expand=True, padx=20, pady=(0, 15))
        self.consola.configure(state='disabled')

    def _iniciar_pipeline(self):
        self.btn_ejecutar.configure(state='disabled')
        
        self.progress.pack(fill='x', padx=20, pady=(0, 10), before=self.consola)
        self.progress.start()

        self.consola.configure(state='normal')
        self.consola.delete('1.0', "end")
        self.consola.configure(state='disabled')
        
        sys.stdout = TextRedirector(self.consola)
        
        hilo = threading.Thread(target=self._hilo_pipeline)
        hilo.start()

    def _hilo_pipeline(self):
        extraer = self.var_extraer.get()
        limpiar = self.var_limpiar.get()
        redes_activas = [red for red, var in self.redes_vars.items() if var.get()]

        try:
            if not extraer and not limpiar:
                print("[ERROR] No se ha seleccionado ninguna fase del proceso.")
                return
            if not redes_activas:
                print("[ERROR] No se ha seleccionado ninguna fuente de datos.")
                return

            print("==================================================")
            print("[SISTEMA] Iniciando pipeline de procesamiento")
            print(f"[SISTEMA] Fases activas: {'Extracción ' if extraer else ''}{'- Limpieza' if limpiar else ''}")
            print(f"[SISTEMA] Fuentes objetivo: {', '.join(redes_activas).upper()}")
            print("==================================================\n")
            
            if extraer:
                print("[PROCESO] Iniciando fase de extracción de datos...")
                extractor = Extractor()
                
                if 'tiktok' in redes_activas: 
                    extractor._extraccion_tiktok()
                    if not hasattr(extractor, 'df_tiktok') or not extractor.df_tiktok.empty: 
                        extractor.df_tiktok.to_parquet(extractor.data_path / "historico_tiktok.parquet", index=False)
                if 'instagram' in redes_activas: 
                    extractor._extraccion_instagram()
                    if not hasattr(extractor, 'df_instagram') or not extractor.df_instagram.empty: 
                        extractor.df_instagram.to_parquet(extractor.data_path / "historico_instagram.parquet", index=False)
                if 'youtube' in redes_activas: 
                    extractor._extraccion_youtube()
                    if not hasattr(extractor, 'df_youtube') or not extractor.df_youtube.empty: 
                        extractor.df_youtube.to_parquet(extractor.data_path / "historico_youtube.parquet", index=False)
                if 'twitch' in redes_activas: 
                    extractor._extraccion_twitch()
                    if not hasattr(extractor, 'df_twitch') or not extractor.df_twitch.empty: 
                        extractor.df_twitch.to_parquet(extractor.data_path / "historico_twitch.parquet", index=False)
                    
                print("[INFO] Fase de extracción finalizada correctamente.\n")

            if limpiar:
                print("[PROCESO] Iniciando fase de limpieza y consolidación...")
                cleaner = Cleaner()
                
                if 'instagram' in redes_activas: cleaner._limpiar_instagram()
                if 'tiktok' in redes_activas: cleaner._limpiar_tiktok()
                if 'youtube' in redes_activas: cleaner._limpiar_youtube()
                if 'twitch' in redes_activas: cleaner._limpiar_twitch()
                    
                print("[INFO] Fase de limpieza finalizada correctamente.\n")

            print("==================================================")
            print("[SISTEMA] Proceso completado sin errores críticos.")
            print("==================================================")
            
        except Exception as e:
            print(f"\n[ERROR CRÍTICO] Excepción capturada durante la ejecución: {e}")
        finally:
            sys.stdout = sys.__stdout__
            self.after(0, self._finalizar_interfaz_pipeline)

    def _finalizar_interfaz_pipeline(self):
        self.progress.stop()
        self.progress.pack_forget()
        self.btn_ejecutar.configure(state='normal')

    # ==========================================
    # PESTAÑA 2: AÑADIR CREADOR
    # ==========================================
    def _construir_tab_add(self):
        ctk.CTkLabel(self.tab_add, text="Registro de Nuevo Creador", font=self.font_title).pack(pady=(20, 20))

        frame_form = ctk.CTkFrame(self.tab_add, corner_radius=8)
        frame_form.pack(pady=10, padx=80, fill="both", expand=True)

        # Sistema de Grid perfecto: Etiqueta fija, Entry expansible
        frame_form.grid_columnconfigure(0, weight=0, minsize=200) # Columna de texto
        frame_form.grid_columnconfigure(1, weight=1)              # Columna de cajas (se estira)

        campos = ["Nombre del Creador (*)", "Identificador Instagram", "Identificador TikTok", "Identificador YouTube", "Identificador Twitch", "Identificador Podcast"]
        self.entradas_add = {}

        for i, campo in enumerate(campos):
            ctk.CTkLabel(frame_form, text=campo, font=self.font_body).grid(row=i, column=0, sticky='w', padx=(30, 15), pady=15)
            ent = ctk.CTkEntry(frame_form, font=self.font_body)
            ent.grid(row=i, column=1, sticky="ew", padx=(0, 30), pady=15) # "ew" hace que se expanda horizontalmente
            
            key = "creador" if i == 0 else campo.lower().replace("identificador ", "nick_").replace(" (*)", "")
            self.entradas_add[key] = ent

        btn_guardar = ctk.CTkButton(self.tab_add, text="Registrar Entidad", height=40, width=250, font=self.font_button, command=self._guardar_nuevo_creador)
        btn_guardar.pack(pady=30)

    def _guardar_nuevo_creador(self):
        nuevo_dato = {}
        for key, entry in self.entradas_add.items():
            valor = entry.get().strip()
            nuevo_dato[key] = valor if valor != "" else None

        if not nuevo_dato['creador']:
            messagebox.showwarning("Aviso del Sistema", "El Nombre del Creador es un campo obligatorio.")
            return

        if nuevo_dato['creador'] in self.df_creadores['creador'].values:
            messagebox.showerror("Error de Integridad", "Ya existe un registro con este nombre en la base de datos.")
            return

        df_nuevo = pd.DataFrame([nuevo_dato])
        self.df_creadores = pd.concat([self.df_creadores, df_nuevo], ignore_index=True)
        self._guardar_creadores()

        messagebox.showinfo("Operación Exitosa", f"El registro '{nuevo_dato['creador']}' ha sido guardado correctamente.")
        
        for entry in self.entradas_add.values():
            entry.delete(0, "end")

    # ==========================================
    # PESTAÑA 3: MODIFICAR CREADOR
    # ==========================================
    def _construir_tab_modify(self):
        ctk.CTkLabel(self.tab_modify, text="Gestión y Actualización de Perfiles", font=self.font_title).pack(pady=(20, 10))

        # Panel superior de selección
        frame_top = ctk.CTkFrame(self.tab_modify, corner_radius=8)
        frame_top.pack(pady=10, padx=80, fill="x")

        ctk.CTkLabel(frame_top, text="Seleccione el perfil a gestionar:", font=self.font_body).pack(side="left", padx=(30, 10), pady=15)
        
        # EL GRAN CAMBIO: Sustituimos CTkComboBox (permite escribir) por CTkOptionMenu (solo permite elegir de lista)
        self.combo_creadores = ctk.CTkOptionMenu(frame_top, width=300, font=self.font_body, command=self._cargar_datos_modificar)
        self.combo_creadores.pack(side="left", padx=10, pady=15)
        self.combo_creadores.set("Desplegar opciones...")

        # Formulario de modificación
        frame_form = ctk.CTkFrame(self.tab_modify, corner_radius=8)
        frame_form.pack(pady=10, padx=80, fill="both", expand=True)
        
        # Sistema de Grid perfecto
        frame_form.grid_columnconfigure(0, weight=0, minsize=200)
        frame_form.grid_columnconfigure(1, weight=1)

        campos = ["Identificador Instagram", "Identificador TikTok", "Identificador YouTube", "Identificador Twitch", "Identificador Podcast"]
        self.entradas_mod = {}

        for i, campo in enumerate(campos):
            ctk.CTkLabel(frame_form, text=campo, font=self.font_body).grid(row=i, column=0, sticky='w', padx=(30, 15), pady=15)
            ent = ctk.CTkEntry(frame_form, font=self.font_body)
            ent.grid(row=i, column=1, sticky="ew", padx=(0, 30), pady=15)
            
            key = campo.lower().replace("identificador ", "nick_")
            self.entradas_mod[key] = ent

        btn_actualizar = ctk.CTkButton(self.tab_modify, text="Aplicar Cambios", height=40, width=250, font=self.font_button, command=self._actualizar_creador)
        btn_actualizar.pack(pady=30)

    def _al_cambiar_pestana(self):
        # Actualizamos la lista de creadores en el OptionMenu
        creadores = self.df_creadores['creador'].dropna().tolist()
        creadores.sort()
        if not creadores:
            creadores = ["Sin creadores registrados"]
            
        self.combo_creadores.configure(values=creadores)
        self.combo_creadores.set("Seleccionar perfil...")

    def _cargar_datos_modificar(self, choice):
        creador_seleccionado = choice
        if not creador_seleccionado or creador_seleccionado in ["Seleccionar perfil...", "Sin creadores registrados"]: 
            return

        fila = self.df_creadores[self.df_creadores['creador'] == creador_seleccionado].iloc[0]

        for key, entry in self.entradas_mod.items():
            entry.delete(0, "end")
            valor = fila.get(key)
            if pd.notna(valor) and valor is not None:
                entry.insert(0, str(valor))

    def _actualizar_creador(self):
        creador_seleccionado = self.combo_creadores.get()
        if not creador_seleccionado or creador_seleccionado in ["Seleccionar perfil...", "Sin creadores registrados"]:
            messagebox.showwarning("Aviso del Sistema", "Debe seleccionar un perfil del menú desplegable.")
            return

        indice = self.df_creadores.index[self.df_creadores['creador'] == creador_seleccionado].tolist()[0]
        fila_antigua = self.df_creadores.iloc[indice]
        
        cambios_realizados = False
        resumen_cambios = []

        for key, entry in self.entradas_mod.items():
            valor_nuevo = entry.get().strip()
            valor_nuevo = valor_nuevo if valor_nuevo != "" else None
            
            valor_antiguo = fila_antigua.get(key)
            if pd.isna(valor_antiguo): 
                valor_antiguo = None

            if str(valor_antiguo) != str(valor_nuevo):
                self.df_creadores.at[indice, key] = valor_nuevo
                cambios_realizados = True
                
                if key.startswith("nick_") and valor_nuevo is not None:
                    red_social = key.replace("nick_", "")
                    filas_modificadas = self._actualizar_historico_nick(red_social, col_nick=key, creador_nombre=creador_seleccionado, nick_nuevo=valor_nuevo)
                    resumen_cambios.append(f"  - {red_social.capitalize()}: {filas_modificadas} registros históricos actualizados.")

        if cambios_realizados:
            self._guardar_creadores()
            mensaje_exito = f"El perfil '{creador_seleccionado}' ha sido actualizado exitosamente.\n\nImpacto en Base de Datos:\n"
            mensaje_exito += "\n".join(resumen_cambios) if resumen_cambios else "No se detectaron registros históricos que requirieran propagación de cambios."
            messagebox.showinfo("Proceso Completado", mensaje_exito)
        else:
            messagebox.showinfo("Información", "No se han detectado modificaciones en los campos proporcionados.")

    def _actualizar_historico_nick(self, red_social, col_nick, creador_nombre, nick_nuevo):
        archivo_maestro = self.data_path / f"clean_{red_social}.parquet"
        filas_afectadas = 0
        
        if archivo_maestro.exists():
            try:
                df = pd.read_parquet(archivo_maestro)
                if not df.empty and 'creador' in df.columns and col_nick in df.columns:
                    mascara = df['creador'] == creador_nombre
                    filas_afectadas = mascara.sum()
                    
                    if filas_afectadas > 0:
                        df.loc[mascara, col_nick] = nick_nuevo
                        df.to_parquet(archivo_maestro, index=False)
            except Exception as e:
                print(f"[ERROR] Fallo al intentar actualizar históricos de {red_social}: {e}")
                
        return filas_afectadas

if __name__ == "__main__":
    app = PipelineApp()
    app.mainloop()