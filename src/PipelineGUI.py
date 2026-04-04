import customtkinter as ctk
import tkinter as tk
import tkinter.messagebox as messagebox
import pandas as pd
from pathlib import Path
import threading
import sys

# Nuevas importaciones según tu archivo PipelineGUI.py
from extractors.InstagramExtractor import InstagramExtractor
from extractors.TikTokExtractor import TikTokExtractor
from extractors.YoutubeExtractor import YoutubeExtractor
from extractors.TwitchExtractor import TwitchExtractor
from extractors.DataCleaner import DataCleaner
from extractors.DataTransformer import DataTransformer

ctk.set_appearance_mode("System")  
ctk.set_default_color_theme("dark-blue")  

class TextRedirector(object):
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
        
        self.title("Gestor de Pipeline de Dades - Xarxes Socials")
        self.geometry("950x850") # Un poco más de altura para acomodar todo
        self.minsize(850, 700)
        
        self.data_path = Path().cwd() / "data"
        self.archivo_creadores = self.data_path / "creadors_poblet.parquet"
        self.df_creadores = self._cargar_creadores()
        
        self.creador_actual = None

        # Tipografías
        self.font_title = ctk.CTkFont(family="Segoe UI", size=18, weight="bold")
        self.font_subtitle = ctk.CTkFont(family="Segoe UI", size=14, weight="bold")
        self.font_body = ctk.CTkFont(family="Segoe UI", size=12)
        self.font_button = ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        self.font_console = ctk.CTkFont(family="Consolas", size=11)

        # Variables de control para el Pipeline
        self.progreso_redes = {red: {"actual": 0, "total": 0} for red in ["IG", "TT", "YT", "TW"]}
        self.estado_vivo = {"IG": [], "TT": [], "YT": [], "TW": []}
        
        self.redes_vars = {
            'instagram': ctk.BooleanVar(value=True),
            'tiktok': ctk.BooleanVar(value=True),
            'youtube': ctk.BooleanVar(value=True),
            'twitch': ctk.BooleanVar(value=True)
        }
        self.var_extraer = ctk.BooleanVar(value=True)
        self.var_limpiar = ctk.BooleanVar(value=True)

        self._construir_interfaz_completa()

    def _cargar_creadores(self):
        if self.archivo_creadores.exists():
            try:
                return pd.read_parquet(self.archivo_creadores)
            except Exception as e:
                print(f"Error carregant creadors: {e}")
        return pd.DataFrame(columns=['creador', 'nick_instagram', 'nick_tiktok', 'nick_youtube', 'nick_twitch'])

    def _guardar_creadores(self):
        self.data_path.mkdir(exist_ok=True)
        self.df_creadores.to_parquet(self.archivo_creadores, index=False)

    def _construir_interfaz_completa(self):
        # Crear Tabview Principal
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(padx=20, pady=20, fill="both", expand=True)
        
        self.tab_pipeline = self.tabview.add("Processament")
        self.tab_bd = self.tabview.add("Base de Dades")

        self._construir_tab_pipeline()
        self._construir_tab_bd()

    def _construir_tab_pipeline(self):
        # Frame de Opciones
        frame_opciones = ctk.CTkFrame(self.tab_pipeline, fg_color="transparent")
        frame_opciones.pack(fill="x", pady=10)

        # Columna Izquierda: Redes
        frame_izq = ctk.CTkFrame(frame_opciones)
        frame_izq.pack(side="left", fill="both", expand=True, padx=(0, 10))
        ctk.CTkLabel(frame_izq, text="1. Selecció de Plataformes", font=self.font_subtitle).pack(pady=(10, 5), padx=10, anchor="w")
        
        for red, var in self.redes_vars.items():
            ctk.CTkCheckBox(frame_izq, text=red.capitalize(), variable=var, font=self.font_body).pack(padx=20, pady=5, anchor="w")

        # Columna Derecha: Fases
        frame_der = ctk.CTkFrame(frame_opciones)
        frame_der.pack(side="right", fill="both", expand=True, padx=(10, 0))
        ctk.CTkLabel(frame_der, text="2. Fases del Pipeline", font=self.font_subtitle).pack(pady=(10, 5), padx=10, anchor="w")
        
        ctk.CTkCheckBox(frame_der, text="Extracció de Dades Noves", variable=self.var_extraer).pack(pady=5, padx=20, anchor="w")
        ctk.CTkCheckBox(frame_der, text="Neteja i Consolidació", variable=self.var_limpiar).pack(pady=5, padx=20, anchor="w")

        # Botón Ejecutar
        self.btn_ejecutar = ctk.CTkButton(self.tab_pipeline, text="INICIAR PROCESSAMENT", height=45, font=self.font_button, command=self._iniciar_pipeline)
        self.btn_ejecutar.pack(pady=20)

        # Tracker en vivo
        ctk.CTkLabel(self.tab_pipeline, text="📡 Tracker d'Extracció en Viu", font=self.font_subtitle, text_color="#00D2FF").pack(anchor="w", padx=20)
        self.caja_vivo = ctk.CTkTextbox(self.tab_pipeline, height=80, font=self.font_console, fg_color="#2b2b2b")
        self.caja_vivo.pack(fill='x', padx=20, pady=(0, 10))
        self.caja_vivo.configure(state='disabled')

        # Consola principal
        ctk.CTkLabel(self.tab_pipeline, text="Consola d'Operacions", font=self.font_subtitle).pack(anchor="w", padx=20)
        self.consola = ctk.CTkTextbox(self.tab_pipeline, font=self.font_console, text_color="#A9B7C6", fg_color="#1E1E1E")
        self.consola.pack(fill="both", expand=True, padx=20, pady=(5, 20))

    def _construir_tab_bd(self):
        # SECCIÓN 1: AÑADIR
        frame_add = ctk.CTkFrame(self.tab_bd)
        frame_add.pack(fill="x", padx=20, pady=(15, 5))
        ctk.CTkLabel(frame_add, text="➕ Afegir Nou Creador:", font=self.font_subtitle).pack(side="left", padx=15, pady=15)
        self.entrada_nou_creador = ctk.CTkEntry(frame_add, width=250, placeholder_text="Nom del nou creador...")
        self.entrada_nou_creador.pack(side="left", padx=10, pady=15)
        ctk.CTkButton(frame_add, text="CREAR PERFIL", command=self._crear_nou_creador).pack(side="left", padx=10)

        # SECCIÓN 2: MODIFICAR/ELIMINAR
        frame_edicion = ctk.CTkFrame(self.tab_bd)
        frame_edicion.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Lista Izquierda
        frame_lista = ctk.CTkFrame(frame_edicion, fg_color="transparent")
        frame_lista.pack(side="left", fill="y", padx=15, pady=15)
        self.entrada_buscador = ctk.CTkEntry(frame_lista, width=250, placeholder_text="Buscar...")
        self.entrada_buscador.pack(pady=(0, 10))
        self.entrada_buscador.bind("<KeyRelease>", self._filtrar_lista)
        
        self.lista_creadores = tk.Listbox(frame_lista, bg="#343638", fg="white", font=("Segoe UI", 11), borderwidth=0)
        self.lista_creadores.pack(side="left", fill="both", expand=True)
        self.lista_creadores.bind("<<ListboxSelect>>", self._on_select_creador)
        
        # Formulario Derecho
        frame_campos = ctk.CTkFrame(frame_edicion, fg_color="transparent")
        frame_campos.pack(side="right", fill="both", expand=True, padx=15, pady=15)
        
        self.entradas = {}
        for col, label in [('instagram', 'Instagram'), ('tiktok', 'TikTok'), ('youtube', 'YouTube'), ('twitch', 'Twitch')]:
            f = ctk.CTkFrame(frame_campos, fg_color="transparent")
            f.pack(fill="x", pady=5)
            ctk.CTkLabel(f, text=label+":", width=100, anchor="w").pack(side="left")
            self.entradas[col] = ctk.CTkEntry(f, width=250)
            self.entradas[col].pack(side="left", padx=10)

        # Botones de Acción
        btn_f = ctk.CTkFrame(frame_campos, fg_color="transparent")
        btn_f.pack(pady=20)
        ctk.CTkButton(btn_f, text="GUARDAR CANVIS", command=self._guardar_cambios_creador).pack(side="left", padx=10)
        ctk.CTkButton(btn_f, text="ELIMINAR", fg_color="#E63946", hover_color="#C1121F", command=self._eliminar_creador).pack(side="left", padx=10)

        self._actualitzar_llista()

    # --- Lógica de Base de Datos ---
    def _crear_nou_creador(self):
        nom = self.entrada_nou_creador.get().strip()
        if not nom or (not self.df_creadores.empty and nom.lower() in self.df_creadores['creador'].str.lower().values):
            messagebox.showwarning("Error", "Nom buit o ja existent.")
            return
        
        nova_fila = {'creador': nom, 'nick_instagram': None, 'nick_tiktok': None, 'nick_youtube': None, 'nick_twitch': None}
        self.df_creadores = pd.concat([self.df_creadores, pd.DataFrame([nova_fila])], ignore_index=True)
        self._guardar_creadores()
        self._actualitzar_llista()
        self.entrada_nou_creador.delete(0, 'end')

    def _actualitzar_llista(self, filtre=""):
        self.lista_creadores.delete(0, tk.END)
        if self.df_creadores.empty: return
        noms = sorted(self.df_creadores['creador'].dropna().unique().tolist(), key=str.lower)
        for n in noms:
            if filtre.lower() in n.lower(): self.lista_creadores.insert(tk.END, n)

    def _filtrar_lista(self, event):
        self._actualitzar_llista(self.entrada_buscador.get())

    def _on_select_creador(self, event):
        sel = self.lista_creadores.curselection()
        if sel:
            self.creador_actual = self.lista_creadores.get(sel[0])
            fila = self.df_creadores[self.df_creadores['creador'] == self.creador_actual].iloc[0]
            for red, entry in self.entradas.items():
                entry.delete(0, 'end')
                val = fila.get(f'nick_{red}', '')
                if pd.notna(val): entry.insert(0, str(val))

    def _guardar_cambios_creador(self):
        if not self.creador_actual: return
        mascara = self.df_creadores['creador'] == self.creador_actual
        for red, entry in self.entradas.items():
            nuevo_val = entry.get().strip() or None
            self.df_creadores.loc[mascara, f'nick_{red}'] = nuevo_val
        self._guardar_creadores()
        messagebox.showinfo("Èxit", "Canvis guardats.")

    def _eliminar_creador(self):
        if not self.creador_actual: return
        if messagebox.askyesno("Confirmar", f"Eliminar a {self.creador_actual}?"):
            self.df_creadores = self.df_creadores[self.df_creadores['creador'] != self.creador_actual]
            self._guardar_creadores()
            self.creador_actual = None
            self._actualitzar_llista()
            for e in self.entradas.values(): e.delete(0, 'end')

    # --- Lógica del Pipeline (Integrada con las nuevas clases) ---
    def _iniciar_pipeline(self):
        self.btn_ejecutar.configure(state="disabled")
        self.consola.configure(state="normal")
        self.consola.delete("0.0", "end")
        self.consola.configure(state="disabled")
        
        thread = threading.Thread(target=self._hilo_pipeline, daemon=True)
        thread.start()

    def _hilo_pipeline(self):
        sys.stdout = TextRedirector(self.consola)
        try:
            redes_activas = [r for r, v in self.redes_vars.items() if v.get()]
            print(f"[INFO] Iniciant Pipeline per a: {', '.join(redes_activas)}")
            
            if self.var_extraer.get():
                print("[1/2] FASE EXTRACCIÓ...")
                # Aquí instanciarías tus extractores específicos según tu nueva estructura
                # Ejemplo: inst = InstagramExtractor() ...
            
            if self.var_limpiar.get():
                print("[2/2] FASE NETEJA...")
                cleaner = DataCleaner()
                # cleaner.process()...
                
            print("[OK] Pipeline finalitzat correctament.")
        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            sys.stdout = sys.__stdout__
            self.after(0, lambda: self.btn_ejecutar.configure(state="normal"))

if __name__ == "__main__":
    app = PipelineApp()
    app.mainloop()