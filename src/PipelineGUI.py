import customtkinter as ctk
import tkinter as tk
import tkinter.messagebox as messagebox
import pandas as pd
from pathlib import Path
import threading
import sys

from Extractor import Extractor
from DataClean import Cleaner

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
        self.geometry("950x750")
        self.minsize(850, 650)
        
        self.data_path = Path().cwd() / "data"
        self.archivo_creadores = self.data_path / "creadors_poblet.parquet"
        self.df_creadores = self._cargar_creadores()
        
        self.creador_actual = None

        self.font_title = ctk.CTkFont(family="Segoe UI", size=18, weight="bold")
        self.font_subtitle = ctk.CTkFont(family="Segoe UI", size=14, weight="bold")
        self.font_body = ctk.CTkFont(family="Segoe UI", size=12)
        self.font_button = ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        self.font_console = ctk.CTkFont(family="Consolas", size=11)

        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(padx=20, pady=20, fill="both", expand=True)
        
        self.tab_pipeline = self.tabview.add("Processament")
        self.tab_bd = self.tabview.add("Base de Dades")

        self.redes_vars = {
            'instagram': ctk.BooleanVar(value=True),
            'tiktok': ctk.BooleanVar(value=True),
            'youtube': ctk.BooleanVar(value=True),
            'twitch': ctk.BooleanVar(value=True)
        }
        self.var_extraer = ctk.BooleanVar(value=True)
        self.var_limpiar = ctk.BooleanVar(value=True)

        self.estado_vivo = {"IG": [], "TT": [], "YT": [], "TW": []}

        self._construir_tab_pipeline()
        self._construir_tab_bd()

    def _cargar_creadores(self):
        if self.archivo_creadores.exists():
            return pd.read_parquet(self.archivo_creadores)
        else:
            return pd.DataFrame(columns=['creador', 'nick_instagram', 'nick_tiktok', 'nick_youtube', 'nick_twitch'])

    def _guardar_creadores(self):
        self.df_creadores.to_parquet(self.archivo_creadores, index=False)

    def _construir_tab_pipeline(self):
        frame_opciones = ctk.CTkFrame(self.tab_pipeline, fg_color="transparent")
        frame_opciones.pack(fill="x", pady=10)

        frame_izq = ctk.CTkFrame(frame_opciones)
        frame_izq.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        ctk.CTkLabel(frame_izq, text="1. Selecció de Plataformes", font=self.font_subtitle).pack(pady=(10, 5), padx=10, anchor="w")
        ctk.CTkLabel(frame_izq, text="Selecciona les fonts de dades a processar:", font=self.font_body, text_color="gray").pack(padx=10, anchor="w")
        
        grid_redes = ctk.CTkFrame(frame_izq, fg_color="transparent")
        grid_redes.pack(pady=10, padx=20, anchor="w")
        
        for idx, (red, var) in enumerate(self.redes_vars.items()):
            row, col = divmod(idx, 2)
            cb = ctk.CTkCheckBox(grid_redes, text=red.capitalize(), variable=var, font=self.font_body)
            cb.grid(row=row, column=col, padx=15, pady=8, sticky="w")

        frame_der = ctk.CTkFrame(frame_opciones)
        frame_der.pack(side="right", fill="both", expand=True, padx=(10, 0))
        
        ctk.CTkLabel(frame_der, text="2. Fases del Pipeline", font=self.font_subtitle).pack(pady=(10, 5), padx=10, anchor="w")
        
        cb_ext = ctk.CTkCheckBox(frame_der, text="Extracció de Dades Noves", variable=self.var_extraer, font=self.font_body)
        cb_ext.pack(pady=(10, 2), padx=20, anchor="w")
        ctk.CTkLabel(frame_der, text="Descarrega els últims posts i vídeos dels perfils", font=ctk.CTkFont(size=10), text_color="gray").pack(padx=45, anchor="w")
        
        cb_limp = ctk.CTkCheckBox(frame_der, text="Neteja i Consolidació", variable=self.var_limpiar, font=self.font_body)
        cb_limp.pack(pady=(10, 2), padx=20, anchor="w")
        ctk.CTkLabel(frame_der, text="Elimina duplicats i actualitza els històrics", font=ctk.CTkFont(size=10), text_color="gray").pack(padx=45, anchor="w")

        self.btn_ejecutar = ctk.CTkButton(self.tab_pipeline, text="INICIAR PROCESSAMENT", height=40, width=250, font=self.font_button, command=self._iniciar_pipeline)
        self.btn_ejecutar.pack(pady=25)

        self.lbl_vivo = ctk.CTkLabel(self.tab_pipeline, text="📡 Tracker d'Extracció en Viu", font=self.font_subtitle, text_color="#00D2FF")
        self.lbl_vivo.pack(anchor="w", padx=20)
        
        self.caja_vivo = ctk.CTkTextbox(self.tab_pipeline, height=100, font=self.font_console, text_color="#FFFFFF", fg_color="#2b2b2b", corner_radius=8)
        self.caja_vivo.pack(fill='x', padx=20, pady=(0, 15))
        self.caja_vivo.insert("0.0", "Esperant inici d'extracció...\n")
        self.caja_vivo.configure(state='disabled')

        ctk.CTkLabel(self.tab_pipeline, text="Consola d'Operacions", font=self.font_subtitle).pack(anchor="w", padx=20)
        self.consola = ctk.CTkTextbox(self.tab_pipeline, font=self.font_console, text_color="#A9B7C6", fg_color="#1E1E1E")
        self.consola.pack(fill="both", expand=True, padx=20, pady=(5, 20))

    def _construir_tab_bd(self):
        # =========================================================
        # SECCIÓ 1: AFEGIR NOU CREADOR
        # =========================================================
        frame_add = ctk.CTkFrame(self.tab_bd)
        frame_add.pack(fill="x", padx=20, pady=(15, 5))
        
        ctk.CTkLabel(frame_add, text="➕ Afegir Nou Creador:", font=self.font_subtitle).pack(side="left", padx=15, pady=15)
        
        self.entrada_nou_creador = ctk.CTkEntry(frame_add, width=250, placeholder_text="Nom del nou creador...")
        self.entrada_nou_creador.pack(side="left", padx=10, pady=15)
        
        btn_add = ctk.CTkButton(frame_add, text="CREAR PERFIL", font=self.font_button, width=120, command=self._crear_nou_creador)
        btn_add.pack(side="left", padx=10, pady=15)

        # =========================================================
        # SECCIÓ 2: MODIFICAR O ELIMINAR CREADOR EXISTENT
        # =========================================================
        frame_edicion = ctk.CTkFrame(self.tab_bd)
        frame_edicion.pack(fill="both", expand=True, padx=20, pady=10)
        
        # --- PANNELL ESQUERRE: Cercador i Llista robusta ---
        frame_lista = ctk.CTkFrame(frame_edicion, fg_color="transparent")
        frame_lista.pack(side="left", fill="y", padx=15, pady=15)
        
        ctk.CTkLabel(frame_lista, text="🔍 Buscar i Seleccionar:", font=self.font_subtitle).pack(anchor="w", pady=(0, 5))
        
        self.entrada_buscador = ctk.CTkEntry(frame_lista, width=250, placeholder_text="Escriu per a filtrar...")
        self.entrada_buscador.pack(pady=(0, 10))
        self.entrada_buscador.bind("<KeyRelease>", self._filtrar_lista)
        
        frame_listbox = ctk.CTkFrame(frame_lista)
        frame_listbox.pack(fill="both", expand=True)
        
        self.lista_creadores = tk.Listbox(
            frame_listbox, bg="#343638", fg="white", font=("Segoe UI", 11),
            selectbackground="#1f538d", selectforeground="white", 
            borderwidth=0, highlightthickness=0
        )
        self.lista_creadores.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=5)
        self.lista_creadores.bind("<<ListboxSelect>>", self._on_select_creador)
        
        scrollbar = ctk.CTkScrollbar(frame_listbox, command=self.lista_creadores.yview)
        scrollbar.pack(side="right", fill="y", padx=(0, 5), pady=5)
        self.lista_creadores.configure(yscrollcommand=scrollbar.set)
        
        # --- PANNELL DRET: Camps d'edició i botons d'acció ---
        frame_campos = ctk.CTkFrame(frame_edicion, fg_color="transparent")
        frame_campos.pack(side="right", fill="both", expand=True, padx=15, pady=15)
        
        ctk.CTkLabel(frame_campos, text="✏️ Dades del Perfil (Nicknames)", font=self.font_subtitle).pack(pady=(0, 20))

        self.entradas = {}
        redes = [('instagram', 'Instagram'), ('tiktok', 'TikTok'), ('youtube', 'YouTube'), ('twitch', 'Twitch')]
        
        for col_name, display_name in redes:
            row_frame = ctk.CTkFrame(frame_campos, fg_color="transparent")
            row_frame.pack(fill="x", padx=20, pady=10)
            
            ctk.CTkLabel(row_frame, text=display_name + ":", width=100, anchor="w", font=self.font_body).pack(side="left")
            
            entrada = ctk.CTkEntry(row_frame, width=300)
            entrada.pack(side="left", padx=10)
            self.entradas[col_name] = entrada

        # Contenidor per als botons Guardar i Eliminar
        frame_botones_bd = ctk.CTkFrame(frame_campos, fg_color="transparent")
        frame_botones_bd.pack(pady=30)

        self.btn_guardar_bd = ctk.CTkButton(frame_botones_bd, text="GUARDAR CANVIS", height=40, font=self.font_button, command=self._guardar_cambios_creador)
        self.btn_guardar_bd.pack(side="left", padx=10)

        self.btn_eliminar_bd = ctk.CTkButton(frame_botones_bd, text="ELIMINAR PERFIL", height=40, font=self.font_button, fg_color="#E63946", hover_color="#C1121F", command=self._eliminar_creador)
        self.btn_eliminar_bd.pack(side="left", padx=10)

        # Omplim la llista a l'arrancar
        self._actualitzar_llista()

    def _crear_nou_creador(self):
        nou_nom = self.entrada_nou_creador.get().strip()
        if not nou_nom:
            messagebox.showwarning("Atenció", "El nom del creador no pot estar buit.")
            return
            
        if not self.df_creadores.empty and nou_nom.lower() in self.df_creadores['creador'].str.lower().values:
            messagebox.showwarning("Atenció", f"El creador '{nou_nom}' ja existeix a la Base de Dades.")
            return
            
        nova_fila = {
            'creador': nou_nom, 
            'nick_instagram': None, 
            'nick_tiktok': None, 
            'nick_youtube': None, 
            'nick_twitch': None
        }
        
        self.df_creadores = pd.concat([self.df_creadores, pd.DataFrame([nova_fila])], ignore_index=True)
        self._guardar_creadores()
        
        self._actualitzar_llista()
        self.entrada_nou_creador.delete(0, 'end')
        messagebox.showinfo("Èxit", f"S'ha creat el perfil de '{nou_nom}' correctament. Selecciona'l a la llista per afegir els seus nicks.")

    def _actualitzar_llista(self, filtre=""):
        self.lista_creadores.delete(0, tk.END)
        noms = self.df_creadores['creador'].dropna().unique().tolist() if not self.df_creadores.empty else []
        noms.sort(key=str.lower)
        
        for nom in noms:
            if filtre.lower() in nom.lower():
                self.lista_creadores.insert(tk.END, nom)

    def _filtrar_lista(self, event):
        text_busqueda = self.entrada_buscador.get()
        self._actualitzar_llista(text_busqueda)

    def _on_select_creador(self, event):
        seleccion = self.lista_creadores.curselection()
        if not seleccion:
            return
            
        nom_creador = self.lista_creadores.get(seleccion[0])
        self.creador_actual = nom_creador
        self._cargar_datos_creador(nom_creador)

    def _cargar_datos_creador(self, nom_creador):
        datos = self.df_creadores[self.df_creadores['creador'] == nom_creador]
        if not datos.empty:
            fila = datos.iloc[0]
            for red, entrada in self.entradas.items():
                entrada.delete(0, 'end')
                valor = fila.get(f'nick_{red}', '')
                if pd.notna(valor):
                    entrada.insert(0, str(valor))

    def _guardar_cambios_creador(self):
        if not self.creador_actual:
            messagebox.showwarning("Atenció", "Si us plau, selecciona un creador de la llista abans de guardar.")
            return

        creador_seleccionado = self.creador_actual

        if self.df_creadores.empty:
            return

        mascara = self.df_creadores['creador'] == creador_seleccionado
        if not mascara.any():
            return

        cambios_realizados = False
        resumen_cambios = []
        fila_actual = self.df_creadores[mascara].iloc[0]

        confirmacion = messagebox.askyesno("Confirmar Canvis", f"Estàs segur que vols guardar els nous nicks per a '{creador_seleccionado}'?")
        if not confirmacion:
            return

        for red, entrada in self.entradas.items():
            nuevo_valor = entrada.get().strip()
            if nuevo_valor == "": nuevo_valor = None
            
            col_name = f'nick_{red}'
            valor_antiguo = fila_actual.get(col_name)
            
            if pd.isna(valor_antiguo): valor_antiguo = None
                
            if nuevo_valor != valor_antiguo:
                self.df_creadores.loc[mascara, col_name] = nuevo_valor
                cambios_realizados = True
                
                filas_modificadas = self._actualizar_historico_nick(red, col_name, creador_seleccionado, nuevo_valor)
                resumen_cambios.append(f"• {red.capitalize()}: '{valor_antiguo}' -> '{nuevo_valor}' ({filas_modificadas} registres històrics actualitzats)")

        if cambios_realizados:
            self._guardar_creadores()
            mensaje_exito = f"El perfil '{creador_seleccionado}' ha sigut actualitzat exitosament.\n\nImpacte en Base de Dades:\n"
            mensaje_exito += "\n".join(resumen_cambios) if resumen_cambios else "No s'han detectat registres històrics que requerisquen propagació de canvis."
            messagebox.showinfo("Procés Completat", mensaje_exito)
        else:
            messagebox.showinfo("Informació", "No s'han detectat modificacions en els camps proporcionats.")

    def _eliminar_creador(self):
        if not self.creador_actual:
            messagebox.showwarning("Atenció", "Si us plau, selecciona un creador de la llista per a eliminar-lo.")
            return

        confirmacio = messagebox.askyesno("Eliminar Perfil", f"⚠️ Estàs segur que vols eliminar completament a '{self.creador_actual}'?\n\nJa no s'extrauran dades d'este creador en les futures execucions.")
        
        if confirmacio:
            # Eliminar del DataFrame
            self.df_creadores = self.df_creadores[self.df_creadores['creador'] != self.creador_actual].reset_index(drop=True)
            self._guardar_creadores()
            
            # Netejar la selecció i els camps visuals
            self.creador_actual = None
            for entrada in self.entradas.values():
                entrada.delete(0, 'end')
            
            # Actualitzar la llista aplicant el filtre que estiguera actiu
            self._actualitzar_llista(self.entrada_buscador.get())
            
            messagebox.showinfo("Èxit", "El perfil ha sigut eliminat correctament.")

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
                print(f"[ERROR] Fallada a l'intentar actualitzar històrics de {red_social}: {e}")
                
        return filas_afectadas

    # --- PIPELINE METHODS ---
    def _iniciar_pipeline(self):
        self.btn_ejecutar.configure(state="disabled")
        self.consola.configure(state="normal")
        self.consola.delete("0.0", "end")
        self.consola.configure(state="disabled")
        
        self.caja_vivo.configure(state="normal")
        self.caja_vivo.delete("0.0", "end")
        self.caja_vivo.insert("0.0", "SISTEMA PREPARAT.\n")
        self.caja_vivo.configure(state="disabled")

        hilo = threading.Thread(target=self._hilo_pipeline)
        hilo.daemon = True
        hilo.start()

    def manejar_evento_extractor(self, red, creador, accion):
        self.after(0, self._actualizar_panel_vivo, red, creador, accion)

    def _actualizar_panel_vivo(self, red, creador, accion):
        if accion == "start":
            if creador not in self.estado_vivo[red]:
                self.estado_vivo[red].append(creador)
        elif accion == "done":
            if creador in self.estado_vivo[red]:
                self.estado_vivo[red].remove(creador)

        lineas_texto = []
        for plataforma, creadores in self.estado_vivo.items():
            for c in creadores:
                lineas_texto.append(f"[{plataforma}] extraient de {c}")
        
        texto_final = "\n".join(lineas_texto)
        if not texto_final.strip():
            texto_final = "✅ Extracció finalitzada o en espera."

        self.caja_vivo.configure(state="normal")
        self.caja_vivo.delete("0.0", "end")
        self.caja_vivo.insert("0.0", texto_final)
        self.caja_vivo.configure(state="disabled")

    def _hilo_pipeline(self):
        extraer = self.var_extraer.get()
        limpiar = self.var_limpiar.get()
        redes_activas = [red for red, var in self.redes_vars.items() if var.get()]

        sys.stdout = TextRedirector(self.consola)

        try:
            if not extraer and not limpiar:
                print("[ERROR] No s'ha seleccionat cap fase del procés.")
                return
            if not redes_activas:
                print("[ERROR] No s'ha seleccionat cap font de dades.")
                return

            print("==================================================")
            print("[SISTEMA] Iniciant pipeline de processament")
            print(f"[SISTEMA] Fases actives: {'Extracció ' if extraer else ''}{'- Neteja' if limpiar else ''}")
            print(f"[SISTEMA] Fonts objectiu: {', '.join(redes_activas).upper()}")
            print("==================================================\n")
            
            if extraer:
                print("[PROCÉS] Iniciant fase d'extracció de dades en PARAL·LEL...")
                extractor = Extractor(ui_callback=self.manejar_evento_extractor)
                
                if 'instagram' not in redes_activas: extractor.df_instagram = pd.DataFrame()
                if 'tiktok' not in redes_activas: extractor.df_tiktok = pd.DataFrame()
                if 'youtube' not in redes_activas: extractor.df_youtube = pd.DataFrame()
                if 'twitch' not in redes_activas: extractor.df_twitch = pd.DataFrame()
                
                extractor.extraction()
                    
                print("[INFO] Fase d'extracció finalitzada correctament.\n")

            if limpiar:
                print("[PROCÉS] Iniciant fase de neteja i consolidació...")
                cleaner = Cleaner()
                
                if 'instagram' in redes_activas: cleaner._limpiar_instagram()
                if 'tiktok' in redes_activas: cleaner._limpiar_tiktok()
                if 'youtube' in redes_activas: cleaner._limpiar_youtube()
                if 'twitch' in redes_activas: cleaner._limpiar_twitch()
                    
                print("[INFO] Fase de neteja finalitzada correctament.\n")

            print("==================================================")
            print("[SISTEMA] Procés completat sense errors crítics.")
            print("==================================================")
            
        except Exception as e:
            print(f"\n[ERROR CRÍTIC] Excepció capturada durant l'execució: {e}")
        finally:
            sys.stdout = sys.__stdout__
            self.after(0, self._finalizar_interfaz_pipeline)

    def _finalizar_interfaz_pipeline(self):
        self.btn_ejecutar.configure(state="normal")
        messagebox.showinfo("Notificacions", "Procés de Pipeline finalitzat. Revisa la consola per a detalls.")


if __name__ == "__main__":
    app = PipelineApp()
    app.mainloop()