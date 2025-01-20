# src/ui/visual.py

import streamlit as st
import pandas as pd
from src.funcion.data_processing import cargar_logs


def render_ui():
    """
    Renderiza la interfaz de usuario de la aplicación.
    """
    st.title("📂 Procesamiento de Archivos ATIA (.log)")
    st.markdown("""
        Esta aplicación permite subir múltiples archivos `.log`, procesarlos de manera eficiente y visualizar una muestra de los datos en una tabla.
    """)

    # Subida de archivos
    archivos_subidos = st.file_uploader(
        "Selecciona uno o más archivos `.log` para subir",
        type=["log"],
        accept_multiple_files=True
    )

    if archivos_subidos:
        # Generar una clave única para los archivos subidos (nombre + tamaño)
        uploaded_files_key = "_".join([f"{file.name}_{file.size}" for file in archivos_subidos])

        # Verificar si los archivos han cambiado
        if 'uploaded_files_key' not in st.session_state or st.session_state.uploaded_files_key != uploaded_files_key:
            # Actualizar la clave en Session State
            st.session_state.uploaded_files_key = uploaded_files_key
            # Resetear el DataFrame almacenado
            st.session_state.df_completo = None

        # Si el DataFrame no está procesado, procesarlo
        if st.session_state.df_completo is None:
            total_archivos = len(archivos_subidos)
            st.info(f"{total_archivos} archivo(s) `.log` subido(s) exitosamente.")

            # Inicializar la barra de progreso
            progress_bar = st.progress(0)
            status_text = st.empty()

            datos = []

            for idx, archivo in enumerate(archivos_subidos):
                # Actualizar el estado
                status_text.text(f"Procesando archivo {idx + 1} de {total_archivos}: {archivo.name}")

                # Procesar el archivo
                registros = cargar_logs([archivo])
                datos.append(registros)

                # Actualizar la barra de progreso
                progress = (idx + 1) / total_archivos
                progress_bar.progress(progress)

            # Concatenar todos los DataFrames
            if datos:
                df_completo = pd.concat(datos, ignore_index=True)
                if not df_completo.empty:
                    st.success("¡Archivos `.log` procesados con éxito!")
                    # Almacenar el DataFrame en Session State
                    st.session_state.df_completo = df_completo
                else:
                    st.warning("No se cargaron datos. Verifica el formato de los archivos `.log`.")
            else:
                st.warning("No se cargaron datos de los archivos.")

        # Mostrar botón para visualizar DataFrame si está procesado
        if st.session_state.df_completo is not None:
            if 'mostrar_df' not in st.session_state:
                st.session_state.mostrar_df = False

            # Crear un botón toggle para mostrar/ocultar el DataFrame
            mostrar_df = st.button("Mostrar DF")
            if mostrar_df:
                st.session_state.mostrar_df = True

            if st.session_state.mostrar_df:
                st.subheader("Vista Previa del DataFrame")
                # Si el DataFrame es muy grande, mostrar solo las primeras N filas
                MAX_ROWS_DISPLAY = 1000
                df_completo = st.session_state.df_completo
                if len(df_completo) > MAX_ROWS_DISPLAY:
                    st.warning(
                        f"El DataFrame tiene {len(df_completo)} filas. Mostrando las primeras {MAX_ROWS_DISPLAY}.")
                    st.dataframe(df_completo.head(MAX_ROWS_DISPLAY))
                else:
                    st.dataframe(df_completo)
    else:
        # Si no hay archivos subidos, pero hay un DataFrame en Session State, opcionalmente puedes mostrarlo
        pass
