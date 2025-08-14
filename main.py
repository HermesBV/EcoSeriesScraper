import pandas as pd
import os
import requests
import time
from datetime import datetime
from aspiradora import obtener_datos_api as api

def crear_carpeta_logs():
    if not os.path.exists('logs'):
        os.makedirs('logs')

def escribir_log(id_serie, estado, mensaje=""):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"logs/log_{datetime.now().strftime('%Y%m%d')}.txt"
    with open(log_file, 'a') as f:
        f.write(f"{timestamp}|{id_serie}|{estado}")
        if mensaje:
            mensaje_limpio = mensaje.replace('\n', ' || ')
            f.write(f"|{mensaje_limpio}")
        f.write("\n")

def procesar_datos():
    crear_carpeta_logs()
    escribir_log("SISTEMA", "INICIO", "Inicio del proceso de descarga")
    
    try:
        # Leer archivo y manejar valores nulos en Pestaña BD
        df_codigos = pd.read_excel(
            "Codigos.xlsx",
            usecols=["ID", "Pestaña BD", "Serie"]
        )
        
        # Asignar "Otros" a las filas sin Pestaña BD
        df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].fillna("Otros")
        df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].astype(str)
        
    except Exception as e:
        error_msg = f"Error al leer archivo Codigos.xlsx: {str(e)}"
        print(error_msg)
        escribir_log("SISTEMA", "ERROR", error_msg)
        return

    try:
        bd_hojas = pd.read_excel("BD.xlsx", sheet_name=None, parse_dates=['fecha'])
    except FileNotFoundError:
        bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}
    except Exception as e:
        error_msg = f"Error al leer archivo BD.xlsx: {str(e)}"
        print(error_msg)
        escribir_log("SISTEMA", "ERROR", error_msg)
        bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}

    total_series = len(df_codigos)
    series_exitosas = 0
    series_fallidas = 0

    for _, fila in df_codigos.iterrows():
        series_id = fila["ID"]
        pestaña = str(fila["Pestaña BD"]).strip() or "Otros"  # Asegurar nombre válido
        serie_nombre = fila["Serie"]
        
        print(f"\nProcesando: {serie_nombre} (ID: {series_id}) - Hoja: '{pestaña}'")
        
        try:
            categorias_api, valores_api = api(series_id)
            df_api = pd.DataFrame({
                'fecha': pd.to_datetime(categorias_api).dt.date,
                serie_nombre: valores_api
            })
            print(f"Datos descargados: {len(df_api)} registros")
            escribir_log(series_id, "OK", f"Registros descargados: {len(df_api)}")
            series_exitosas += 1
        except Exception as e:
            error_msg = str(e)
            print(f"Error al obtener datos: {error_msg}")
            escribir_log(series_id, "ERROR", error_msg)
            series_fallidas += 1
            continue

        # Inicializar hoja si no existe
        if pestaña not in bd_hojas:
            bd_hojas[pestaña] = pd.DataFrame(columns=['fecha'])
        
        # Asegurar formato de fecha
        bd_hojas[pestaña]['fecha'] = pd.to_datetime(bd_hojas[pestaña]['fecha']).dt.date

        # Fusionar datos
        df_merged = pd.merge(
            bd_hojas[pestaña],
            df_api,
            on='fecha',
            how='outer',
            suffixes=('_existente', '_nuevo')
        )

        # Actualizar valores
        if f"{serie_nombre}_nuevo" in df_merged.columns:
            mask = df_merged[f"{serie_nombre}_nuevo"].notna()
            df_merged.loc[mask, serie_nombre] = df_merged.loc[mask, f"{serie_nombre}_nuevo"]
            df_merged.drop(columns=[f"{serie_nombre}_nuevo"], inplace=True, errors='ignore')

        if f"{serie_nombre}_existente" in df_merged.columns:
            df_merged.drop(columns=[f"{serie_nombre}_existente"], inplace=True, errors='ignore')

        # Ordenar y guardar
        df_merged = df_merged.sort_values('fecha').drop_duplicates('fecha', keep='first')
        bd_hojas[pestaña] = df_merged
        
        print(f"Registros totales en BD: {len(df_merged)}")

    # Guardar resultados
    try:
        with pd.ExcelWriter("BD.xlsx", engine='openpyxl') as writer:
            for hoja, datos in bd_hojas.items():
                hoja_str = str(hoja).strip()[:31]
                if not hoja_str:  # Si por algún motivo está vacío
                    hoja_str = "Otros"
                datos.to_excel(writer, sheet_name=hoja_str, index=False)
        
        resumen = f"Proceso completado. Series: {total_series} | Exitosas: {series_exitosas} | Fallidas: {series_fallidas}"
        print(f"\n{resumen}")
        escribir_log("SISTEMA", "FIN", resumen)
    except Exception as e:
        error_msg = f"Error al guardar el archivo BD.xlsx: {str(e)}"
        print(f"\n{error_msg}")
        escribir_log("SISTEMA", "ERROR", error_msg)

if __name__ == "__main__":
    procesar_datos()