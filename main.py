import pandas as pd
import os
import numpy as np
import re
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font

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

def parse_fecha_manual(s):
    try:
        s = str(s).strip()
        # Caso 1: año de 4 dígitos
        if re.match(r'^\d{4}$', s):
            year = int(s)
            return pd.Timestamp(year=year, month=12, day=31)
        # Caso 2: trimestre (I 24, II 24, etc.)
        elif re.match(r'^[IVXLCDM]+\s+\d{2}$', s):
            partes = s.split()
            trimestre = partes[0]
            año_corto = int(partes[1])
            año = 2000 + año_corto
            if trimestre == 'I':
                return pd.Timestamp(año, 3, 31)
            elif trimestre == 'II':
                return pd.Timestamp(año, 6, 30)
            elif trimestre == 'III':
                return pd.Timestamp(año, 9, 30)
            elif trimestre == 'IV':
                return pd.Timestamp(año, 12, 31)
            else:
                return pd.NaT
        # Caso 3: meses (Ene-24, Ene 24, etc.)
        else:
            meses_esp = {
                'Ene': 'Jan', 'Feb': 'Feb', 'Mar': 'Mar', 'Abr': 'Apr', 
                'May': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Ago': 'Aug', 
                'Sep': 'Sep', 'Oct': 'Oct', 'Nov': 'Nov', 'Dic': 'Dec'
            }
            for mes_esp, mes_eng in meses_esp.items():
                if s.startswith(mes_esp):
                    s_eng = s.replace(mes_esp, mes_eng)
                    fecha = pd.to_datetime(s_eng, format='%b%y', errors='coerce')
                    if not pd.isna(fecha):
                        return fecha + pd.offsets.MonthEnd(0)
            return pd.to_datetime(s, errors='coerce')
    except:
        return pd.NaT

def extraer_serie_excel(id_serie, excel_data):
    id_serie = str(id_serie)
    for sheet_name, df in excel_data.items():
        # Convertir todo a string para la búsqueda
        df_str = df.astype(str)
        # Buscar el ID en todo el DataFrame
        mask = df_str == id_serie
        if mask.any().any():
            filas, cols = np.where(mask)
            fila = filas[0]
            col = cols[0]
            
            valores = []
            fechas = []
            r = fila + 1
            
            # Extraer datos hasta encontrar celda vacía
            while r < df.shape[0] and not pd.isna(df.iat[r, col]):
                fecha_val = df.iat[r, 0]  # Columna A
                valor_val = df.iat[r, col] # Columna de la serie
                
                # Manejar valores numéricos
                try:
                    valor_num = float(valor_val)
                    valores.append(valor_num)
                except (ValueError, TypeError):
                    valores.append(valor_val)
                
                fechas.append(fecha_val)
                r += 1
            
            # Convertir fechas
            fechas_dt = pd.to_datetime(fechas, errors='coerce')
            # Aplicar parser manual para las que no se convirtieron
            if fechas_dt.isna().any():
                fechas_manual = [parse_fecha_manual(f) for f in fechas]
                fechas_dt = pd.DatetimeIndex(fechas_manual)
            
            return fechas_dt, valores
    
    raise ValueError(f"ID {id_serie} no encontrado")

def procesar_datos():
    try:
        crear_carpeta_logs()
        escribir_log("SISTEMA", "INICIO", "Inicio del proceso de extracción")
        
        # Cargar archivo Excel con los códigos
        codigos_file = "Codigos.xlsx"
        wb = load_workbook(codigos_file)
        ws = wb.active
        red_font = Font(color="FF0000")
        
        try:
            df_codigos = pd.read_excel(
                codigos_file,
                usecols=["ID", "Pestaña BD", "Serie"]
            )
            df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].fillna("Otros")
            df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].astype(str)
        except Exception as e:
            error_msg = f"Error al leer Codigos.xlsx: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            return

        # Cargar archivo con los datos
        try:
            excel_file = "actividad_ied.xlsx"
            excel_data = {}
            with pd.ExcelFile(excel_file) as xls:
                for sheet_name in xls.sheet_names:
                    # Leer sin encabezados
                    excel_data[sheet_name] = pd.read_excel(xls, sheet_name, header=None)
            print(f"Excel '{excel_file}' cargado correctamente con {len(excel_data)} hojas")
        except Exception as e:
            error_msg = f"Error al leer {excel_file}: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            return

        # Cargar o inicializar BD
        try:
            bd_hojas = pd.read_excel("BD.xlsx", sheet_name=None)
            # Asegurar que todas las hojas tengan columna 'fecha'
            for hoja in bd_hojas:
                if 'fecha' not in bd_hojas[hoja].columns:
                    bd_hojas[hoja]['fecha'] = pd.NaT
        except FileNotFoundError:
            bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}
        except Exception as e:
            error_msg = f"Error al leer BD.xlsx: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}

        total_series = len(df_codigos)
        series_exitosas = 0
        series_fallidas = 0
        filas_con_error = []

        for idx, fila in df_codigos.iterrows():
            series_id = fila["ID"]
            pestaña = str(fila["Pestaña BD"]).strip() or "Otros"
            serie_nombre = fila["Serie"]
            
            print(f"\nProcesando: {serie_nombre} (ID: {series_id}) - Hoja: '{pestaña}'")
            
            try:
                # Extraer datos del Excel local
                categorias_api, valores_api = extraer_serie_excel(series_id, excel_data)
                
                # Crear DataFrame con los datos
                df_api = pd.DataFrame({
                    'fecha': categorias_api,  # Ya es DatetimeIndex
                    serie_nombre: valores_api
                })
                
                print(f"Datos extraídos: {len(df_api)} registros")
                escribir_log(series_id, "OK", f"Registros extraídos: {len(df_api)}")
                series_exitosas += 1
            except Exception as e:
                error_msg = str(e)
                print(f"Error al obtener datos: {error_msg}")
                escribir_log(series_id, "ERROR", error_msg)
                series_fallidas += 1
                filas_con_error.append(idx + 2)
                continue

            # Procesar la hoja de destino en BD
            if pestaña not in bd_hojas:
                bd_hojas[pestaña] = pd.DataFrame(columns=['fecha'])
            
            # Convertir fechas a datetime si no lo están
            if 'fecha' in bd_hojas[pestaña].columns:
                bd_hojas[pestaña]['fecha'] = pd.to_datetime(bd_hojas[pestaña]['fecha'])

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

            # Limpiar y ordenar
            df_merged = df_merged.sort_values('fecha').drop_duplicates('fecha', keep='first')
            bd_hojas[pestaña] = df_merged
            print(f"Registros totales en BD: {len(df_merged)}")

        # Guardar BD actualizada
        try:
            with pd.ExcelWriter("BD.xlsx", engine='openpyxl') as writer:
                for hoja, datos in bd_hojas.items():
                    hoja_str = str(hoja).strip()[:31]
                    if not hoja_str:
                        hoja_str = "Otros"
                    datos.to_excel(writer, sheet_name=hoja_str, index=False)
            
            # Marcar errores en Codigos.xlsx
            if filas_con_error:
                for fila in filas_con_error:
                    for col in range(1, ws.max_column + 1):
                        ws.cell(row=fila, column=col).font = red_font
                wb.save(codigos_file)
                print(f"\nSe marcaron en rojo las filas con error: {filas_con_error}")
            
            resumen = f"Proceso completado. Series: {total_series} | Exitosas: {series_exitosas} | Fallidas: {series_fallidas}"
            print(f"\n{resumen}")
            escribir_log("SISTEMA", "FIN", resumen)
        except Exception as e:
            error_msg = f"Error al guardar archivos: {str(e)}"
            print(f"\n{error_msg}")
            escribir_log("SISTEMA", "ERROR", error_msg)

    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario")
        escribir_log("SISTEMA", "INTERRUMPIDO", "Proceso detenido manualmente")
    except Exception as e:
        print(f"\nError inesperado: {str(e)}")
        escribir_log("SISTEMA", "ERROR CRITICO", str(e))

if __name__ == "__main__":
    procesar_datos()