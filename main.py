import pandas as pd
import os
import numpy as np
import re
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font
import requests
import urllib3
from tqdm import tqdm
import warnings

# Configuración inicial
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Diccionario de URLs y nombres de archivos
EXCEL_URLS = {
    "empleo_ingresos.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice3a.xlsx",
    "sector_externo.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice5.xlsx",
    "internacional.xlsx": "https://www.economia.gob.ar/download/infoeco/internacional_ied.xlsx",
    "dinero_bancos.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice8.xlsx",
    "precios.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice4.xlsx",
    "finanzas.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice-financiero.xlsx",
    "finanzas_publicas.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice6.xlsx",
    "actividad.xlsx": "https://www.economia.gob.ar/download/infoeco/actividad_ied.xlsx"
}

def crear_carpeta_logs():
    """Crea la carpeta de logs si no existe"""
    if not os.path.exists('logs'):
        os.makedirs('logs')

def crear_carpeta_excels():
    """Crea la carpeta para los excels si no existe"""
    if not os.path.exists('Excels_IED'):
        os.makedirs('Excels_IED')

def descargar_excels():
    """Descarga todos los archivos Excel sobrescribiendo los existentes"""
    crear_carpeta_excels()
    descargados = {}
    
    for nombre_archivo, url in tqdm(EXCEL_URLS.items(), desc="Descargando archivos"):
        destino = os.path.join('Excels_IED', nombre_archivo)
        
        try:
            response = requests.get(url, stream=True, verify=False, timeout=30)
            response.raise_for_status()
            
            with open(destino, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filtrar chunks vacíos
                        f.write(chunk)
            
            descargados[nombre_archivo] = destino
            print(f"\nDescargado: {nombre_archivo}")
            
        except Exception as e:
            error_msg = f"Error al descargar {nombre_archivo}: {str(e)}"
            print(f"\n{error_msg}")
            escribir_log("SISTEMA", "ERROR_DESCARGA", error_msg)
    
    return descargados

def escribir_log(id_serie, estado, mensaje=""):
    """Escribe un mensaje en el archivo de log"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"logs/log_{datetime.now().strftime('%Y%m%d')}.txt"
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp}|{id_serie}|{estado}")
        if mensaje:
            mensaje_limpio = mensaje.replace('\n', ' || ')
            f.write(f"|{mensaje_limpio}")
        f.write("\n")

def parse_fechas(fechas):
    """Función mejorada para el parseo de fechas con manejo específico de formatos"""
    # Primero intentamos con formatos conocidos
    formatos_conocidos = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
        '%b-%y', '%b %Y', '%m-%Y',
        '%Y',
        'I %y', 'II %y', 'III %y', 'IV %y'
    ]
    
    for fmt in formatos_conocidos:
        try:
            return pd.to_datetime(fechas, format=fmt, errors='raise')
        except:
            continue
    
    # Si no funciona con formatos conocidos, aplicamos el parser manual
    fechas_parseadas = []
    for fecha in fechas:
        fecha_parseada = parse_fecha_manual(fecha)
        fechas_parseadas.append(fecha_parseada if not pd.isna(fecha_parseada) else pd.NaT)
    
    return pd.DatetimeIndex(fechas_parseadas)

def parse_fecha_manual(s):
    """Parser manual para formatos de fecha no estándar"""
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
    """Busca y extrae una serie de datos de los excels cargados"""
    id_serie = str(id_serie)
    for sheet_name, df in excel_data.items():
        df_str = df.astype(str)
        mask = df_str == id_serie
        if mask.any().any():
            filas, cols = np.where(mask)
            fila = filas[0]
            col = cols[0]
            
            valores = []
            fechas = []
            r = fila + 1
            
            while r < df.shape[0] and not pd.isna(df.iat[r, col]):
                fecha_val = df.iat[r, 0]
                valor_val = df.iat[r, col]
                
                try:
                    valor_num = float(valor_val)
                    valores.append(valor_num)
                except (ValueError, TypeError):
                    valores.append(valor_val)
                
                fechas.append(fecha_val)
                r += 1
            
            fechas_dt = parse_fechas(fechas)
            
            return fechas_dt, valores
    
    raise ValueError(f"ID {id_serie} no encontrado")

def cargar_excel_completo(ruta):
    """Carga un archivo Excel completo con todas sus hojas"""
    excel_data = {}
    try:
        with pd.ExcelFile(ruta) as xls:
            for sheet_name in xls.sheet_names:
                excel_data[sheet_name] = pd.read_excel(xls, sheet_name, header=None)
        return excel_data
    except Exception as e:
        raise ValueError(f"Error al cargar archivo {os.path.basename(ruta)}: {str(e)}")

def procesar_datos():
    """Función principal que orquesta todo el proceso"""
    try:
        crear_carpeta_logs()
        escribir_log("SISTEMA", "INICIO", "Inicio del proceso de extracción")
        
        # Descargar todos los excels (siempre sobrescribiendo)
        archivos_descargados = descargar_excels()
        if not archivos_descargados:
            raise Exception("No se pudo descargar ningún archivo Excel")
        
        # Cargar archivo Excel con los códigos
        codigos_file = "Codigos.xlsx"
        try:
            wb = load_workbook(codigos_file)
            ws = wb.active
            red_font = Font(color="FF0000")
            
            df_codigos = pd.read_excel(
                codigos_file,
                usecols=["ID", "Pestaña BD", "Serie"],
                dtype={"ID": str, "Pestaña BD": str, "Serie": str}
            )
            # Asegurar que Pestaña BD no tenga valores NaN o 'nan'
            df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].fillna("Otros")
            df_codigos["Pestaña BD"] = df_codigos["Pestaña BD"].replace({"nan": "Otros", "": "Otros"})
            
            # Asegurar columna Ubicación
            if "Ubicación" not in df_codigos.columns:
                df_codigos["Ubicación"] = ""
            df_codigos["Ubicación"] = df_codigos["Ubicación"].astype(str)
            
        except Exception as e:
            error_msg = f"Error al leer Codigos.xlsx: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            return

        # Cargar o inicializar BD
        try:
            bd_hojas = pd.read_excel("BD.xlsx", sheet_name=None)
            
            # Migrar datos de hojas 'nan' a 'Otros'
            for hoja in list(bd_hojas.keys()):  # Usamos list() para evitar RuntimeError
                if pd.isna(hoja) or str(hoja).strip().lower() in ["nan", ""]:
                    datos = bd_hojas.pop(hoja)
                    if "Otros" not in bd_hojas:
                        bd_hojas["Otros"] = datos
                    else:
                        bd_hojas["Otros"] = pd.concat([bd_hojas["Otros"], datos])
            
            # Asegurar columna 'fecha' en todas las hojas
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
        ids_no_encontrados = set(df_codigos["ID"].unique())
        ubicaciones = {}  # Diccionario para guardar ubicaciones de los IDs

        # Procesar cada archivo Excel descargado
        for nombre_archivo, ruta_archivo in archivos_descargados.items():
            print(f"\nProcesando archivo: {nombre_archivo}")
            
            try:
                excel_data = cargar_excel_completo(ruta_archivo)
                print(f"Excel '{nombre_archivo}' cargado correctamente con {len(excel_data)} hojas")
                
                ids_a_buscar = list(ids_no_encontrados)
                if not ids_a_buscar:
                    print("Todos los IDs ya fueron encontrados en archivos anteriores")
                    break
                    
                for id_serie in tqdm(ids_a_buscar, desc=f"Buscando IDs en {nombre_archivo}"):
                    try:
                        mask = df_codigos["ID"].astype(str) == id_serie
                        fila_idx = df_codigos[mask].index[0]
                        fila_codigo = df_codigos.loc[fila_idx]
                        
                        # Asegurar que pestaña sea válida o "Otros"
                        pestaña = str(fila_codigo["Pestaña BD"]).strip()
                        pestaña = "Otros" if not pestaña or pestaña.lower() == "nan" else pestaña
                        serie_nombre = fila_codigo["Serie"]
                        
                        categorias_api, valores_api = extraer_serie_excel(id_serie, excel_data)
                        
                        # Crear DataFrame con los datos
                        df_api = pd.DataFrame({
                            'fecha': categorias_api,
                            serie_nombre: valores_api
                        })
                        
                        # Actualizar la BD
                        if pestaña not in bd_hojas:
                            bd_hojas[pestaña] = pd.DataFrame(columns=['fecha'])
                        
                        bd_hojas[pestaña]['fecha'] = pd.to_datetime(bd_hojas[pestaña]['fecha'])

                        df_merged = pd.merge(
                            bd_hojas[pestaña],
                            df_api,
                            on='fecha',
                            how='outer',
                            suffixes=('_existente', '_nuevo')
                        )

                        if f"{serie_nombre}_nuevo" in df_merged.columns:
                            mask = df_merged[f"{serie_nombre}_nuevo"].notna()
                            df_merged.loc[mask, serie_nombre] = df_merged.loc[mask, f"{serie_nombre}_nuevo"]
                            df_merged.drop(columns=[f"{serie_nombre}_nuevo"], inplace=True, errors='ignore')

                        df_merged.drop(columns=[col for col in df_merged.columns if '_existente' in col], 
                                      inplace=True, errors='ignore')
                        df_merged = df_merged.sort_values('fecha').drop_duplicates('fecha', keep='first')
                        bd_hojas[pestaña] = df_merged
                        
                        # Registrar ubicación
                        ubicacion = nombre_archivo.replace('.xlsx', '')
                        ubicaciones[id_serie] = ubicacion
                        df_codigos.at[fila_idx, "Ubicación"] = ubicacion
                        
                        ids_no_encontrados.discard(id_serie)
                        series_exitosas += 1
                        escribir_log(id_serie, "OK", f"Encontrado en {nombre_archivo}. Registros: {len(df_api)}")
                        
                    except ValueError:
                        continue
                    except Exception as e:
                        error_msg = str(e)
                        print(f"Error procesando ID {id_serie}: {error_msg}")
                        escribir_log(id_serie, "ERROR", error_msg)
                        continue
                        
            except Exception as e:
                error_msg = f"Error al procesar {nombre_archivo}: {str(e)}"
                print(error_msg)
                escribir_log("SISTEMA", "ERROR_ARCHIVO", error_msg)
                continue

        # Marcar IDs no encontrados
        series_fallidas = len(ids_no_encontrados)
        if ids_no_encontrados:
            print(f"\nIDs no encontrados en ningún archivo: {len(ids_no_encontrados)}")
            for id_no_encontrado in ids_no_encontrados:
                escribir_log(id_no_encontrado, "NO_ENCONTRADO", "ID no encontrado en ningún archivo")
                fila_idx = df_codigos[df_codigos["ID"].astype(str) == id_no_encontrado].index[0]
                filas_con_error.append(fila_idx + 2)  # +2 porque Excel empieza en 1 y la primera fila es encabezado

        # Guardar resultados
        try:
            # Guardar BD
            with pd.ExcelWriter("BD.xlsx", engine='openpyxl') as writer:
                for hoja, datos in bd_hojas.items():
                    # Asegurar que siempre vaya a "Otros" si es NaN o "nan"
                    hoja_str = str(hoja).strip() if pd.notna(hoja) else "Otros"
                    hoja_str = hoja_str[:31] if hoja_str else "Otros"
                    if hoja_str.lower() == "nan":
                        hoja_str = "Otros"
                    
                    datos.to_excel(writer, sheet_name=hoja_str, index=False)
            
            # Actualizar columna Ubicación en el archivo original
            ubicacion_col = None
            for idx, cell in enumerate(ws[1], 1):
                if cell.value == "Ubicación":
                    ubicacion_col = idx
                    break
            
            if ubicacion_col is None:
                ubicacion_col = ws.max_column + 1
                ws.cell(row=1, column=ubicacion_col, value="Ubicación")
            
            for idx, fila in df_codigos.iterrows():
                id_serie = str(fila["ID"])
                if id_serie in ubicaciones:
                    ws.cell(row=idx+2, column=ubicacion_col, value=ubicaciones[id_serie])
                
                if idx+2 in filas_con_error:
                    for col in range(1, ws.max_column):
                        ws.cell(row=idx+2, column=col).font = red_font
            
            wb.save(codigos_file)
            
            if filas_con_error:
                print(f"\nSe marcaron en rojo {len(filas_con_error)} IDs no encontrados")
            
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
        escribir_log("SISTEMA", "ERROR_CRITICO", str(e))

if __name__ == "__main__":
    procesar_datos()