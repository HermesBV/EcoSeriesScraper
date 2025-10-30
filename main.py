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
import io # Importado del primer script, aunque no se usa aquí, es buena práctica tenerlo si se usara requests.get().text

# Configuración inicial
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Diccionario de URLs y nombres de archivos (sin cambios)
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

# Mapeo de 'Excel Origen' (de Codigos.xlsx) a los nombres de archivo locales.
# --- ¡Este es el bloque modificado por el usuario, se respeta! ---
MAPEO_ORIGEN_ARCHIVO = {
    # Mapeos probables (basados en las URLs de EXCEL_URLS)
    "Empleo e Ingresos: Apendice3A": "empleo_ingresos.xlsx",
    "Sector Externo: Apendice5": "sector_externo.xlsx",
    "Economía Internacional: internacional_ied": "internacional.xlsx",
    "Dinero y Bancos: Apendice8": "dinero_bancos.xlsx",
    "Precios: Apendice4": "precios.xlsx",
    "Finanzas: Apendice-Financiero": "finanzas.xlsx",
    "Finanzas Públicas: Apendice6": "finanzas_publicas.xlsx",
    "Actividad: Actividad_IED": "actividad.xlsx",
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
    
    fechas_limpias = [str(f).strip() for f in fechas]
    
    for fmt in formatos_conocidos:
        try:
            # Intentar parsear todo el vector
            return pd.to_datetime(fechas_limpias, format=fmt, errors='raise')
        except (ValueError, TypeError):
            continue
    
    # Si no funciona con formatos conocidos, aplicamos el parser manual
    fechas_parseadas = []
    for fecha in fechas_limpias:
        fecha_parseada = parse_fecha_manual(fecha)
        fechas_parseadas.append(fecha_parseada if not pd.isna(fecha_parseada) else pd.NaT)
    
    return pd.DatetimeIndex(fechas_parseadas)

def parse_fecha_manual(s):
    """Parser manual para formatos de fecha no estándar"""
    try:
        s = str(s).strip()
        # Caso 1: Timestamp de Excel (número flotante o entero)
        if re.match(r'^\d+(\.\d+)?$', s):
             # 45000 es aprox 2023. Asumimos que es una fecha si es > 10000 (aprox 1927)
            if float(s) > 10000:
                try:
                    return pd.to_datetime(float(s), unit='D', origin='1899-12-30')
                except:
                    pass # Seguir a otros métodos

        # Caso 2: año de 4 dígitos
        if re.match(r'^\d{4}$', s):
            year = int(s)
            return pd.Timestamp(year=year, month=12, day=31)
        
        # --- OPTIMIZACIÓN ---
        # Caso 3: trimestre (I 24, II 24, I 2024, etc.)
        # Se cambia \d{2} por \d{2,4} para aceptar años de 2 o 4 dígitos
        elif re.match(r'^[IVXLCDM]+\s+\d{2,4}$', s, re.IGNORECASE):
            partes = s.upper().split()
            trimestre = partes[0]
            año_corto = int(partes[1])
            año = 2000 + año_corto if año_corto < 100 else año_corto # Manejar '24' (2024) y '2024'
            if año_corto > 50 and año_corto < 100: # Asumir siglo XX para años > 50 (ej. 99 -> 1999)
                 año = 1900 + año_corto

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
        
        # --- ¡MODIFICACIÓN! ---
        # Caso 3b: trimestre formato "IV.02", "I.24" (visto en imagen)
        elif re.match(r'^[IVXLCDM]+\.\d{2}$', s, re.IGNORECASE):
            partes = s.upper().split('.')
            trimestre = partes[0]
            año_corto = int(partes[1])
            año = 2000 + año_corto
            if año_corto > 50: # Asumir siglo XX para años > 50 (ej. 99 -> 1999)
                 año = 1900 + año_corto
            
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
        
        # Caso 4: meses (Ene-24, Ene 24, etc.)
        else:
            meses_esp = {
                'ENE': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'ABR': 'Apr', 
                'MAY': 'May', 'JUN': 'Jun', 'JUL': 'Jul', 'AGO': 'Aug', 
                'SEP': 'Sep', 'OCT': 'Oct', 'NOV': 'Nov', 'DIC': 'Dec'
            }
            s_upper = s.upper()
            for mes_esp, mes_eng in meses_esp.items():
                if s_upper.startswith(mes_esp):
                    s_eng = s_upper.replace(mes_esp, mes_eng)
                    # Intentar formatos comunes de mes-año
                    for fmt in ['%b %y', '%b-%y', '%b%y', '%b %Y', '%b-%Y']:
                        try:
                            fecha = pd.to_datetime(s_eng, format=fmt, errors='raise')
                            return fecha + pd.offsets.MonthEnd(0)
                        except:
                            continue
            # Último intento con el parser genérico de pandas
            return pd.to_datetime(s, errors='coerce')
    except:
        return pd.NaT

# --- ¡NUEVA FUNCIÓN DE OPTIMIZACIÓN! ---
def crear_indice_excel(excel_data):
    """
    Recorre un archivo Excel (cargado como diccionario de DataFrames) UNA VEZ
    y crea un índice (diccionario) de todos los IDs encontrados.
    El índice mapea: id_limpio -> (sheet_name, fila, columna)
    """
    indice_local = {}
    for sheet_name, df in excel_data.items():
        # Usar .values para iteración rápida sobre numpy array
        df_values = df.values
        for r in range(df_values.shape[0]):
            for c in range(df_values.shape[1]):
                valor = df_values[r, c]
                
                # Heurística para detectar un posible ID
                if valor and isinstance(valor, str):
                    valor_limpio = valor.strip()
                    # Un ID debe tener > 4 caracteres, no ser "nan", y contener un número O un punto.
                    # Esto evita strings simples como "Total", "Fecha", "Enero", etc.
                    if len(valor_limpio) > 4 and valor_limpio.lower() != 'nan' and (re.search(r'\d', valor_limpio) or '.' in valor_limpio):
                        if valor_limpio not in indice_local: # Guardar solo la primera aparición
                            indice_local[valor_limpio] = (sheet_name, r, c)
    return indice_local

# --- ¡FUNCIÓN MODIFICADA! ---
def extraer_serie_desde_indice(id_serie, excel_data, ubicacion):
    """
    Extrae datos de una serie usando la ubicación exacta (hoja, fila, col)
    obtenida del índice.
    AHORA MANEJA FILAS VACÍAS ENTRE EL ID Y LOS DATOS.
    """
    sheet_name, fila_id, col_id = ubicacion
    df = excel_data[sheet_name] # Acceso directo a la hoja correcta
    
    valores = []
    fechas = []
    
    # --- ¡MODIFICACIÓN! ---
    # Buscar la primera fila de datos válida y la columna de fecha
    
    col_fecha = 0
    fila_inicio_datos = -1
    max_filas_a_buscar = 100 # Límite de búsqueda de 100 filas vacías (antes 20)
    
    for r_temp in range(fila_id + 1, min(fila_id + 1 + max_filas_a_buscar, df.shape[0])):
        valor_val_temp = df.iat[r_temp, col_id]
        
        # Si la columna de valor no está vacía
        if not pd.isna(valor_val_temp) and str(valor_val_temp).strip() != "":
            
            # Ahora, encontrar la columna de fecha en esta fila
            fecha_encontrada = False
            for c_fecha in range(min(3, df.shape[1])): # Revisar primeras 3 columnas
                if not pd.isna(df.iat[r_temp, c_fecha]):
                    col_fecha = c_fecha
                    fecha_encontrada = True
                    break
            
            # Si encontramos un valor Y una fecha, esta es nuestra fila de inicio
            if fecha_encontrada:
                fila_inicio_datos = r_temp
                break
                
    if fila_inicio_datos == -1:
        raise ValueError(f"ID {id_serie} encontrado en ({fila_id}, {col_id}), pero no se encontraron datos válidos (fecha y valor) en las {max_filas_a_buscar} filas siguientes.")
    # --- FIN MODIFICACIÓN ---

    # Iterar hacia abajo desde la primera fila de datos encontrada
    r = fila_inicio_datos
    while r < df.shape[0]:
        valor_val = df.iat[r, col_id] # Usar col_id
        # Parar si encontramos un NaN o nulo en la columna de valores
        if pd.isna(valor_val) or str(valor_val).strip() == "":
            break

        fecha_val = df.iat[r, col_fecha]
        # Parar si la fecha es nula
        if pd.isna(fecha_val) or str(fecha_val).strip() == "":
            break
        
        # --- ¡MODIFICACIÓN! Manejo robusto de porcentajes y texto ---
        
        valor_val_limpio = valor_val
        if isinstance(valor_val, str):
            valor_val_limpio = valor_val.strip()

        try:
            # 1. Intento de conversión directa (para números 5.5, 10, etc.)
            valor_num = float(valor_val_limpio)
            valores.append(valor_num)
        except (ValueError, TypeError):
            # 2. Si falla, ¿es un porcentaje?
            if isinstance(valor_val_limpio, str):
                if valor_val_limpio.endswith('%'):
                    try:
                        # Intentar convertir el número del porcentaje (ej "5.5%" -> 5.5)
                        valor_num_pct = float(valor_val_limpio.replace('%', '').strip())
                        valores.append(valor_num_pct)
                    except (ValueError, TypeError):
                        # Es un string con % pero no numérico (ej. "N/A %")
                        valores.append(np.nan)
                
                # 3. ¿Es un string vacío o un guión?
                elif valor_val_limpio == '-' or valor_val_limpio == '':
                    valores.append(np.nan)
                
                # 4. Es otro string (ej. "N/D", "Preliminar")
                else:
                    # Convertir cualquier otro texto a NaN para mantener la columna numérica
                    valores.append(np.nan)
            
            # 5. No es string y no es float (ej. un objeto de error de Excel?)
            else:
                valores.append(np.nan)
        
        # --- FIN MODIFICACIÓN ---
        
        fechas.append(fecha_val)
        r += 1
    
    if not fechas:
        # Este error no debería ocurrir si fila_inicio_datos se encontró
        raise ValueError(f"ID {id_serie} encontrado pero sin datos asociados debajo.")

    fechas_dt = parse_fechas(fechas)
    
    # Crear un DataFrame temporal para limpiar NaNs
    df_temp = pd.DataFrame({'fecha': fechas_dt, 'valor': valores})
    df_temp = df_temp.dropna(subset=['fecha', 'valor']) # Eliminar filas donde fecha O valor sea NaN
    
    return df_temp['fecha'], df_temp['valor']


def cargar_excel_completo(ruta):
    """Carga un archivo Excel completo con todas sus hojas"""
    excel_data = {}
    try:
        # Usar openpyxl para data_only=True para obtener valores de fórmulas
        wb = load_workbook(ruta, data_only=True, read_only=True) # read_only para más velocidad
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            data = ws.values
            # Convertir a DataFrame
            cols = next(data, []) # Obtener encabezados si existen (aunque usamos header=None)
            df = pd.DataFrame(data, columns=cols)
            excel_data[sheet_name] = df
        
        # Fallback si openpyxl falla (ej. formato muy antiguo)
        if not excel_data:
             raise Exception("Fallback a pd.ExcelFile")
             
        return excel_data
        
    except Exception as e_openpyxl:
        # Fallback a pandas.ExcelFile
        print(f"Advertencia: Falló carga con openpyxl para {os.path.basename(ruta)} ({e_openpyxl}), reintentando con pd.ExcelFile. Los valores de fórmulas pueden no ser correctos.")
        try:
            excel_data_pd = {}
            with pd.ExcelFile(ruta) as xls:
                for sheet_name in xls.sheet_names:
                    excel_data_pd[sheet_name] = pd.read_excel(xls, sheet_name, header=None)
            return excel_data_pd
        except Exception as e_pandas:
             raise ValueError(f"Error al cargar archivo {os.path.basename(ruta)} con ambos motores: {str(e_pandas)}")


def procesar_datos():
    """Función principal que orquesta todo el proceso"""
    try:
        crear_carpeta_logs()
        escribir_log("SISTEMA", "INICIO", "Inicio del proceso de extracción")
        
        # Descargar todos los excels (siempre sobrescribiendo)
        archivos_descargados = descargar_excels()
        if not archivos_descargados:
            raise Exception("No se pudo descargar ningún archivo Excel")
        
        # --- LÓGICA MODIFICADA ---
        # Cargar archivo Excel con los códigos
        codigos_file = "Codigos.xlsx"
        try:
            wb = load_workbook(codigos_file)
            ws = wb.active
            red_font = Font(color="FF0000")
            
            df_codigos = pd.read_excel(
                codigos_file,
                # Leer las 4 nuevas columnas
                usecols=["Excel Origen", "ID", "Variable", "Pestaña Renombrada"],
                dtype={
                    "Excel Origen": str,
                    "ID": str,
                    "Variable": str,
                    "Pestaña Renombrada": str
                }
            )
            
            # Limpiar datos de Codigos.xlsx
            df_codigos["Pestaña Renombrada"] = df_codigos["Pestaña Renombrada"].fillna("Otros")
            df_codigos["Pestaña Renombrada"] = df_codigos["Pestaña Renombrada"].replace({"nan": "Otros", "": "Otros"})
            df_codigos["Excel Origen"] = df_codigos["Excel Origen"].str.strip()
            df_codigos["ID"] = df_codigos["ID"].str.strip()
            
            # --- MODIFICACIÓN: Lógica de "Ubicación" eliminada ---
            
        except Exception as e:
            error_msg = f"Error al leer Codigos.xlsx: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            return

        # Cargar o inicializar BD
        try:
            bd_hojas = pd.read_excel("BD.xlsx", sheet_name=None)
            
            # Migrar datos de hojas 'nan' a 'Otros'
            for hoja in list(bd_hojas.keys()): # Usamos list() para evitar RuntimeError
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
                # Convertir fecha a datetime
                bd_hojas[hoja]['fecha'] = pd.to_datetime(bd_hojas[hoja]['fecha'])
                
        except FileNotFoundError:
            bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}
        except Exception as e:
            error_msg = f"Error al leer BD.xlsx: {str(e)}"
            print(error_msg)
            escribir_log("SISTEMA", "ERROR", error_msg)
            bd_hojas = {"Otros": pd.DataFrame(columns=['fecha'])}

        # --- LÓGICA DE PROCESAMIENTO OPTIMIZADA ---
        
        # 1. Cargar TODOS los excels descargados en memoria primero
        todos_los_excels_cargados = {}
        print("\nCargando archivos Excel descargados en memoria...")
        for nombre_archivo, ruta_archivo in archivos_descargados.items():
            try:
                todos_los_excels_cargados[nombre_archivo] = cargar_excel_completo(ruta_archivo)
                print(f" · {nombre_archivo} cargado.")
            except Exception as e:
                error_msg = f"Error al cargar {nombre_archivo} en memoria: {str(e)}"
                print(error_msg)
                escribir_log("SISTEMA", "ERROR_CARGA_MEMORIA", error_msg)

        if not todos_los_excels_cargados:
             raise Exception("Falló la carga de todos los archivos Excel. Abortando.")

        # --- ¡NUEVO! PASO 2: Crear índices para cada archivo cargado ---
        indices_por_archivo = {}
        print("\nCreando índices de búsqueda para los archivos...")
        for nombre_archivo, excel_data in tqdm(todos_los_excels_cargados.items(), desc="Indexando archivos"):
            indices_por_archivo[nombre_archivo] = crear_indice_excel(excel_data)
        print("Índices creados.")


        total_series = len(df_codigos)
        series_exitosas = 0
        series_fallidas = 0
        filas_con_error = []

        print("\nProcesando series desde Codigos.xlsx...")
        # 3. Iterar sobre el DataFrame df_codigos
        for fila_idx, fila_codigo in tqdm(df_codigos.iterrows(), total=total_series, desc="Procesando series"):
            
            id_serie = str(fila_codigo["ID"])
            fila_excel_num = fila_idx + 2 # +2 porque Excel empieza en 1 y la fila 1 es encabezado
            
            try:
                # Obtener datos de la fila de Codigos.xlsx
                excel_origen_key = str(fila_codigo["Excel Origen"]).strip()
                variable_nombre = str(fila_codigo["Variable"]).strip() # Nuevo
                pestaña = str(fila_codigo["Pestaña Renombrada"]).strip() # Nuevo
                pestaña = "Otros" if not pestaña or pestaña.lower() == "nan" else pestaña

                # Validaciones
                if not id_serie or id_serie.lower() == 'nan':
                     raise ValueError(f"Fila {fila_excel_num}: ID está vacío")
                if not variable_nombre or variable_nombre.lower() == 'nan':
                    raise ValueError(f"La columna 'Variable' está vacía para el ID {id_serie} (Fila {fila_excel_num})")
                if not excel_origen_key or excel_origen_key.lower() == 'nan':
                    raise ValueError(f"'Excel Origen' está vacío para el ID {id_serie} (Fila {fila_excel_num})")

                # Encontrar el archivo local usando el mapeo
                if excel_origen_key not in MAPEO_ORIGEN_ARCHIVO:
                    raise ValueError(f"'{excel_origen_key}' (de 'Excel Origen') no se reconoce. Revise MAPEO_ORIGEN_ARCHIVO y Codigos.xlsx (Fila {fila_excel_num})")
                
                nombre_archivo_local = MAPEO_ORIGEN_ARCHIVO[excel_origen_key]
                
                # Verificar que el archivo se haya cargado e indexado
                if nombre_archivo_local not in todos_los_excels_cargados:
                    raise ValueError(f"El archivo '{nombre_archivo_local}' (mapeado desde '{excel_origen_key}') no pudo ser cargado o descargado.")
                if nombre_archivo_local not in indices_por_archivo:
                     raise ValueError(f"No se pudo crear un índice para '{nombre_archivo_local}'.")
                
                excel_data = todos_los_excels_cargados[nombre_archivo_local]
                indice_del_archivo = indices_por_archivo[nombre_archivo_local]

                # --- ¡OPTIMIZACIÓN! Búsqueda instantánea en el índice ---
                ubicacion = indice_del_archivo.get(id_serie)
                
                if not ubicacion:
                    raise ValueError(f"ID {id_serie} no encontrado en el índice de {nombre_archivo_local}")

                # Extraer la serie usando la ubicación exacta
                categorias_api, valores_api = extraer_serie_desde_indice(id_serie, excel_data, ubicacion)
                
                # Crear DataFrame con los datos (Usando 'variable_nombre')
                df_api = pd.DataFrame({
                    'fecha': categorias_api,
                    variable_nombre: valores_api
                })
                df_api['fecha'] = pd.to_datetime(df_api['fecha'])
                
                # Actualizar la BD
                if pestaña not in bd_hojas:
                    bd_hojas[pestaña] = pd.DataFrame(columns=['fecha'])
                    bd_hojas[pestaña]['fecha'] = pd.to_datetime(bd_hojas[pestaña]['fecha'])
                
                # Asegurar que la fecha de la hoja BD sea datetime
                bd_hojas[pestaña]['fecha'] = pd.to_datetime(bd_hojas[pestaña]['fecha'])

                # --- Lógica de Merge (Actualizada) ---
                
                # 1. Eliminar la columna 'variable_nombre' si ya existe en la hoja destino
                if variable_nombre in bd_hojas[pestaña].columns:
                     bd_hojas[pestaña] = bd_hojas[pestaña].drop(columns=[variable_nombre])
                     
                # 2. Hacer el merge (outer)
                df_merged = pd.merge(
                    bd_hojas[pestaña],
                    df_api,
                    on='fecha',
                    how='outer'
                )

                # Ordenar por fecha y asegurarse que fecha es la primera columna
                df_merged = df_merged.sort_values('fecha').reset_index(drop=True)
                
                # Reordenar columnas para que 'fecha' esté primero
                cols = ['fecha'] + [col for col in df_merged.columns if col != 'fecha']
                bd_hojas[pestaña] = df_merged[cols]

                # Registrar éxito
                series_exitosas += 1
                escribir_log(id_serie, "OK", f"Encontrado en {nombre_archivo_local} ({excel_origen_key}). Registros: {len(df_api)}")
                
                # --- MODIFICACIÓN: Lógica de "Ubicación" eliminada ---
                
            except Exception as e:
                # Registrar falla
                series_fallidas += 1
                error_msg = str(e)
                # No imprimir en tqdm, solo log
                escribir_log(id_serie, "ERROR", error_msg)
                filas_con_error.append(fila_excel_num)
                continue
        
        # --- FIN LÓGICA DE PROCESAMIENTO MODIFICADA ---

        if series_fallidas > 0:
             print(f"\nSe produjeron {series_fallidas} errores. Revise el log.")

        # Guardar resultados
        try:
            # Guardar BD
            with pd.ExcelWriter("BD.xlsx", engine='openpyxl') as writer:
                for hoja, datos in bd_hojas.items():
                    hoja_str = str(hoja).strip() if pd.notna(hoja) else "Otros"
                    hoja_str = hoja_str[:31] if hoja_str else "Otros"
                    if hoja_str.lower() == "nan":
                        hoja_str = "Otros"
                    
                    # --- ¡MODIFICACIÓN! No crear hojas vacías (como "Otros" si no tiene series) ---
                    if datos.shape[1] <= 1: # Si solo tiene la columna 'fecha'
                        continue
                    
                    # Eliminar filas donde la fecha es NaT
                    datos_limpios = datos.dropna(subset=['fecha']).copy() # .copy() para evitar warnings

                    # --- ¡MODIFICACIÓN! Formatear fecha según la frecuencia de la pestaña ---
                    frecuencia = ''
                    if hoja_str: # Asegurarse que hoja_str no esté vacía
                        frecuencia = hoja_str[-1].upper()
                    
                    if frecuencia in ['A', 'T', 'M']: # Anual, Trimestral, Mensual
                        datos_limpios['fecha'] = datos_limpios['fecha'].dt.strftime('%Y-%m')
                    else: # Default for D (Diaria), S (Semanal), or others
                        datos_limpios['fecha'] = datos_limpios['fecha'].dt.strftime('%Y-%m-%d')
                    
                    datos_limpios.to_excel(writer, sheet_name=hoja_str, index=False)
            
            # --- MODIFICACIÓN: Lógica de "Ubicación" eliminada ---
            # --- Nueva lógica para marcar/desmarcar errores en Codigos.xlsx
            print(f"\nActualizando marcado de errores en '{codigos_file}'...")
            normal_font = Font() # Fuente por defecto (sin color)
            
            # Iterar por todas las filas que lee df_codigos (fila_idx de 0 a len(df_codigos)-1)
            for fila_idx in range(len(df_codigos)):
                fila_excel = fila_idx + 2 # Fila de Excel
                
                # Determinar la fuente a aplicar
                current_font = red_font if fila_excel in filas_con_error else normal_font
                
                for col in range(1, ws.max_column + 1):
                    try:
                        ws.cell(row=fila_excel, column=col).font = current_font
                    except:
                        pass # Ignorar si la celda no se puede marcar

            wb.save(codigos_file)
            
            if filas_con_error:
                print(f"Se marcaron en rojo {len(filas_con_error)} filas con error.")
            else:
                print("Se limpió el formato de error (no se encontraron errores).")
            # --- Fin nueva lógica de marcado ---
            
            resumen = f"Proceso completado. Series: {total_series} | Exitosas: {series_exitosas} | Fallidas: {series_fallidas}"
            print(f"\n{resumen}")
            escribir_log("SISTEMA", "FIN", resumen)
        except Exception as e:
            error_msg = f"Error al guardar archivos: {str(e)}"
            print(f"\n{error_msg}")
            escribir_log("SISTEMA", "ERROR", error_msg)

    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario")
        escribir_log("SISTEMA", "INTERRUMPIDO", "Proceso detenido manually")
    except Exception as e:
        import traceback
        print(f"\nError crítico inesperado: {str(e)}")
        print(traceback.format_exc())
        escribir_log("SISTEMA", "ERROR_CRITICO", str(e) + " | " + traceback.format_exc())

if __name__ == "__main__":
    procesar_datos()
