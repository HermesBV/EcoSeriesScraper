import pandas as pd
import main  # Importamos tu script principal para usar sus herramientas
import os
from tqdm import tqdm

def debug_masivo():
    print("--- INICIANDO DEBUGGER AUTOMÁTICO MASIVO ---")
    
    # 1. Cargar Codigos.xlsx
    print("Leyendo Codigos.xlsx...")
    try:
        df_codigos = pd.read_excel("Codigos.xlsx", dtype=str)
    except Exception as e:
        print(f"Error fatal leyendo Codigos.xlsx: {e}")
        return

    # 2. Cargar todos los Excels en memoria una sola vez
    print("Cargando archivos Excel en memoria (esto puede tardar un poco)...")
    todos_los_excels = {}
    todos_los_indices = {}
    
    # Listamos los archivos reales en la carpeta
    archivos_en_carpeta = os.listdir('Excels_IED')
    
    for archivo in tqdm(archivos_en_carpeta):
        if archivo.endswith(".xlsx") or archivo.endswith(".xls"):
            ruta = os.path.join('Excels_IED', archivo)
            try:
                # Usamos la función de carga de main.py
                data = main.cargar_excel_completo(ruta)
                todos_los_excels[archivo] = data
                # Creamos índice
                todos_los_indices[archivo] = main.crear_indice_excel(data)
            except Exception as e:
                print(f"Advertencia: No se pudo cargar {archivo}: {e}")

    # 3. Iterar sobre cada serie y diagnosticar
    reporte = []
    
    print("\nAnalizando series...")
    for idx, row in tqdm(df_codigos.iterrows(), total=len(df_codigos)):
        id_serie = str(row.get('ID', '')).strip()
        origen = str(row.get('Excel Origen', '')).strip()
        
        resultado = {
            "Fila Excel": idx + 2,
            "ID": id_serie,
            "Origen en Codigos": origen,
            "Archivo Mapeado": "",
            "Estado": "",
            "Detalle Error": "",
            "Sugerencia": ""
        }

        if not id_serie or id_serie == 'nan':
            resultado["Estado"] = "OMITIDO"
            resultado["Detalle Error"] = "ID vacío"
            continue

        # A. Chequeo de Mapeo
        nombre_archivo = main.MAPEO_ORIGEN_ARCHIVO.get(origen)
        resultado["Archivo Mapeado"] = nombre_archivo if nombre_archivo else "NO ENCONTRADO"
        
        if not nombre_archivo:
            resultado["Estado"] = "ERROR MAPEO"
            resultado["Detalle Error"] = f"El origen '{origen}' no está en el diccionario MAPEO_ORIGEN_ARCHIVO de main.py"
            resultado["Sugerencia"] = "Agregar este nombre al diccionario en main.py"
            reporte.append(resultado)
            continue

        # B. Chequeo de Existencia de Archivo
        if nombre_archivo not in todos_los_excels:
            resultado["Estado"] = "ARCHIVO FALTA"
            resultado["Detalle Error"] = f"El archivo '{nombre_archivo}' no existe en la carpeta o falló al cargar."
            resultado["Sugerencia"] = "Verificar descarga o nombre del archivo"
            reporte.append(resultado)
            continue

        # C. Búsqueda del ID
        indice = todos_los_indices[nombre_archivo]
        if id_serie not in indice:
            resultado["Estado"] = "ID NO ENCONTRADO"
            resultado["Detalle Error"] = "El texto exacto del ID no está en ninguna celda."
            
            # Búsqueda fuzzy (parecidos)
            sugerencia = "Verificar espacios o mayúsculas."
            for key in indice.keys():
                if id_serie.lower() in str(key).lower():
                    sugerencia = f"¿Quizás es '{key}'? (Encontrado en hoja: {indice[key][0]})"
                    break
            resultado["Sugerencia"] = sugerencia
            reporte.append(resultado)
            continue

        # D. Intento de Extracción
        ubicacion = indice[id_serie]
        excel_data = todos_los_excels[nombre_archivo]
        
        try:
            fechas, valores = main.extraer_serie_desde_indice(excel_data, ubicacion)
            if len(fechas) == 0:
                resultado["Estado"] = "DATOS VACÍOS"
                resultado["Detalle Error"] = "Se encontró el ID, pero no se pudieron leer fechas/valores debajo."
                resultado["Sugerencia"] = "Aumentar 'max_filas_a_buscar' o revisar formato fecha."
            else:
                resultado["Estado"] = "OK"
                resultado["Detalle Error"] = f"Se extrajeron {len(fechas)} registros correctamente."
        except Exception as e:
            resultado["Estado"] = "ERROR EXTRACCIÓN"
            resultado["Detalle Error"] = str(e)
            resultado["Sugerencia"] = "Revisar estructura de la tabla en el Excel."

        reporte.append(resultado)

    # 4. Guardar Reporte
    df_reporte = pd.DataFrame(reporte)
    df_reporte.to_csv("reporte_errores.csv", index=False, encoding='utf-8-sig', sep=';')
    print("\n------------------------------------------------")
    print(f"Diagnóstico completado. Se generó el archivo: reporte_errores.csv")
    print("Ábrelo en Excel para filtrar los errores y ver las sugerencias.")
    
    # Resumen rápido en consola
    print("\nRESUMEN DE ESTADOS:")
    print(df_reporte['Estado'].value_counts())

if __name__ == "__main__":
    debug_masivo()