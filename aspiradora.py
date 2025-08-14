import pandas as pd
import requests
import io

def obtener_datos_api(series_id):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # Nueva URL con límite aumentado a 1000 y paginación
    url = f"https://apis.datos.gob.ar/series/api/series?ids={series_id}&format=csv&limit=1000"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    df = pd.read_csv(io.StringIO(response.text))
    
    # Si hay más de 1000 registros, usar paginación con 'start'
    if len(df) >= 1000:
        all_data = [df]
        start = 1000
        while True:
            url_paginated = f"{url}&start={start}"
            response = requests.get(url_paginated, headers=headers)
            if response.status_code != 200:
                break
            df_page = pd.read_csv(io.StringIO(response.text))
            if df_page.empty:
                break
            all_data.append(df_page)
            start += 1000
        df = pd.concat(all_data, ignore_index=True)
    
    # Detectar columna de valores y manejar fechas completas
    value_column = [col for col in df.columns if col != 'indice_tiempo'][0]
    categorias = df['indice_tiempo']  # Sin truncar para mantener formato completo
    valores = df[value_column].tolist()
    
    return categorias, valores