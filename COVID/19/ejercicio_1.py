# ejercicio_covid.py
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import io


PAIS_EJEMPLO = "Italy"  # Cambiar por el país deseado

# ---------------------------
# a) y b) Descargar y cargar datos
# ---------------------------
def descargar_datos():
    start_date = datetime(2020, 1, 22)
    end_date = datetime.now()
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv"

    print("Descargando datos de COVID-19...")
    print(f"Fechas a descargar: {len(date_range)}")

    datos_combinados = []

    for fecha in date_range:
        formatted_date = fecha.strftime("%m-%d-%Y")
        url = base_url.format(formatted_date)
        respuesta = requests.get(url)
        
        if respuesta.status_code == 200:
            df = pd.read_csv(io.StringIO(respuesta.text))
            
            df.rename(columns={
                'Country/Region': 'Country_Region',
                'Province/State': 'Province_State',
                'Last Update': 'Last_Update'
            }, inplace=True, errors='ignore')
            
            if 'Country_Region' not in df.columns or 'Confirmed' not in df.columns or 'Deaths' not in df.columns:
                continue
            
            df['Confirmed'] = pd.to_numeric(df['Confirmed'], errors='coerce').fillna(0)
            df['Deaths'] = pd.to_numeric(df['Deaths'], errors='coerce').fillna(0)
            df_agrupado = df.groupby('Country_Region', as_index=False).agg({'Confirmed': 'sum', 'Deaths': 'sum'})
            df_agrupado['Date'] = fecha.strftime("%Y-%m-%d")
            
            datos_combinados.append(df_agrupado)

    df_completo = pd.concat(datos_combinados, ignore_index=True)
    
    return df_completo

# ---------------------------
# c) Promedio diario de casos
# ---------------------------
def calcular_promedio_diario(df, pais):
    pais_df = df[df['Country_Region'] == pais].sort_values('Date')
    
    if pais_df.empty:
        raise ValueError(f"No hay datos para {pais}")
    
    pais_df['Nuevos_Casos'] = pais_df['Confirmed'].diff()
    pais_df = pais_df.dropna(subset=['Nuevos_Casos'])
    
    return pais_df['Nuevos_Casos'].mean()

# ---------------------------
# d) Top 10 mortalidad
# ---------------------------
def top_mortalidad(df):
    ultima_fecha = df['Date'].max()
    df_reciente = df[df['Date'] == ultima_fecha]
    
    # Filtrar países con casos confirmados
    df_reciente = df_reciente[df_reciente['Confirmed'] > 0]
    
    # Calcular tasa de mortalidad
    df_reciente['Tasa_Mortalidad'] = df_reciente['Deaths'] / df_reciente['Confirmed']
    
    return df_reciente.sort_values('Tasa_Mortalidad', ascending=False).head(10)[['Country_Region', 'Tasa_Mortalidad']]


if __name__ == "__main__":
    df_covid = descargar_datos()
    
    # c) Calcular promedio para el país especificado
    try:
        promedio = calcular_promedio_diario(df_covid, PAIS_EJEMPLO)
        print(f"\nPromedio diario de nuevos casos en {PAIS_EJEMPLO}: {promedio:.2f}")
    except ValueError as e:
        print(f"\nError: {str(e)}")
    
    # d) Obtener top 10 mortalidad
    print("\nTop 10 países con mayor tasa de mortalidad:")
    print(top_mortalidad(df_covid).to_string(index=False))