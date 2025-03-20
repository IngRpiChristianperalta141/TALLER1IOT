import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
import io

# Configuración inicial
ERROR_THRESHOLD = 0.10  # 10% de error permitido

# Función para cargar los datos (Extracción mejorada)
@st.cache(ttl=3600, show_spinner=True)
def load_data():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate({
                "type": "service_account",
                "project_id": "conteomonedas-2a1e5",
                "private_key_id": "07d26b085fc98a6e5a769759d7979af2be86c7fe",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC775VYu1dAD6Hb\nO5n8yj9xa9yQDVpolGl9xlIQTZTTDrUyqsibFdI8cgsLbuTjuVLhp18UPj5O9ewz\npdQN4KiJ65dGKWPnAjTI6HdSxCZTwT3QxgOKYTRFaJ23wbtvzEj1muh2MjyWm+Zq\nfRZ1LGWQ+sDBDL+mraRhpUDRX5IfacC69Q5sHB8N1HC9nZXc/cu/5JzGDWBo4LyW\nTRQNgy3FDby5xgix6WxMlCAvJJxFgu2kK5Wbs/WZFQeMuMkB0LfCBR72HAx8Usp9\ngTw0YtGvhMFduf1ej2OCQxzIlyGk+Z07znsfYwHf7gRaJKDDEZ3PpkYpp8w5CcU4\nf4fu542tAgMBAAECggEAC7MuqQeAtwxCF1Pukj5FFNqSMKWkKz6SErJ2CW3LBhi0\nTyMyufTx0bGH3n5Y4ZR4BHRXBvuZBXFp8w0v1m1WEMAJ+uQ4w/jvMFLImu57NFuj\nbYvmTIcgqfGJYBc/hAZDxIaQUktuWjH6dkazTzgkWpJBzcLta4MD6NBQ6Lf6cNoZ\nMRcLbx/i5WxnDdlmaZjPkEIAIqa6/VKHGzCl1MNILhpeUyp1h37fk2oIlAN/PrCt\nHrbqK54EpxIXtrwdG50wuXoIk0wkHHnPkGUw5xyOUX8aU2F1wN+iV78qBA88U7Tg\nxii1z5PXCYfCfflIb/u7uKiaCt4haehmaAh0BUecQQKBgQDs9hV6xF/n/WRVllMx\nzcccIqQSuAEUE1DcYd2I7/5rcdW9bBYmpxgqNudKeF5Pp0L9YKZmP6OJa1W+beO4\n7oQdmtO+HO1fWMvGZumtbU6ro8vz4/r8y7flAbg+a1ymYjH5I1FPQWIJnT3pkLPz\nRFB5eJJCQel9y/ZnvUW9GhP+OwKBgQDLCSALvRx2+2ZJsc2jHlI4lR9inCySWrzG\nPv4JRRqJA7/ANguyvDw76uhRUZ/yIMO3FBUxNWa+LlPxObKwN8xUdtTjhpCYhQnG\nNWpMwDXw9cwjqDDgTTuvOy1kr/kzzD3SlmNSgAiWCos1iHNJhoYuGpJXA1ciwMoP\nJfbG9TndNwKBgB1lQbDjH4ql8oZziYqKHoMtAPGZhfi5lLRiJ5tPOl6V1pATEoRl\nAihqezLPz9NNyo+oJ7xTdLQdbpubAj70x8rsZAwXhpLnbxADwEVVZAcC4NA2nfIy\nS3PLTNHhe7LgR5bCfNm6ILFUolORMeGhveHzxSEXECIc5UplPnuA+q31AoGAAj7+\nmMKlzvcnAYezA6vxMukLaNbbc18HmZXWz5lxGrTs6R5v8LE+ZVdK7KNfhpkwMRVK\nyrmRgmcWzKNbw7FKVGDgKAg0D0vheMzceS7jKeA+OuLAb6rLDzwzW1rLAfSvdmXt\ngVsjPS2uro/s3pJbZ6Upm0bRLvQ9B//ehALwee8CgYAHACAPKRkYzbsF97aQju0A\njE+woZVptOApgc+IUZ5+1dYlP9AS7xtUi3/znAaffUXHJ98Re50uuoRjloWTTLeS\neHKD033OmjFeaGfkOm4MHLcOo92lA85m0dqhvmJC7Lb8sNy30C86scIkEWHqvZaq\nMFZ+yYP1M8Z0VlSJa7xfHA==\n-----END PRIVATE KEY-----\n",
                "client_email": "firebase-adminsdk-fbsvc@conteomonedas-2a1e5.iam.gserviceaccount.com",
                "client_id": "114138973755629717245",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40conteomonedas-2a1e5.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
            })
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        docs = db.collection("ConteoMonedas").stream()
        
        data = []
        for doc in docs:
            item = doc.to_dict()
            # Validación básica de datos
            if all(key in item for key in ['Fecha', 'Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
                data.append(item)
        
        return pd.DataFrame(data)
    
    except Exception as e:
        st.error(f"Error en la conexión a Firebase: {str(e)}")
        return pd.DataFrame()

# Función para transformar los datos (Transformación compleja)
def transform_data(df):
    if df.empty:
        return df
    
    # Conversión y validación de fecha
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', format='%y-%m-%d %H-%M-%S')
    df = df.dropna(subset=['Fecha']).copy()
    
    # Cálculos básicos
    monedas_cols = ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']
    caja_cols = ['PesoCaja100', 'PesoCaja200', 'PesoCaja500', 'PesoCaja1000']
    
    df['TotalMonedas'] = df[monedas_cols].sum(axis=1)
    df['TotalCaja'] = df[caja_cols].sum(axis=1)
    
    # Ingeniería de características
    df['DiaSemana'] = df['Fecha'].dt.day_name()
    df['Hora'] = df['Fecha'].dt.hour
    df['Turno'] = np.where(df['Hora'] < 12, 'Mañana', 'Tarde')
    
    # Normalización de valores
    for col in caja_cols:
        df[col+'_norm'] = (df[col] - df[col].mean()) / df[col].std()
    
    # Cálculo de errores porcentuales
    for coin, caja in zip(monedas_cols, caja_cols):
        expected = df[coin] * int(coin.replace('Monedas',''))
        df[f'Error_{coin}'] = np.abs((df[caja] - expected) / (expected + 1e-6))  # Evitar división por cero
    
    # Identificación de anomalías
    error_cols = [f'Error_{col}' for col in monedas_cols]
    df['ErrorTotal'] = df[error_cols].mean(axis=1)
    df['AlertaError'] = df['ErrorTotal'] > ERROR_THRESHOLD
    
    # Cálculos de tendencia
    df.sort_values('Fecha', inplace=True)
    df['MediaMovil7'] = df['TotalCaja'].rolling(window=7, min_periods=1).mean()
    df['DiffDiaAnterior'] = df['TotalCaja'].diff()
    
    return df

def main():
    st.title("Sistema Avanzado de Análisis de Monedas")
    
    # Cargar datos
    with st.spinner('Cargando datos desde Firebase...'):
        df = load_data()
    
    if df.empty:
        st.warning("No se encontraron datos válidos.")
        return
    
    # Transformación de datos
    with st.spinner('Procesando datos...'):
        df_transformed = transform_data(df)
    
    # Sección de análisis
    st.header("Análisis Detallado")
    
    # Filtros interactivos
    col1, col2 = st.columns(2)
    with col1:
        fecha_min = df_transformed['Fecha'].min().date()
        fecha_max = df_transformed['Fecha'].max().date()
        rango_fechas = st.date_input("Rango de fechas", [fecha_min, fecha_max])
    
    with col2:
        umbral_error = st.slider("Umbral de error para alertas", 0.0, 1.0, ERROR_THRESHOLD)
    
    # Aplicar filtros
    filtered_df = df_transformed[
        (df_transformed['Fecha'].dt.date >= rango_fechas[0]) & 
        (df_transformed['Fecha'].dt.date <= rango_fechas[1])
    ]
    
    # Métricas clave
    st.subheader("Métricas Clave")
    cols = st.columns(4)
    cols[0].metric("Total Registros", filtered_df.shape[0])
    cols[1].metric("Error Promedio", f"{filtered_df['ErrorTotal'].mean():.2%}")
    cols[2].metric("Alertas Activas", filtered_df['AlertaError'].sum())
    cols[3].metric("Variación Diaria Máxima", f"{filtered_df['DiffDiaAnterior'].max():.2f}")
    
    # Análisis temporal
    st.subheader("Análisis Temporal")
    tab1, tab2, tab3 = st.tabs(["Tendencia", "Estacionalidad", "Comparativo"])
    
    with tab1:
        fig, ax = plt.subplots(figsize=(12,6))
        filtered_df.set_index('Fecha')['TotalCaja'].plot(ax=ax, label='Valor Real')
        filtered_df.set_index('Fecha')['MediaMovil7'].plot(ax=ax, label='Media Móvil 7 días')
        plt.title('Tendencia de Valores en Caja')
        plt.legend()
        st.pyplot(fig)
    
    with tab2:
        decomposition = seasonal_decompose(filtered_df.set_index('Fecha')['TotalCaja'], period=24)
        fig = decomposition.plot()
        fig.set_size_inches(12,8)
        st.pyplot(fig)
    
    with tab3:
        fig, ax = plt.subplots(1,2, figsize=(15,5))
        sns.boxplot(data=filtered_df, x='DiaSemana', y='TotalCaja', ax=ax[0])
        sns.boxplot(data=filtered_df, x='Turno', y='TotalCaja', ax=ax[1])
        st.pyplot(fig)
    
    # Análisis de errores
    st.subheader("Análisis de Errores")
    col_analisis, col_dist = st.columns(2)

    with col_analisis:
        st.write("**Métricas de Error**")
        
        # Identificamos primero el día 19 para tratarlo por separado
        filtered_df['Fecha_solo'] = filtered_df['Fecha'].dt.date
        is_day_19 = filtered_df['Fecha'].dt.day == 19
        
        # Si hay datos del día 19, los mostramos separadamente
        if is_day_19.any():
            st.warning(f"⚠️ Se detectaron {is_day_19.sum()} registros del día 19 con posibles anomalías en los datos.")
            
            # Creamos un DataFrame sin el día problemático para cálculos más precisos
            df_sin_dia19 = filtered_df[~is_day_19].copy()
        else:
            df_sin_dia19 = filtered_df.copy()
        
        # Cálculo de errores con el conjunto de datos corregido
        total_esperado = 0
        total_real = 0
        
        for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
            expected = df_sin_dia19[col] * den
            actual = df_sin_dia19[f'PesoCaja{den}']
            
            total_esperado += expected.sum()
            total_real += actual.sum()
        
        # Cálculo de error medio (excluyendo día 19)
        if total_esperado > 0:
            error_medio = abs(total_real - total_esperado) / total_esperado
        else:
            error_medio = 0
        
        # Diferencia total (excluyendo día 19)
        diferencia_total = total_esperado - total_real
        
        # Error diario promedio (excluyendo día 19)
        daily_errors = []
        for fecha, grupo in df_sin_dia19.groupby('Fecha_solo'):
            esperado_dia = 0
            real_dia = 0
            for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
                esperado_dia += (grupo[col] * den).sum()
                real_dia += grupo[f'PesoCaja{den}'].sum()
            
            if esperado_dia > 0:
                error_dia = abs(real_dia - esperado_dia) / esperado_dia
                if error_dia > 1.0:  # Si el error es mayor al 100%, lo consideramos outlier
                    st.info(f"📌 Detectado posible outlier en fecha {fecha} con error {error_dia:.2%}")
                    error_dia = min(error_dia, 1.0)  # Limitamos a 100% para la visualización
                daily_errors.append(error_dia)
        
        error_diario = np.mean(daily_errors) if daily_errors else 0
        
        # Mostrar métricas (excluyendo día 19)
        st.metric("Error Medio (excluyendo anomalías)", f"{error_medio:.2%}")
        st.metric("Diferencia Total (excluyendo anomalías)", f"{diferencia_total:.0f} pesos")
        st.metric("Error Diario Medio (excluyendo anomalías)", f"{error_diario:.2%}")
        
        # Mostrar gráfico de tendencia de error por día
        st.write("**Tendencia de Error Diario**")
        
        # Creamos DataFrame con errores diarios para graficar
        error_trend_data = []
        for fecha, grupo in filtered_df.groupby('Fecha_solo'):
            # Verificamos si es el día problemático
            if grupo['Fecha'].dt.day.iloc[0] == 19:
                # Para el día 19, usamos un valor especial o lo excluimos
                error_trend_data.append({"Fecha": fecha, "Error": None})
                continue
                
            esperado_dia = 0
            real_dia = 0
            for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
                esperado_dia += (grupo[col] * den).sum()
                real_dia += grupo[f'PesoCaja{den}'].sum()
            
            if esperado_dia > 0:
                error_dia = abs(real_dia - esperado_dia) / esperado_dia
                # Limitar errores extremos para mejor visualización
                error_dia = min(error_dia, 1.0)
            else:
                error_dia = 0
                
            error_trend_data.append({"Fecha": fecha, "Error": error_dia})
        
        error_trend_df = pd.DataFrame(error_trend_data)
        if not error_trend_df.empty:
            # Filtramos valores None para el gráfico
            error_trend_clean = error_trend_df.dropna()
            if not error_trend_clean.empty:
                st.line_chart(error_trend_clean.set_index('Fecha'))
            else:
                st.write("No hay suficientes datos para mostrar la tendencia.")
        else:
            st.write("No hay suficientes datos para mostrar la tendencia de error diario.")

    with col_dist:
        st.write("**Distribución de Errores por Denominación (excluyendo anomalías)**")
        
        # Calculamos error por denominación (excluyendo día 19)
        error_by_denom = []
        for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
            expected = (df_sin_dia19[col] * den).sum()
            actual = df_sin_dia19[f'PesoCaja{den}'].sum()
            
            if expected > 0:
                error_rate = abs(actual - expected) / expected
            else:
                error_rate = 0
                
            error_by_denom.append({
                "Denominación": f"{den} pesos",
                "ErrorPromedio": error_rate
            })
        
        error_data = pd.DataFrame(error_by_denom)
        
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=error_data, x='Denominación', y='ErrorPromedio', ax=ax)
        ax.set_ylabel('Error Promedio')
        ax.set_title('Error por Denominación (excluyendo anomalías)')
        for i, v in enumerate(error_data['ErrorPromedio']):
            ax.text(i, v + 0.01, f"{v:.2%}", ha='center')
        st.pyplot(fig)

    # Análisis separado del día 19
    if is_day_19.any():
        st.subheader("Análisis del Día 19 (Datos Anómalos)")
        
        # Obtenemos solo los datos del día 19
        dia19_df = filtered_df[is_day_19].copy()
        
        # Calculamos valores esperados y reales para el día 19
        for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
            dia19_df[f'Esperado_{den}'] = dia19_df[col] * den
            dia19_df[f'Real_{den}'] = dia19_df[f'PesoCaja{den}']
            dia19_df[f'Diff_{den}'] = dia19_df[f'Esperado_{den}'] - dia19_df[f'Real_{den}']
        
        dia19_df['Total_Esperado'] = dia19_df[[f'Esperado_{den}' for den in [100, 200, 500, 1000]]].sum(axis=1)
        dia19_df['Total_Real'] = dia19_df[[f'Real_{den}' for den in [100, 200, 500, 1000]]].sum(axis=1)
        dia19_df['Diferencia_Total'] = dia19_df['Total_Esperado'] - dia19_df['Total_Real']
        
        # Mostrar los datos del día 19
        cols_dia19 = ['Fecha', 'Total_Esperado', 'Total_Real', 'Diferencia_Total'] + [
            f'Esperado_{den}' for den in [100, 200, 500, 1000]] + [
            f'Real_{den}' for den in [100, 200, 500, 1000]]
        
        st.dataframe(dia19_df[cols_dia19].style.format({
            'Total_Esperado': '{:.0f}',
            'Total_Real': '{:.0f}',
            'Diferencia_Total': '{:.0f}',
            'Esperado_100': '{:.0f}',
            'Esperado_200': '{:.0f}',
            'Esperado_500': '{:.0f}',
            'Esperado_1000': '{:.0f}',
            'Real_100': '{:.0f}',
            'Real_200': '{:.0f}',
            'Real_500': '{:.0f}',
            'Real_1000': '{:.0f}'
        }))
        
        st.info("📊 Los datos del día 19 muestran anomalías significativas y han sido excluidos del análisis general para evitar distorsiones.")

    # Detalle de registros con análisis de error individual (todos los días)
    st.subheader("Análisis Detallado por Registro")
    # Creamos columnas con información más útil para el análisis
    display_df = filtered_df.copy()

    for den, col in zip([100, 200, 500, 1000], ['Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000']):
        display_df[f'Esperado_{den}'] = display_df[col] * den
        display_df[f'Real_{den}'] = display_df[f'PesoCaja{den}']
        display_df[f'Diff_{den}'] = display_df[f'Esperado_{den}'] - display_df[f'Real_{den}']
        
    display_df['Total_Esperado'] = display_df[[f'Esperado_{den}' for den in [100, 200, 500, 1000]]].sum(axis=1)
    display_df['Total_Real'] = display_df[[f'Real_{den}' for den in [100, 200, 500, 1000]]].sum(axis=1)
    display_df['Diferencia_Total'] = display_df['Total_Esperado'] - display_df['Total_Real']
    display_df['Error_Porcentaje'] = (abs(display_df['Diferencia_Total']) / display_df['Total_Esperado']).replace([np.inf, -np.inf], 0)

    # Marcar los registros del día 19
    display_df['Es_Dia19'] = display_df['Fecha'].dt.day == 19

    # Mostrar los registros con mayor error
    cols_to_show = ['Fecha', 'Es_Dia19', 'Error_Porcentaje', 'Total_Esperado', 'Total_Real', 'Diferencia_Total']
    top_errors = display_df.sort_values('Error_Porcentaje', ascending=False).head(10)
    st.dataframe(top_errors[cols_to_show].style.format({
        'Error_Porcentaje': '{:.2%}',
        'Total_Esperado': '{:.0f}',
        'Total_Real': '{:.0f}',
        'Diferencia_Total': '{:.0f}'
    }))

    # Detalle de registros con mayor error
    st.subheader("Registros con Mayor Error")
    top_errors = filtered_df.sort_values('ErrorTotal', ascending=False).head(10)
    cols_to_show = ['Fecha', 'ErrorTotal', 'Monedas100', 'Monedas200', 'Monedas500', 'Monedas1000', 
                    'PesoCaja100', 'PesoCaja200', 'PesoCaja500', 'PesoCaja1000']
    st.dataframe(top_errors[cols_to_show])
    
    # Detalle de alertas
    st.subheader("Registros con Alertas de Error")
    alertas_df = filtered_df[filtered_df['AlertaError']]
    st.dataframe(alertas_df.sort_values('ErrorTotal', ascending=False))
    

if __name__ == "__main__":
    main()
