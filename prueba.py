import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px

# Configuración de la página
st.set_page_config(
    page_title="Evaluación de Usuarios Coink", 
    layout="wide", 
    initial_sidebar_state="expanded", 
    page_icon="💰"
)

# CSS personalizado para modo oscuro
st.markdown("""
    <style>
    body {
        background-color: #121212;
        font-family: 'Segoe UI', sans-serif;
        color: #ffffff;
    }
    .stApp {
        background-color: #121212;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff;
    }
    /* Personalización de la barra lateral */
    .css-1aumxhk {
        background-color: #2E7D32;
        color: white;
    }
    /* Botones y otros elementos */
    .stButton button {
        background-color: #2E7D32;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 16px;
    }
    </style>
    """, unsafe_allow_html=True)

# Título e introducción
st.title("Evaluación del Desempeño de Usuarios de Coink")
st.markdown("Esta aplicación analiza los depósitos y operaciones de los usuarios, generando gráficos interactivos para explorar los datos de forma dinámica y visualmente atractiva.")

# Barra lateral para filtros
st.sidebar.header("Filtros")
fecha_min = st.sidebar.date_input("Fecha mínima de operación", value=pd.to_datetime("2022-01-01"))
fecha_max = st.sidebar.date_input("Fecha máxima de operación", value=pd.to_datetime("2022-12-31"))

# Ruta al archivo CSV
ruta_archivo = r"C:\Users\ingch\OneDrive\Escritorio\IOT\TALLER1\depositos_oinks.csv"

# Cargar el CSV
df = pl.read_csv(ruta_archivo)
st.subheader("Columnas disponibles en el dataset")
st.write(df.columns)

# Corregir y convertir operation_date (considerando el formato duplicado)
if "operation_date" in df.columns:
    df = df.with_columns([
        pl.col("operation_date").str.slice(0, 10).alias("operation_date_fixed")
    ]).with_columns([
        pl.col("operation_date_fixed").str.strptime(pl.Date, "%Y-%m-%d")
    ])
    df = df.drop("operation_date").rename({"operation_date_fixed": "operation_date"})

# Convertir user_createddate a Datetime
if "user_createddate" in df.columns:
    df = df.with_columns([
        pl.col("user_createddate").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f")
    ])
else:
    st.error("No se encontró la columna 'user_createddate'.")

# Filtrar por fecha (usando pandas para mayor flexibilidad)
df_pd = df.to_pandas()
df_pd["operation_date"] = pd.to_datetime(df_pd["operation_date"])
df_filtrado = df_pd[
    (df_pd["operation_date"] >= pd.to_datetime(fecha_min)) & 
    (df_pd["operation_date"] <= pd.to_datetime(fecha_max))
]
df = pl.from_pandas(df_filtrado)

# Agrupar datos por usuario y calcular métricas básicas
agg_expressions = [
    pl.col("operation_value").sum().alias("total_deposit"),
    pl.col("operation_value").count().alias("num_operations"),
    pl.col("operation_value").mean().alias("avg_deposit")
]

if "operation_date" in df.columns:
    agg_expressions.append(pl.col("operation_date").max().alias("last_operation"))
if "user_createddate" in df.columns:
    agg_expressions.append(pl.col("user_createddate").first().alias("user_created_date"))

agg_df = df.group_by("user_id").agg(agg_expressions)

# Calcular la antigüedad de la cuenta en días y el score
if "last_operation" in agg_df.columns and "user_created_date" in agg_df.columns:
    agg_df = agg_df.with_columns([
        ((pl.col("last_operation") - pl.col("user_created_date").cast(pl.Date)))
        .dt.total_days().alias("account_age_dias")
    ])
    agg_df = agg_df.with_columns([
        ((pl.col("total_deposit") * pl.col("num_operations")) / (pl.col("account_age_dias") + 1))
        .alias("score")
    ])
else:
    agg_df = agg_df.with_columns([
        (pl.col("total_deposit") * pl.col("num_operations")).alias("score")
    ])

df_final = agg_df.to_pandas()

# Métricas adicionales
df_final["deposit_efficiency"] = df_final["total_deposit"] / df_final["num_operations"]
df_final["deposit_per_day"] = df_final["total_deposit"] / (df_final["account_age_dias"] + 1)
df_final["operations_per_day"] = df_final["num_operations"] / (df_final["account_age_dias"] + 1)
df_final = df_final.sort_values("score", ascending=False)

st.subheader("Métricas por Usuario")
st.dataframe(df_final)

# Gráficos interactivos con Plotly (modo oscuro)

# Gráfico 1: Top 10 usuarios por score (barras interactivas)

top10 = df_final.head(10)
fig_top10 = px.bar(
    top10, 
    x="user_id", 
    y="score", 
    title="Top 10 Usuarios por Score", 
    labels={"user_id": "User ID", "score": "Score"},
    color="score", 
    color_continuous_scale="Blues"
)
fig_top10.update_layout(xaxis_tickangle=-45, template="plotly_dark")
st.plotly_chart(fig_top10, use_container_width=True)

# Gráfico 2: Histograma de la antigüedad de cuentas (días)
st.markdown("""
### Distribución de la Antigüedad de Cuentas

Esta gráfica muestra cómo se distribuyen los usuarios según la antigüedad de sus cuentas (en días).  
Los intervalos incluyen categorías como **100, 200, 300, 400 y 500 días**, representadas con barras que indican la cantidad de usuarios en cada rango.

#### 🔍 Observaciones:

- **Mayor concentración en cuentas jóvenes:**  
  La barra más alta corresponde a cuentas con **100 días de antigüedad**, lo que sugiere que la mayoría de los usuarios son relativamente nuevos.  

- **Cuentas antiguas escasas:**  
  La barra de **500 días** es la más baja, indicando que hay pocos usuarios con cuentas muy antiguas.  

- **Posible rotación de usuarios:**  
  Este patrón podría reflejar una **alta tasa de abandono** o **baja retención** a medida que las cuentas envejecen.  
""")
fig_hist = px.histogram(
    df_final, 
    x="account_age_dias", 
    nbins=20, 
    title="Distribución de la Antigüedad de Cuentas (días)",
    labels={"account_age_dias": "Antigüedad (días)"},
    color_discrete_sequence=["coral"]
)
fig_hist.update_layout(template="plotly_dark")
st.plotly_chart(fig_hist, use_container_width=True)

# Gráfico 3: Dispersión Score vs. Antigüedad de la cuenta
st.markdown("""
### Relación entre Score y Antigüedad de la Cuenta  

Este gráfico muestra un análisis de la relación entre el **score de los usuarios** (M1, M2, ..., M6) y la **antigüedad de sus cuentas** (en días).  

#### 🔍 Observaciones:

- **📈 Correlación positiva:**  
  A mayor antigüedad, el score tiende a aumentar.  
  Por ejemplo, los usuarios con **500 días** suelen alcanzar **M6**, lo que sugiere que los usuarios más antiguos son más activos o confiables.  

- **⚠️ Excepciones notables:**  
  Algunas cuentas recientes (ejemplo: **100 días**) tienen scores altos (**M5 o M6**), lo que podría indicar usuarios con **rápido crecimiento o alto valor** en la plataforma.  
""")

fig_scatter = px.scatter(
    df_final, 
    x="account_age_dias", 
    y="score", 
    size="num_operations", 
    color="num_operations",
    title="Relación entre Score y Antigüedad de la Cuenta",
    labels={"account_age_dias": "Antigüedad (días)", "score": "Score"},
    color_continuous_scale="viridis"
)
fig_scatter.update_layout(template="plotly_dark")
st.plotly_chart(fig_scatter, use_container_width=True)

# Gráfico 4: Evolución diaria del total depositado (línea interactiva)
st.markdown("""
### 📊 Evolución Diaria del Total Depositado  

Este gráfico de línea muestra la evolución del **monto total depositado por día** durante **enero de 2022**  
(los puntos de referencia incluyen fechas como **"Jan 2", "Jan 9", etc.**).  

#### 🔍 Observaciones:

- **📈 Picos significativos:**  
  - El **9 de enero** y el **23 de enero** presentan depósitos máximos de **5M y 4M**, respectivamente.  
  - Esto podría deberse a **promociones especiales** o **eventos que incentivan los depósitos**.  

- **📉 Tendencia estable con alta volatilidad:**  
  - Entre los picos, los depósitos oscilan entre **1M y 3M**, lo que indica **actividad constante** pero con fluctuaciones notables.  

- **⚠️ Caída final:**  
  - El **30 de enero** registra un mínimo de **1M**, lo que podría estar relacionado con un **descenso en la actividad post-evento**.  
""")

df_time_group = df_filtrado.groupby("operation_date")["operation_value"].sum().reset_index()
fig_line = px.line(
    df_time_group, 
    x="operation_date", 
    y="operation_value", 
    title="Evolución Diaria del Total Depositado",
    labels={"operation_date": "Fecha", "operation_value": "Total Depositado"},
    markers=True
)
fig_line.update_layout(template="plotly_dark")
st.plotly_chart(fig_line, use_container_width=True)

# Gráfico 5: Gráfico de pastel para identificar las zonas de depósito con mayor valor ingresado
st.markdown("""
### 📊 Distribución de Depósitos por Zona (Grafico de torta) 

Este gráfico de torta muestra el **porcentaje de depósitos** según la **ubicación geográfica** de los usuarios.  
Se identifican **tres zonas principales** con la siguiente distribución:  

- **🏆 Zona dominante:** **76.7%** (ejemplo: `"2dt8831e..."`).  
- **📍 Zonas secundarias:** **20.3% y 3.05%**.  

#### 🔍 Observaciones:

- **🌍 Concentración geográfica:**  
  - La zona con **76.7%** acapara la mayoría de los depósitos.  
  - Esto podría reflejar una **mayor adopción del servicio** o una **población con mayor poder adquisitivo**.  

- **📢 Oportunidades de expansión:**  
  - Las zonas con **3.05% y 20.3%** tienen menor participación.  
  - Esto sugiere un **potencial de crecimiento** mediante **estrategias de marketing localizadas**.  
""")

zone_deposits = df_filtrado.groupby("maplocation_name")["operation_value"].sum().reset_index()
zone_deposits = zone_deposits.sort_values("operation_value", ascending=False)
fig_pie = px.pie(
    zone_deposits, 
    names="maplocation_name", 
    values="operation_value", 
    title="Distribución de Depósitos por Zona",
    color_discrete_sequence=px.colors.qualitative.Pastel
)
fig_pie.update_layout(template="plotly_dark")
st.plotly_chart(fig_pie, use_container_width=True)

# Gráfico 6: Ranking visual interactivo – Top 5 usuarios por Depósito Total

top5_deposit = df_final.sort_values("total_deposit", ascending=False).head(5)
fig_top5 = px.bar(
    top5_deposit, 
    x="total_deposit", 
    y="user_id", 
    orientation="h", 
    title="Top 5 Usuarios por Depósito Total",
    labels={"user_id": "User ID", "total_deposit": "Depósito Total"},
    text="total_deposit",
    color="total_deposit",
    color_continuous_scale="magma"
)
fig_top5.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
fig_top5.update_layout(template="plotly_dark", yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig_top5, use_container_width=True)
st.markdown("""
## 🔎 Observaciones Generales  

### ⚠️ Inconsistencias en los Datos:  
- 📌 En la **tabla de métricas**, se detectan **usuarios duplicados**.  
  - Ejemplo: **user_id=827** aparece **dos veces con los mismos valores**.  
- ❗ Existen **valores faltantes** en columnas como **avg_deposit** y **last_operation** para algunos usuarios.  

### ⭐ Usuarios Destacados:  
- **👤 Usuario 827:**  
  - Alto **valor total depositado** (**7,027,900**) y **38 operaciones**.  
  - Sin embargo, su **avg_deposit** es **moderado (184,944)**.  
- **💰 Usuario 75bcf93d:**  
  - Realizó solo **5 operaciones**, pero con un **avg_deposit extremadamente alto (434,290)**.  
  - Esto sugiere un **perfil de alto valor** en la plataforma.  

### ❌ Posibles Errores en el Dataset:  
- **🔄 El último registro** de la tabla tiene un **user_id vacío**, lo que podría indicar un **error de carga**.  
- **📅 Fechas con formato incorrecto:**  
  - Ejemplo: `"2022-02-0900：00：00"` en lugar de `"2022-02-09 00:00:00"`.  

### ✅ Recomendaciones:  
- 🔍 **Investigar** la causa de la **concentración geográfica de depósitos**.  
- 🛠️ **Limpiar datos** duplicados o inconsistentes para obtener análisis más precisos.  
- 📊 **Analizar los picos de depósitos** (ej: **9 y 23 de enero**) para replicar estrategias exitosas.  
""")
