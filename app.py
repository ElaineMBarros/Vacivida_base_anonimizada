
import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Vacivida - Dashboard de Eventos Adversos", layout="wide")

st.title("💉 Vacivida - Dashboard de Eventos Adversos Pós-Vacinação")

# Caminho para o arquivo parquet
caminho_arquivo = os.path.join("data", "eventos_adversos.parquet")

# Leitura do arquivo parquet
@st.cache_data
def carregar_dados():
    return pd.read_parquet(caminho_arquivo)

df = carregar_dados()

# Filtros
st.sidebar.header("Filtros")
vacina = st.sidebar.multiselect("Nome da Vacina", options=df['nome_vacina'].dropna().unique())
sexo = st.sidebar.multiselect("Sexo", options=df['sexo'].dropna().unique())
estado = st.sidebar.multiselect("UF", options=df['estado'].dropna().unique())

df_filtrado = df.copy()
if vacina:
    df_filtrado = df_filtrado[df_filtrado['nome_vacina'].isin(vacina)]
if sexo:
    df_filtrado = df_filtrado[df_filtrado['sexo'].isin(sexo)]
if estado:
    df_filtrado = df_filtrado[df_filtrado['estado'].isin(estado)]

st.subheader("📊 Distribuição dos Eventos Adversos por Tipo")
fig1, ax1 = plt.subplots()
df_filtrado['evento_adverso'].value_counts().plot(kind='bar', ax=ax1)
ax1.set_xlabel("Evento Adverso")
ax1.set_ylabel("Número de Casos")
st.pyplot(fig1)

st.subheader("📈 Evolução Temporal dos Casos")
df_filtrado['data_notificacao'] = pd.to_datetime(df_filtrado['data_notificacao'], errors='coerce')
df_tempo = df_filtrado.groupby(df_filtrado['data_notificacao'].dt.to_period("M")).size()
fig2, ax2 = plt.subplots()
df_tempo.plot(kind='line', ax=ax2)
ax2.set_xlabel("Mês")
ax2.set_ylabel("Número de Casos")
st.pyplot(fig2)

st.subheader("📥 Baixar dados filtrados")
st.download_button(
    label="📁 Download Excel",
    data=df_filtrado.to_excel(index=False, engine='openpyxl'),
    file_name="dados_filtrados_vacivida.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
