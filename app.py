
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Vacivida - Dashboard de Eventos Adversos", layout="wide")
st.title("💉 Vacivida - Dashboard de Eventos Adversos Pós-Vacinação")

# Nova URL funcional do arquivo CSV
url = "http://multibeat.com.br/estudonautapy/prod.vcvd_eventos_adverso.csv"

# Leitura dos dados
@st.cache_data
def carregar_dados():
    return pd.read_csv(url, encoding="latin1", sep=";")

df = carregar_dados()

# Filtros
st.sidebar.header("Filtros")
vacinas = st.sidebar.multiselect("Nome da Vacina", df['vcvd_vacinacaoname'].dropna().unique())
sexo = st.sidebar.multiselect("Sexo", df['vcvd_sexo'].dropna().unique())
uf = st.sidebar.multiselect("UF", df['vcvd_uf'].dropna().unique())

df_filtrado = df.copy()
if vacinas:
    df_filtrado = df_filtrado[df_filtrado['vcvd_vacinacaoname'].isin(vacinas)]
if sexo:
    df_filtrado = df_filtrado[df_filtrado['vcvd_sexo'].isin(sexo)]
if uf:
    df_filtrado = df_filtrado[df_filtrado['vcvd_uf'].isin(uf)]

# Gráfico de eventos adversos
st.subheader("📊 Distribuição dos Eventos Adversos por Tipo")
fig1, ax1 = plt.subplots()
df_filtrado['vcvd_evento_adverso'].value_counts().head(20).plot(kind='barh', ax=ax1)
ax1.set_xlabel("Número de Casos")
ax1.set_ylabel("Evento Adverso")
st.pyplot(fig1)

# Gráfico de linha temporal
st.subheader("📈 Evolução Temporal dos Casos")
df_filtrado['vcvd_data_notificacao'] = pd.to_datetime(df_filtrado['vcvd_data_notificacao'], errors='coerce')
df_tempo = df_filtrado.groupby(df_filtrado['vcvd_data_notificacao'].dt.to_period("M")).size()
fig2, ax2 = plt.subplots()
df_tempo.plot(kind='line', ax=ax2)
ax2.set_xlabel("Mês")
ax2.set_ylabel("Número de Casos")
st.pyplot(fig2)

# Download dos dados
st.subheader("📥 Baixar dados filtrados")
st.download_button(
    label="📁 Download Excel",
    data=df_filtrado.to_excel(index=False, engine='openpyxl'),
    file_name="dados_filtrados_vacivida.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
