#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 1. Montar Google Drive
from google.colab import drive
drive.mount('/content/drive')

# 2. Caminho da pasta de dados (atualize se necessário)
pasta = "/content/drive/MyDrive/vacivida"

# 3. Importar libs
import pandas as pd
import os
from zipfile import ZipFile

# 4. Listar arquivos CSV
arquivos = [f for f in os.listdir(pasta) if f.endswith(".csv")]
print("Arquivos encontrados:", arquivos)

# 5. Função para carregar e mostrar amostra
# 📥 Nova função para carregar com encoding e delimitador automático
def carregar_dataframes(arquivos, pasta):
    dfs = {}
    for f in arquivos:
        path = os.path.join(pasta, f)
        try:
            df = pd.read_csv(path, encoding='latin1', sep=None, engine='python')  # <- ajuste aqui
            print(f"\n✅ {f}: {df.shape[0]} linhas × {df.shape[1]} colunas")
            display(df.head(3))
            dfs[f] = df
        except Exception as e:
            print(f"❌ Erro ao ler {f}: {e}")
    return dfs

# 🚀 Roda de novo com a função corrigida
dfs = carregar_dataframes(arquivos, pasta)

# 6. (Opcional) Selecionar colunas principais ou Converter para Parquet
for nome, df in dfs.items():
    df.to_parquet(f"/content/{nome[:-4]}.parquet", index=False)
    print(f"Salvo: {nome[:-4]}.parquet")

# 7. (Opcional) Criar ZIP com todos os arquivos Parquet
with ZipFile('/content/vacivida_parquet.zip', 'w') as zipf:
    for nome in dfs:
        fname = nome[:-4] + '.parquet'
        zipf.write(fname)
print("📦 ZIP (vacivida_parquet.zip) criado!")


# In[ ]:


# Ver quantos arquivos carregamos e quantas linhas cada um tem
for nome, df in dfs.items():
    print(f"📄 {nome}: {df.shape[0]} linhas × {df.shape[1]} colunas")


# In[ ]:


# Carrega o DataFrame principal
df_eventos = dfs["prod.vcvd_eventos_adverso.csv"]

# Ver as colunas disponíveis
df_eventos.columns.tolist()


# In[ ]:


# Ver tipos de eventos adversos mais frequentes
df_eventos['vcvd_evento_adverso'].value_counts(dropna=False).head(10)


# In[ ]:


# Converter colunas para datetime
df_eventos['data_dose'] = pd.to_datetime(df_eventos['vcvd_data_primeira_dose'], errors='coerce')
df_eventos['data_evento'] = pd.to_datetime(df_eventos['vcvd_data_notificacao'], errors='coerce')

# Calcular diferença em dias
df_eventos['dias_ate_evento'] = (df_eventos['data_evento'] - df_eventos['data_dose']).dt.days

# Ver resumo
df_eventos['dias_ate_evento'].describe()


# In[ ]:





# In[ ]:


df_eventos_validos['dias_ate_evento'].hist(bins=30)
plt.title("Dias entre vacinação e evento adverso (0–60 dias)")
plt.xlabel("Dias")
plt.ylabel("Número de casos")
plt.grid(True)
plt.show()


# In[ ]:


# Top 10 tipos de eventos
df_eventos_validos['vcvd_evento_adverso'].value_counts().head(10)


# In[ ]:


import matplotlib.pyplot as plt
import seaborn as sns

# Seleciona os 5 eventos mais frequentes
top_eventos = df_eventos_validos['vcvd_evento_adverso'].value_counts().head(5).index
df_top_eventos = df_eventos_validos[df_eventos_validos['vcvd_evento_adverso'].isin(top_eventos)]

# Ordena os tipos de evento por mediana do tempo
ordem = df_top_eventos.groupby('vcvd_evento_adverso')['dias_ate_evento'].median().sort_values().index

# Gráfico aprimorado
plt.figure(figsize=(14, 8))  # Tamanho maior
sns.set(style="whitegrid", font_scale=1.2)

sns.boxplot(
    data=df_top_eventos,
    x='vcvd_evento_adverso',
    y='dias_ate_evento',
    order=ordem,
    palette="crest"
)

plt.title("⏱️ Tempo até evento por tipo de evento adverso (Top 5)", fontsize=16)
plt.xlabel("Tipo de evento adverso", fontsize=13)
plt.ylabel("Dias após vacinação", fontsize=13)
plt.xticks(rotation=30, ha="right", fontsize=11, wrap=True)
plt.tight_layout()
plt.grid(True)
plt.show()


# In[ ]:


# Contagem dos eventos por cidade (nome do paciente)
contagem_cidade = df_eventos_validos['vcvd_municipio_pacientename'].value_counts().head(10)

# Criar o gráfico
ax = contagem_cidade.plot(kind='bar', figsize=(12, 6), color='lightseagreen')
plt.title("🏙️ Eventos adversos por cidade (Top 10 - 0 a 60 dias)")
plt.xlabel("Cidade")
plt.ylabel("Número de casos")
plt.xticks(rotation=45, ha='right', fontsize=11)
plt.yticks(fontsize=11)
plt.grid(True, axis='y')
plt.tight_layout()

# Adiciona os valores nas colunas
for i, v in enumerate(contagem_cidade):
    ax.text(i, v + max(contagem_cidade)*0.01, f"{v:,}", ha='center', va='bottom', fontsize=11)

plt.show()



# In[ ]:


# Certifique-se de ter carregado o arquivo de doses
df_dose = dfs["prod.vcvd_dose.csv"]

# Merge com base no Id
df_eventos_dose = pd.merge(df_eventos_validos, df_dose, on='Id', how='left')

# Ver contagem por tipo de dose
df_eventos_dose['pdsp_imunob_vaccinemaster_dosesname'].value_counts()


# In[ ]:


df_dose.columns.tolist()


# In[ ]:


# Recriar merge com base apenas nos eventos válidos com dias preenchidos
df_eventos_validos = df_eventos[df_eventos['dias_ate_evento'].between(0, 60)]
df_eventos_dose = pd.merge(df_eventos_validos, df_dose, on="Id", how="left")


# In[ ]:


df_test = df_eventos_dose[df_eventos_dose['vcvd_tipo_dose'].notna() & df_eventos_dose['dias_ate_evento'].notna()]

sns.boxplot(data=df_test, x='vcvd_tipo_dose', y='dias_ate_evento')
plt.xticks(rotation=45)
plt.title("Tempo até evento por tipo de dose (sem filtro)")
plt.grid(True)
plt.tight_layout()
plt.show()


# In[ ]:


# Criar faixa etária
bins = [0, 12, 18, 30, 45, 60, 75, 90, 150]
labels = ['0–11', '12–17', '18–29', '30–44', '45–59', '60–74', '75–89', '90+']
df_eventos['faixa_etaria'] = pd.cut(df_eventos['vcvd_idade'], bins=bins, labels=labels)

# Contagem por faixa
df_eventos['faixa_etaria'].value_counts().sort_index().plot(kind='bar', figsize=(10, 6))
plt.title("📊 Eventos adversos por faixa etária")
plt.xlabel("Faixa etária")
plt.ylabel("Número de casos")
plt.grid(True)
plt.tight_layout()
plt.show()


# In[ ]:


# Map the numeric 'vcvd_sexo' to descriptive labels.
df_eventos['sexo_label'] = df_eventos['vcvd_sexo'].map({0: 'Ignorado', 1: 'Masculino', 2: 'Feminino'})

# Conta os valores
contagem = df_eventos['sexo_label'].value_counts(dropna=False)

# Cria o gráfico
ax = contagem.plot(kind='bar', figsize=(6, 4), color=["gray", "cornflowerblue", "lightpink"])
plt.title("⚥ Eventos adversos por sexo")
plt.xlabel("Sexo")
plt.ylabel("Número de casos")
plt.xticks(rotation=0)
plt.grid(True)
plt.tight_layout()

# Adiciona os rótulos de valor em cima das barras
for i, v in enumerate(contagem):
    ax.text(i, v + max(contagem)*0.01, f"{v:,}", ha='center', va='bottom', fontsize=11)

plt.show()


# In[ ]:


import matplotlib.pyplot as plt
import seaborn as sns

# Create age groups
bins = [0, 12, 18, 30, 45, 60, 75, 90, 150]
labels = ['0–11', '12–17', '18–29', '30–44', '45–59', '60–74', '75–89', '90+']
df_eventos['faixa_etaria'] = pd.cut(df_eventos['vcvd_idade'], bins=bins, labels=labels)

# Agrupamento
df_sexo_faixa = df_eventos.groupby(['faixa_etaria', 'sexo_label']).size().unstack(fill_value=0)

# Criação do gráfico
ax = df_sexo_faixa.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='Set2')
plt.title("🎯 Eventos adversos por faixa etária e sexo", fontsize=16)
plt.xlabel("Faixa Etária", fontsize=13)
plt.ylabel("Número de Casos", fontsize=13)
plt.xticks(rotation=0, fontsize=11)
plt.yticks(fontsize=11)
plt.legend(title="Sexo")
plt.grid(True, axis='y')
plt.tight_layout()

# Adicionar valores em cima das barras empilhadas
for i, faixa in enumerate(df_sexo_faixa.index):
    total = 0
    for sexo in df_sexo_faixa.columns:
        valor = df_sexo_faixa.loc[faixa, sexo]
        if valor > 0:
            ax.text(i, total + valor / 2, str(valor), ha='center', va='center', fontsize=10, color='black')
            total += valor

plt.show()


# In[ ]:


# Agrupa por faixa e tipo de evento
eventos_por_faixa = df_eventos.groupby(['faixa_etaria', 'vcvd_evento_adverso']).size().reset_index(name='qtd')

# Para cada faixa etária, pegar os 3 eventos mais comuns
top_eventos_faixa = eventos_por_faixa.groupby('faixa_etaria').apply(
    lambda x: x.nlargest(3, 'qtd')
).reset_index(drop=True)

top_eventos_faixa


# In[ ]:


# Cria uma versão truncada para os nomes de evento
top_eventos_faixa['evento_resumido'] = top_eventos_faixa['vcvd_evento_adverso'].str.slice(0, 40) + "..."


# In[ ]:


plt.figure(figsize=(14, 7))
sns.barplot(data=top_eventos_faixa, x='faixa_etaria', y='qtd', hue='evento_resumido', dodge=True)

plt.title("🏷️ Top 3 eventos adversos por faixa etária (resumo)")
plt.xlabel("Faixa Etária")
plt.ylabel("Número de casos")
plt.xticks(rotation=0)
plt.legend(title="Evento Adverso", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, axis='y')
plt.tight_layout()
plt.show()


# In[ ]:


import pandas as pd
import os

# Caminho da pasta onde estão os CSVs no Google Drive
caminho_drive = "/content/drive/MyDrive/vacivida"  # <- ajuste esse nome aqui

# Pasta de destino local
os.makedirs("/content/drive/MyDrive/vacivida", exist_ok=True)

# Converter todos os CSVs para Parquet
for file in os.listdir(caminho_drive):
    if file.endswith(".csv"):
        path_csv = os.path.join(caminho_drive, file)
        path_parquet = os.path.join("/content/drive/MyDrive/vacivida", file.replace(".csv", ".parquet"))
        try:
            df = pd.read_csv(path_csv, encoding='latin1', sep=None, engine='python')
            df.to_parquet(path_parquet, index=False)
            print(f"✅ Convertido: {file}")
        except Exception as e:
            print(f"❌ Erro com {file}: {e}")


# In[ ]:


# 1. Montar o Drive
from google.colab import drive
drive.mount('/content/drive')

# 2. Importar bibliotecas
import pandas as pd
from io import BytesIO
from IPython.display import FileLink
import matplotlib.pyplot as plt # Import matplotlib

# 3. Caminho até o arquivo .parquet no Drive
CAMINHO_PARQUET = '/content/prod.vcvd_eventos_adverso.parquet'

# 4. Carregar os dados
df_eventos = pd.read_parquet(CAMINHO_PARQUET)

# 5. (Opcional) Pré-processar: faixa etária e sexo
bins = [0, 12, 18, 30, 45, 60, 75, 90, 150]
labels = ['0–11', '12–17', '18–29', '30–44', '45–59', '60–74', '75–89', '90+']
df_eventos['faixa_etaria'] = pd.cut(df_eventos['vcvd_idade'], bins=bins, labels=labels)
df_eventos['sexo_label'] = df_eventos['vcvd_sexo'].map({0: 'Ignorado', 1: 'Masculino', 2: 'Feminino'})

# 6. (Opcional) Filtrar só eventos válidos
df_filtrado = df_eventos[
    (df_eventos['faixa_etaria'].notna()) &
    (df_eventos['vcvd_sexo'].isin([1, 2]))
]

# 7. Gerar Excel
caminho_excel = "/content/eventos_filtrados_vacivida.xlsx"
df_filtrado.to_excel(caminho_excel, index=False)

# 8. Exibir botão de download
FileLink(caminho_excel)

# Add plotting code from cell 2625bbb4
# Conta os valores
contagem = df_eventos['sexo_label'].value_counts(dropna=False)

# Cria o gráfico
ax = contagem.plot(kind='bar', figsize=(6, 4), color=["gray", "cornflowerblue", "lightpink"])
plt.title("⚥ Eventos adversos por sexo")
plt.xlabel("Sexo")
plt.ylabel("Número de casos")
plt.xticks(rotation=0)
plt.grid(True)
plt.tight_layout()

# Adiciona os rótulos de valor em cima das barras
for i, v in enumerate(contagem):
    ax.text(i, v + max(contagem)*0.01, f"{v:,}", ha='center', va='bottom', fontsize=11)

plt.show()


# In[ ]:


# Convert columns to datetime
df_eventos['data_dose'] = pd.to_datetime(df_eventos['vcvd_data_primeira_dose'], errors='coerce')
df_eventos['data_evento'] = pd.to_datetime(df_eventos['vcvd_data_notificacao'], errors='coerce')

# Calculate difference in days
df_eventos['dias_ate_evento'] = (df_eventos['data_evento'] - df_eventos['data_dose']).dt.days

# Filter for events within 0 to 60 days
df_eventos_validos = df_eventos[df_eventos['dias_ate_evento'].between(0, 60)].copy()

df_eventos_validos['dias_ate_evento'].hist(bins=30)
plt.title("Dias entre vacinação e evento adverso (0–60 dias)")
plt.xlabel("Dias")
plt.ylabel("Número de casos")
plt.grid(True)
plt.show()


# In[ ]:


# Conta os valores
contagem = df_eventos['sexo_label'].value_counts(dropna=False)

# Cria o gráfico
ax = contagem.plot(kind='bar', figsize=(6, 4), color=["gray", "cornflowerblue", "lightpink"])
plt.title("⚥ Eventos adversos por sexo")
plt.xlabel("Sexo")
plt.ylabel("Número de casos")
plt.xticks(rotation=0)
plt.grid(True)
plt.tight_layout()

# Adiciona os rótulos de valor em cima das barras
for i, v in enumerate(contagem):
    ax.text(i, v + max(contagem)*0.01, f"{v:,}", ha='center', va='bottom', fontsize=11)

plt.show()

