import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests



st.set_page_config(
    page_title="Gear 1 Real Time",
    page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
    layout="wide",
    initial_sidebar_state="expanded",
    # menu_items={
    #     'Get Help': 'https://www.extremelycoolapp.com/help',
    #     'Report a bug': "https://www.extremelycoolapp.com/bug",
    #     'About': "# This is a header. This is an *extremely* cool app!"
    # }
)
st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

# st.title(":green[EM CONSTRUÇÃO]")

# URL do servidor Flask
FLASK_URL = "http://127.0.0.1:5000/dados"

st.title("📊 Dashboard de Dados")

st.write("🔄 Obtendo dados do servidor Flask...")

# Fazer requisição ao servidor Flask
try:
    response = requests.get(FLASK_URL)
    response.raise_for_status()  # Lança erro se a resposta for ruim (ex: 404 ou 500)
    dados = response.json()  # Converte resposta para JSON
    
    if isinstance(dados, list) and len(dados) > 0:
        df = pd.DataFrame(dados)  # Converte JSON para DataFrame
        st.write("✅ Dados carregados com sucesso!")
        st.dataframe(df)  # Exibe os dados como tabela no Streamlit
    else:
        st.warning("⚠️ Nenhum dado disponível no servidor Flask.")
except requests.exceptions.RequestException as e:
    st.error(f"❌ Erro ao conectar ao servidor Flask: {e}")

