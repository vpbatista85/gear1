import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import requests
import json
import os

CONFIG_FILE = "config.json"

def carregar_config():
    """Carrega o IP salvo no arquivo config.json, se existir."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

def salvar_config(config):
    """Salva o IP no arquivo config.json."""
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

def descobrir_ip():
    """Tenta descobrir o IP do client local automaticamente."""
    possiveis_ips = ["127.0.0.1", "localhost"]
    for ip in possiveis_ips:
        try:
            response = requests.get(f"http://{ip}:5000/get_ip", timeout=3)
            response.raise_for_status()
            return response.json().get("ip")
        except requests.exceptions.RequestException:
            continue
    return None
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

# Carregar configuração salva
config = carregar_config()
ip_cliente = config.get("ip_cliente", "")

# Se não houver IP salvo, tentar descobrir automaticamente
if not ip_cliente:
    st.write("🔄 Descobrindo o IP do client local...")
    ip_cliente = descobrir_ip()
    if ip_cliente:
        config["ip_cliente"] = ip_cliente
        salvar_config(config)
        st.success(f"IP {ip_cliente} detectado e salvo!")
    else:
        st.error("❌ Não foi possível detectar o IP do client local.")


# # URL do servidor Flask
# FLASK_URL = "http://127.0.0.1:5000/dados"

# st.title("📊 Dashboard de Dados")

# st.write("🔄 Obtendo dados do servidor Flask...")

# # Fazer requisição ao servidor Flask
# try:
#     response = requests.get(FLASK_URL)
#     response.raise_for_status()  # Lança erro se a resposta for ruim (ex: 404 ou 500)
#     dados = response.json()  # Converte resposta para JSON
    
#     if isinstance(dados, list) and len(dados) > 0:
#         df = pd.DataFrame(dados)  # Converte JSON para DataFrame
#         st.write("✅ Dados carregados com sucesso!")
#         st.dataframe(df)  # Exibe os dados como tabela no Streamlit
#     else:
#         st.warning("⚠️ Nenhum dado disponível no servidor Flask.")
# except requests.exceptions.RequestException as e:
#     st.error(f"❌ Erro ao conectar ao servidor Flask: {e}")

# Interface do Streamlit
st.title("📊 Dashboard de Dados")

if ip_cliente:
    url_client = f"http://{ip_cliente}:5000/dados"
    st.write("🔄 Obtendo dados do servidor Flask...")

    try:
        response = requests.get(url_client, timeout=5)
        response.raise_for_status()
        dados = response.json()
        st.table(dados)
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Erro ao conectar ao servidor Flask: {e}")
else:
    st.warning("⚠️ Nenhum IP detectado. Verifique se o client local está rodando.")