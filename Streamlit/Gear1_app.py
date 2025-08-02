import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
# from flask import Flask, request, jsonify
import json
import os
import requests


# Se estiver rodando no Windows, importe o pywin32
if sys.platform == "win32":
    import win32com.client

IP_FILE = "client_ip.json"  # Arquivo para armazenar o IP do client

# Função para salvar o IP do client no arquivo JSON
def salvar_ip(ip):
    with open(IP_FILE, "w") as f:
        json.dump({"ip": ip}, f)

# Função para carregar o IP salvo
def carregar_ip():
    if os.path.exists(IP_FILE):
        with open(IP_FILE, "r") as f:
            data = json.load(f)
            return data.get("ip")
    return None




st.set_page_config(
    page_title="Gear 1 HQ",
    page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
    layout="wide",
    initial_sidebar_state="expanded",
    # menu_items={
    #     'Get Help': 'https://www.extremelycoolapp.com/help',
    #     'Report a bug': "https://www.extremelycoolapp.com/bug",
    #     'About': "# This is a header. This is an *extremely* cool app!"
    # }
)
st.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png")


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1moly2-480x79.png",width=198)
with col2:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1chem2-480x79.png",width=198)
with col3:
    st.image("https://gear1.gg/wp-content/uploads/2023/09/CompCableAsset-1@378x-300x280.png",width=78)
with col4:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1forms-480x104.png",width=198)
st.title(":green[Bem-vindo ao Gear One Head Quarter]")

st.write(":green[Utilize o menu à esquerda para navegar entre as páginas.]")


# st.title("📡 Controle do iRacing - Gear1App")

# # 🔄 Tenta buscar o IP do client local automaticamente
# st.write("🔄 Descobrindo o IP do client local...")

# try:
#     response = requests.get("http://localhost:5001/get_ip", timeout=3)  # 3s de timeout
#     if response.status_code == 200:
#         ip = response.json().get("ip")
#         st.write(f"✅ IP detectado: {ip}")
#     else:
#         st.write("⚠️ Não foi possível detectar o IP do client local.")
# except requests.exceptions.RequestException:
#     st.write("⚠️ Não foi possível detectar o IP do client local.")

# st.write("📊 Dashboard de Dados em Tempo Real...")

# app = Flask(__name__)

# ip_do_client = None  # Armazena o IP do client local

# @app.route('/registrar_ip', methods=['POST'])
# def registrar_ip():
#     global ip_do_client
#     dados = request.get_json()
#     ip_do_client = dados.get("ip")
#     print(f"Novo IP registrado: {ip_do_client}")
#     return jsonify({"mensagem": "IP registrado com sucesso!"}), 200

# @app.route('/obter_ip', methods=['GET'])
# def obter_ip():
#     """Endpoint que retorna o IP registrado"""
#     return jsonify({"ip": ip_do_client or "Nenhum IP registrado ainda"}), 200


