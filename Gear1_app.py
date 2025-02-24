import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from flask import Flask, request, jsonify
import json


# Se estiver rodando no Windows, importe o pywin32
if sys.platform == "win32":
    import win32com.client

# Chave para armazenar o IP na sessão do Streamlit
if "ip_do_client" not in st.session_state:
    st.session_state["ip_do_client"] = None

# Simula um endpoint na própria página principal
def registrar_ip():
    """Recebe o IP via requisição JSON e armazena no estado do Streamlit."""
    try:
        req = st.query_params()  # Tenta obter dados da URL
        ip = req.get("ip", [None])[0]
        if ip:
            st.session_state["ip_do_client"] = ip
            return json.dumps({"mensagem": "IP registrado com sucesso!"})
    except Exception as e:
        return json.dumps({"erro": str(e)})

# Tenta registrar IP (caso tenha sido enviado)
registrar_ip()


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

# Exibe o IP registrado na interface
st.title("📡 Controle do iRacing - Gear1App")
st.write("🔄 Descobrindo o IP do client local...")

if st.session_state["ip_do_client"]:
    st.success(f"✅ IP do client detectado: {st.session_state['ip_do_client']}")
else:
    st.warning("⚠️ Nenhum IP detectado. Verifique se o client local está rodando.")

st.write("📊 Dashboard de Dados em Tempo Real...")

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


