# 6_RealTime_Dashboard.py

import streamlit as st
import pandas as pd
import json
import logging
from streamlit_autorefresh import st_autorefresh
import paho.mqtt.client as mqtt
import threading

# ---- CONFIGS STREAMLIT UI ----
st.set_page_config(page_title="Monitor Live", layout="wide")
st.logo("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", size="large")
st.title("📡 :green[Dados em Tempo Real - Gear1]")
st_autorefresh(interval=5000, key="auto-refresh")

# MQTT Configs
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "gear1/telemetria"

st.set_page_config(page_title="Dados em Tempo Real", layout="wide")

# ---------- ESTADO INICIAL ----------
if "telemetria_dados" not in st.session_state:
    st.session_state.telemetria_dados = []

if "mqtt_conectado" not in st.session_state:
    st.session_state.mqtt_conectado = False

# ---------- CALLBACK MQTT ----------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        st.session_state.mqtt_conectado = True
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Erro na conexão MQTT: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        dados = json.loads(payload)
        print(f"Mensagem recebida: {payload}")
        st.session_state.telemetria_dados.append(dados)
    except Exception as e:
        print(f"Erro ao processar mensagem MQTT: {e}")

# ---------- CONECTA AO BROKER ----------
def iniciar_mqtt():
    if not st.session_state.mqtt_conectado:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()

# ---------- INICIAR MQTT ----------
iniciar_mqtt()

# ---------- INTERFACE ----------
st.title("📡 Dados em Tempo Real - Gear1")

dados = st.session_state.telemetria_dados

if dados:
    df = pd.DataFrame(dados)
    st.dataframe(df.tail(20), use_container_width=True)
else:
    st.warning("Nenhuma sessão selecionada ou dados indisponíveis ainda.")

# ---------- ATUALIZAÇÃO AUTOMÁTICA ----------
# st.rerun()