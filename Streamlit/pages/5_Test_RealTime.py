import streamlit as st
import paho.mqtt.client as mqtt
import json
import time
import threading
import logging
import pandas as pd

logging.basicConfig(
    filename="telemetria_streamlit.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "gear1/#"  # Escuta todos os subcanais de gear1/

telemetria_lock = threading.Lock()
telemetria_dados = {}
conectado_event = threading.Event()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Conectado ao broker MQTT")
        conectado_event.set()
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(f"Falha na conexão MQTT: {rc}")

def on_message(client, userdata, msg):
    global telemetria_dados
    try:
        payload = json.loads(msg.payload.decode())
        with telemetria_lock:
            telemetria_dados = payload
        logging.info(f"Mensagem recebida: {payload}")
    except Exception as e:
        logging.error(f"Erro ao processar mensagem MQTT: {e}")

def iniciar_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        logging.error(f"Erro ao conectar MQTT: {e}")

if "mqtt_iniciado" not in st.session_state:
    threading.Thread(target=iniciar_mqtt, daemon=True).start()
    st.session_state.mqtt_iniciado = True

if "df_t" not in st.session_state:
    st.session_state.df_t = pd.DataFrame()

# Inicializa lista de sessões em session_state para controlar
if "sessoes_disponiveis" not in st.session_state:
    st.session_state.sessoes_disponiveis = ["Todas"]

# Inicializa valor selecionado da sessão
if "sessao_selecionada" not in st.session_state:
    st.session_state.sessao_selecionada = "Todas"

# --- Atualiza dataframe e sessões ---

with telemetria_lock:
    dados = telemetria_dados.copy()

if dados:
    nova_linha = pd.DataFrame.from_dict(dados, orient='index').T

    # Concatena nova linha no dataframe armazenado
    st.session_state.df_t = pd.concat([nova_linha, st.session_state.df_t], ignore_index=True)

    # Mantém só as últimas 100 linhas para performance
    st.session_state.df_t = st.session_state.df_t.head(100)

    # Atualiza lista de sessões com base nas sessões únicas do dataframe
    if 'session_name' in st.session_state.df_t.columns:
        sessao_unicas = st.session_state.df_t['session_name'].dropna().unique().tolist()
        # Mantém "Todas" + as sessões que ainda não estão listadas
        novas_sessoes = [s for s in sessao_unicas if s not in st.session_state.sessoes_disponiveis]
        if novas_sessoes:
            st.session_state.sessoes_disponiveis += novas_sessoes
            # Para evitar duplicados se houver algum bug, faz um set (opcional)
            st.session_state.sessoes_disponiveis = sorted(list(set(st.session_state.sessoes_disponiveis)))

# --- Sidebar para seleção de sessão com chave fixa ---
sessao_selecionada = st.sidebar.selectbox(
    "Selecione a Sessão",
    st.session_state.sessoes_disponiveis,
    index=st.session_state.sessoes_disponiveis.index(st.session_state.sessao_selecionada)
    if st.session_state.sessao_selecionada in st.session_state.sessoes_disponiveis else 0,
    key="sessao_selectbox"
)

# Atualiza sessão selecionada no session_state se mudou
if sessao_selecionada != st.session_state.sessao_selecionada:
    st.session_state.sessao_selecionada = sessao_selecionada

# --- Título ---
st.title("📡 Telemetria em Tempo Real")

status_placeholder = st.empty()
col1, col2, col3 = st.columns(3)
velocidade_placeholder = col1.empty()
rpm_placeholder = col2.empty()
marcha_placeholder = col3.empty()
placeholder = st.empty()

conectado_msg_exibido = False
tempo_conectado_inicio = None

if conectado_event.is_set():
    if not conectado_msg_exibido:
        status_placeholder.success("✅ Conectado ao broker MQTT!")
        tempo_conectado_inicio = time.time()
        conectado_msg_exibido = True
    else:
        if time.time() - tempo_conectado_inicio > 3:
            status_placeholder.empty()
else:
    status_placeholder.warning("🕒 Aguardando conexão com o broker MQTT...")

# Filtra dataframe pela sessão selecionada (ou mostra tudo)
if not st.session_state.df_t.empty:
    if st.session_state.sessao_selecionada != "Todas" and 'session_name' in st.session_state.df_t.columns:
        df_filtrado = st.session_state.df_t[st.session_state.df_t['session_name'] == st.session_state.sessao_selecionada]
    else:
        df_filtrado = st.session_state.df_t

    if not df_filtrado.empty:
        ultima_linha = df_filtrado.iloc[0]
        velocidade_placeholder.metric("Velocidade", f"{ultima_linha.get('speed', 0)} km/h")
        rpm_placeholder.metric("RPM", f"{ultima_linha.get('rpm', 0)}")
        marcha_placeholder.metric("Marcha", f"{ultima_linha.get('gear', '-')}")

        with placeholder.container():
            st.dataframe(df_filtrado)
    else:
        placeholder.info("📭 Nenhum dado para essa sessão no momento.")
else:
    placeholder.info("📭 Aguardando dados de telemetria no tópico MQTT...")

time.sleep(0.5)
