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
MQTT_TOPIC = "gear1/telemetria"

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

# Coloque o título logo no início, para aparecer no topo da página
st.title("📡 Telemetria em Tempo Real")

# Agora crie os placeholders para status, métricas e dataframe
status_placeholder = st.empty()


col1, col2, col3 = st.columns(3)
velocidade_placeholder = col1.empty()
rpm_placeholder = col2.empty()
marcha_placeholder = col3.empty()
placeholder = st.empty()

conectado_msg_exibido = False
tempo_conectado_inicio = None

while True:
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

    with telemetria_lock:
        dados = telemetria_dados.copy()

    if dados:
        nova_linha = pd.DataFrame.from_dict(dados, orient='index').T
        st.session_state.df_t = pd.concat([nova_linha, st.session_state.df_t], ignore_index=True)
        st.session_state.df_t = st.session_state.df_t.head(100)

        ultima_linha = st.session_state.df_t.iloc[0]

        velocidade_placeholder.metric("Velocidade", f"{ultima_linha.get('speed', 0)} km/h")
        rpm_placeholder.metric("RPM", f"{ultima_linha.get('rpm', 0)}")
        marcha_placeholder.metric("Marcha", f"{ultima_linha.get('gear', '-')}")

        with placeholder.container():
            st.dataframe(st.session_state.df_t)
    else:
        placeholder.info("📭 Aguardando dados de telemetria no tópico MQTT...")

    time.sleep(0.5)
