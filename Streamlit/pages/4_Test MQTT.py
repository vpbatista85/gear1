import streamlit as st
import paho.mqtt.client as mqtt

# Inicializa a variável global
if 'mqtt_message' not in st.session_state:
    st.session_state['mqtt_message'] = 'Esperando mensagem...'

# Callback para quando uma nova mensagem for recebida
def on_message(client, userdata, msg):
    st.session_state['mqtt_message'] = msg.payload.decode()

# Configura o cliente MQTT
client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.subscribe("gear1/telemetria")
client.loop_start()

# Exibe a última mensagem recebida
st.title("Monitor Live")
st.write("Última mensagem:")
st.code(st.session_state['mqtt_message'])

# Atualiza a cada segundo para mostrar mensagens novas
st.experimental_rerun()
