from taipy import Gui
import paho.mqtt.client as mqtt
import json
import threading
import logging
import pandas as pd
import time

# ===================== CONFIGURAÇÕES =====================
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "gear1/telemetria"

logging.basicConfig(
    filename="telemetria_taipy.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ===================== VARIÁVEIS GLOBAIS =====================
telemetria_lock = threading.Lock()
telemetria_dados = {}
conectado_event = threading.Event()
df_t = pd.DataFrame()

velocidade = 0
rpm = 0
marcha = "-"
status_msg = "🕒 Aguardando conexão com o broker MQTT..."

# ===================== FUNÇÕES MQTT =====================
def on_connect(client, userdata, flags, rc):
    global status_msg
    if rc == 0:
        logging.info("Conectado ao broker MQTT")
        status_msg = "✅ Conectado ao broker MQTT!"
        conectado_event.set()
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(f"Falha na conexão MQTT: {rc}")
        status_msg = f"❌ Falha na conexão: {rc}"

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

# ===================== THREAD DE ATUALIZAÇÃO =====================
def atualizar_dados():
    global df_t, velocidade, rpm, marcha, status_msg
    while True:
        if conectado_event.is_set():
            with telemetria_lock:
                dados = telemetria_dados.copy()
                # log para debug (você já tem esse print no emissor)
                print(f"Dados : {dados}")

                if dados:
                    try:
                        # transforma o dict em linha do dataframe
                        nova_linha = pd.DataFrame.from_dict(dados, orient='index').T
                    except Exception:
                        # fallback mais simples caso a estrutura seja estranha
                        nova_linha = pd.DataFrame([dados])

                    # atualiza dataframe (mantém as últimas 100 linhas)
                    df_t = pd.concat([nova_linha, df_t], ignore_index=True).head(100)

                    # atualiza métricas com a primeira linha (mais recente)
                    try:
                        ultima = df_t.iloc[0]
                        velocidade = ultima.get("speed", 0)
                        rpm = ultima.get("rpm", 0)
                        marcha = ultima.get("gear", "-")
                    except Exception as e:
                        logging.error(f"Erro ao extrair métricas da linha: {e}")
        time.sleep(0.5)

# ===================== INTERFACE TAIPY =====================
page = """
# 📡 Telemetria em Tempo Real

**Status:** {status_msg}

<div style="display:flex; gap:2rem; margin-bottom:1rem;">
  <div style="padding:8px; border-radius:8px; background:#f5f5f5; min-width:160px;">
    **Velocidade**  
    <div style="font-size:1.6rem; font-weight:600;">{velocidade} km/h</div>
  </div>

  <div style="padding:8px; border-radius:8px; background:#f5f5f5; min-width:160px;">
    **RPM**  
    <div style="font-size:1.6rem; font-weight:600;">{rpm}</div>
  </div>

  <div style="padding:8px; border-radius:8px; background:#f5f5f5; min-width:120px;">
    **Marcha**  
    <div style="font-size:1.6rem; font-weight:600;">{marcha}</div>
  </div>
</div>

<|{df_t}|table|width=100%|page_size=10|>
"""

# ===================== MAIN =====================
if __name__ == "__main__":
    # Inicia MQTT em thread (daemon)
    threading.Thread(target=iniciar_mqtt, daemon=True).start()
    # Inicia loop de atualização que popula df_t e métricas
    threading.Thread(target=atualizar_dados, daemon=True).start()
    # Roda GUI com refresh automático a cada 0.5s
    Gui(page).run(title="Telemetria iRacing", port=5000, update_interval=0.5)
