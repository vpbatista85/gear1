import dash
from dash import dcc, html, dash_table
from dash.dependencies import Output, Input, State
import pandas as pd
import paho.mqtt.client as mqtt
import threading
import json
import numpy as np
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.FileHandler("telemetria_debug.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Lock para evitar race conditions
telemetria_lock = threading.Lock()

# Guarda somente a ÚLTIMA mensagem por session
ultima_mensagem_por_session = {}
lista_sessions = set()

# MQTT config
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "gear1/#"

COLUNAS_CARROS = [
    'CarIdxBestLapNum', 'CarIdxBestLapTime', 'CarIdxClass', 'CarIdx',
    'CarIdxClassPosition', 'CarIdxEstTime', 'CarIdxF2Time', 'CarIdxFastRepairsUsed',
    'CarIdxGear', 'CarIdxLap', 'CarIdxLapCompleted', 'CarIdxLapDistPct',
    'CarIdxLastLapTime', 'CarIdxOnPitRoad', 'CarIdxP2P_Count', 'CarIdxP2P_Status',
    'CarIdxPaceFlags', 'CarIdxPaceLine', 'CarIdxPaceRow', 'CarIdxPosition',
    'CarIdxQualTireCompound', 'CarIdxQualTireCompoundLocked', 'CarIdxRPM',
    'CarIdxSessionFlags', 'CarIdxSteer', 'CarIdxTireCompound', 'CarIdxTrackSurface',
    'CarIdxTrackSurfaceMaterial',# 'CarIdxSpeed'
]

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Conectado ao broker MQTT")
        client.subscribe(MQTT_TOPIC_BASE)
    else:
        logging.error(f"Falha na conexão MQTT: {rc}")

def on_message(client, userdata, msg):
    global ultima_mensagem_por_session, lista_sessions
    try:
        payload_str = msg.payload.decode()
        payload = json.loads(payload_str)
        session_name = payload.get("session_name", "unknown_session")
        with telemetria_lock:
            lista_sessions.add(session_name)
            ultima_mensagem_por_session[session_name] = payload

        # Loga mensagem bruta
        logging.debug(f"[RAW MQTT] Sessão: {session_name} | Mensagem completa: {payload_str}")
        logging.debug(f"Mensagem recebida para sessão '{session_name}'")
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

def montar_dataframe_por_carro(mensagem):
    if not mensagem or not isinstance(mensagem, dict):
        return pd.DataFrame()

    max_carros = 0
    for c in COLUNAS_CARROS:
        val = mensagem.get(c)
        if isinstance(val, (list, tuple, np.ndarray)):
            max_carros = max(max_carros, len(val))

    dados_por_carro = {c: [] for c in COLUNAS_CARROS}

    for i in range(max_carros):
        for c in COLUNAS_CARROS:
            val = mensagem.get(c)
            if isinstance(val, (list, tuple, np.ndarray)):
                dados_por_carro[c].append(val[i] if i < len(val) else np.nan)
            else:
                dados_por_carro[c].append(np.nan)

    df = pd.DataFrame(dados_por_carro)

    # Garantir coluna CarIdx (se ausente, preenche com 0..N-1)
    if 'CarIdx' not in df.columns or df['CarIdx'].isnull().all():
        df['CarIdx'] = list(range(len(df)))

    return df

threading.Thread(target=iniciar_mqtt, daemon=True).start()

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("📡 Telemetria em Tempo Real"),
    html.Div(id='status'),
    html.Label("Selecione o session_name:"),
    dcc.Dropdown(
        id='dropdown-sessions',
        options=[],
        value="",
        placeholder="Escolha uma sessão..."
    ),
    html.Div([
        html.Div(id='velocidade', style={'display': 'inline-block', 'marginRight': 40}),
        html.Div(id='rpm', style={'display': 'inline-block', 'marginRight': 40}),
        html.Div(id='marcha', style={'display': 'inline-block', 'marginRight': 40}),
    ]),
    dash_table.DataTable(
        id='tabela-telemetria',
        columns=[],
        data=[],
        page_size=15,
        style_table={'overflowX': 'auto', 'maxHeight': '60vh', 'overflowY': 'auto'},
    ),
    dcc.Interval(id='intervalo-atualizacao', interval=500, n_intervals=0)
])

@app.callback(
    [Output('dropdown-sessions', 'options'),
     Output('dropdown-sessions', 'value')],
    Input('intervalo-atualizacao', 'n_intervals'),
    State('dropdown-sessions', 'value')
)
def atualizar_dropdown_sessions(n, valor_atual):
    with telemetria_lock:
        options = [{"label": s, "value": s} for s in sorted(lista_sessions)]
    if not options:
        return [{"label": "Nenhuma sessão encontrada", "value": ""}], ""
    valores_validos = [opt["value"] for opt in options]
    if valor_atual not in valores_validos:
        return options, valores_validos[0]
    return options, valor_atual

@app.callback(
    [Output("status", "children"),
     Output("velocidade", "children"),
     Output("rpm", "children"),
     Output("marcha", "children"),
     Output("tabela-telemetria", "columns"),
     Output("tabela-telemetria", "data")],
    Input("intervalo-atualizacao", "n_intervals"),
    State("dropdown-sessions", "value")
)
def atualizar_telemetria(n, session_selected):
    if not session_selected:
        return "", "0 km/h", "0", "-", [], []

    with telemetria_lock:
        mensagem = ultima_mensagem_por_session.get(session_selected)

    if not mensagem:
        logging.debug(f"Nenhuma mensagem disponível para a sessão: {session_selected}")
        return f"Mostrando dados da sessão: {session_selected}", "0 km/h", "0", "-", [], []

    df = montar_dataframe_por_carro(mensagem)
    if df.empty:
        logging.debug(f"DataFrame vazio para a sessão: {session_selected}")
        return f"Mostrando dados da sessão: {session_selected}", "0 km/h", "0", "-", [], []

    # Converter para numérico e tratar NaNs
    for col in ["CarIdxLapCompleted", "CarIdxPosition", "CarIdxLap", "CarIdxLapDistPct", "CarIdxClass"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in ["CarIdxEstTime", "CarIdxF2Time",'CarIdxBestLapTime','CarIdxLapDistPct','CarIdxLastLapTime']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(3)

    for col in ['CarIdxSteer']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(4)
    for col in ['CarIdxRPM']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(0)
    

    # FILTRO: excluir carros com CarIdxLapDistPct == -1 ou CarIdxClass == -1
    df = df[(df["CarIdxLapDistPct"] != -1) & (df["CarIdxClass"] != -1)]

    if df.empty:
        logging.debug(f"DataFrame filtrado vazio após remover carros com -1: {session_selected}")
        return f"Mostrando dados da sessão: {session_selected}", "0 km/h", "0", "-", [], []

    # Mask para carros na pista ou já completaram voltas
    mask_ontrack_or_lapped = pd.Series(False, index=df.index)
    if "CarIdxTrackSurface" in df.columns:
        try:
            mask_ontrack_or_lapped |= df["CarIdxTrackSurface"].isin([0,1,2,3])
        except Exception:
            pass
    if "CarIdxLapCompleted" in df.columns:
        mask_ontrack_or_lapped |= (df["CarIdxLapCompleted"].fillna(0) > 0)
    if "CarIdxPosition" in df.columns:
        mask_ontrack_or_lapped |= df["CarIdxPosition"].notna()

    df_filtrado = df[mask_ontrack_or_lapped].copy()
    if df_filtrado.empty:
        df_filtrado = df.copy()

    # Ordenar para exibir
    if "CarIdxPosition" in df_filtrado.columns and df_filtrado["CarIdxPosition"].notna().any():
        df_sorted = df_filtrado.sort_values(by="CarIdxPosition", ascending=True, na_position='last')
    elif "CarIdxLapDistPct" in df_filtrado.columns and df_filtrado["CarIdxLapDistPct"].notna().any():
        df_sorted = df_filtrado.sort_values(by="CarIdxLapDistPct", ascending=False, na_position='last')
    elif "CarIdx" in df_filtrado.columns:
        df_sorted = df_filtrado.sort_values(by="CarIdx", ascending=True)
    else:
        df_sorted = df_filtrado

    df_sorted = df_sorted.reset_index(drop=True)

    # --- Pegando valores brutos do carro do piloto direto no DF cru ---
    driver_car_idx = mensagem.get("DriverInfo", {}).get("DriverCarIdx", None)
    speed_bruto = rpm_bruto = gear_bruto = None

    if driver_car_idx is not None and "CarIdx" in df.columns:
        df_driver_cru = df[df["CarIdx"] == driver_car_idx]
        if not df_driver_cru.empty:
            # speed_bruto = df_driver_cru.iloc[0].get("CarIdxSpeed", None)
            rpm_bruto = df_driver_cru.iloc[0].get("CarIdxRPM", None)
            gear_bruto = df_driver_cru.iloc[0].get("CarIdxGear", None)
            logging.debug(f"[info] driver encontrado no DF cru: CarIdx={driver_car_idx} | rpm_bruto={rpm_bruto} | gear_bruto={gear_bruto}")

    # Seleciona carro principal no DF ordenado
    carro_principal = None
    if driver_car_idx is not None and "CarIdx" in df_sorted.columns:
        cond = df_sorted["CarIdx"] == driver_car_idx
        carros_do_driver = df_sorted[cond]
        if not carros_do_driver.empty:
            carro_principal = carros_do_driver.iloc[0]

    if carro_principal is None:
        valid_rpm = df_sorted["CarIdxRPM"].apply(lambda x: x is not None and x != -1 and not pd.isna(x)) if "CarIdxRPM" in df_sorted else pd.Series(False, index=df_sorted.index)
        valid_gear = df_sorted["CarIdxGear"].apply(lambda x: x not in [None, -1, np.nan]) if "CarIdxGear" in df_sorted else pd.Series(False, index=df_sorted.index)
        completed_lap = df_sorted["CarIdxLapCompleted"].fillna(0) > 0 if "CarIdxLapCompleted" in df_sorted else pd.Series(False, index=df_sorted.index)

        candidatos = df_sorted[valid_rpm & valid_gear & completed_lap]
        if not candidatos.empty:
            carro_principal = candidatos.iloc[0]
        else:
            candidatos = df_sorted[valid_rpm & valid_gear]
            if not candidatos.empty:
                carro_principal = candidatos.iloc[0]
            else:
                if not df_sorted.empty:
                    carro_principal = df_sorted.iloc[0]

    # Velocidade
    velocidade_str = "0 km/h"
    if mensagem and "Speed" in mensagem:
        speed_ms = mensagem.get("Speed")
        if speed_ms is not None and not pd.isna(speed_ms):
            velocidade_kmh = speed_ms * 3.6
            velocidade_str = f"{velocidade_kmh:.1f} km/h" if velocidade_kmh >= 1 else "0 km/h"

        # RPM
        if carro_principal is not None:
            rpm_val = carro_principal.get("CarIdxRPM", None)
            if (rpm_val is None or rpm_val == -1 or pd.isna(rpm_val)) and rpm_bruto is not None:
                rpm_val = rpm_bruto
    else:
        rpm_val = rpm_bruto
    rpm_str = str(int(rpm_val or 0))

    # Marcha
    if carro_principal is not None:
        marcha_val = carro_principal.get("CarIdxGear", None)
        if (marcha_val in [None, -1, np.nan]) and gear_bruto is not None:
            marcha_val = gear_bruto
    else:
        marcha_val = gear_bruto

    marcha_str = str(marcha_val if marcha_val not in [None, -1, np.nan] else "-")

    def sanitize_df(df_in):
        df_copy = df_in.copy()
        for col in df_copy.columns:
            if df_copy[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df_copy[col] = df_copy[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        return df_copy

    columns = [{"name": c, "id": c} for c in df_sorted.columns]
    df_out = sanitize_df(df_sorted)
    data = df_out.to_dict("records")

    status = f"Mostrando dados da sessão: {session_selected}"
    logging.debug(
        f"Session: {session_selected} | carros exibidos: {len(df_out)} | "
        f"DriverCarIdx: {driver_car_idx} | "
        f"CarIdxLapCompleted(driver): {None if carro_principal is None else carro_principal.get('CarIdxLapCompleted')}"
    )

    return status, velocidade_str, rpm_str, marcha_str, columns, data

if __name__ == '__main__':
    app.run(debug=True)
