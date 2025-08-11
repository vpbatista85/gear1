# 1_Real Time Unificado com exibição da lógica da 5_Test_RealTime
import streamlit as st
import pandas as pd
import json
import time
from threading import Thread, Lock
from collections import defaultdict
import paho.mqtt.client as mqtt
from streamlit_autorefresh import st_autorefresh

# Configurações do MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "gear1/telemetria"

# Dicionário para armazenar os dados por sessão (client_id)
dados_por_sessao = defaultdict(list)
lock = Lock()

# Configurações do Streamlit
# --- CONFIGURAÇÕES INICIAIS ---
st.logo("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",size="large")
st.set_page_config(page_title="Gear 1 Live Race", page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", layout="wide")
st.title(":green[Análise Ao Vivo]")
# st_autorefresh(interval=5000, key="refresh")  # Atualiza a cada 5 segundos

# Callback MQTT
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        client_id = "sessao_padrao"  # Modifique conforme necessário para múltiplas sessões
        with lock:
            dados_por_sessao[client_id].append(payload)
    except Exception as e:
        print("Erro ao processar mensagem:", e)

# Inicialização do cliente MQTT
def iniciar_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT")
            client.subscribe(MQTT_TOPIC)
        else:
            print("Erro de conexão:", rc)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print("Erro ao conectar ao broker:", e)

# Iniciar MQTT apenas uma vez
if 'mqtt_iniciado' not in st.session_state:
    Thread(target=iniciar_mqtt, daemon=True).start()
    st.session_state['mqtt_iniciado'] = True

# SIDEBAR para seleção de sessões
st.sidebar.title("Sessões")
with lock:
    sessoes_disponiveis = list(dados_por_sessao.keys())

if not sessoes_disponiveis:
    st.sidebar.write("Nenhuma sessão disponível.")
    sessao_selecionada = None
else:
    sessao_selecionada = st.sidebar.selectbox(
        "Selecione a sessão", sessoes_disponiveis, index=0
    )

# Exibição dos dados ao vivo
if sessao_selecionada:
    with lock:
        dados = dados_por_sessao[sessao_selecionada][-200:]

    if dados:
        # Criando o DataFrame transposto acumulado
        if 'df_t' not in st.session_state:
            st.session_state.df_t = pd.DataFrame()

        novo_df = pd.DataFrame.from_dict(dados[-1], orient='index').T
        st.session_state.df_t = pd.concat([novo_df, st.session_state.df_t])

        # Limitar a 100 linhas
        if st.session_state.df_t.shape[0] > 100:
            st.session_state.df_t = st.session_state.df_t.iloc[:100]

        st.subheader(f"Dados em tempo real - Sessão: {sessao_selecionada}")
        st.dataframe(st.session_state.df_t, use_container_width=True)
    else:
        st.info("Aguardando dados da sessão selecionada...")
else:
    st.info("Selecione uma sessão para exibir os dados.")

# import streamlit as st
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from io import BytesIO
# from googleapiclient.http import MediaIoBaseDownload
# from streamlit_autorefresh import st_autorefresh
# import re
# import polars as pl
# import pyarrow.parquet as pq
# import time
# import json
# from google.oauth2 import service_account
# import os
# import json
# import paho.mqtt.client as mqtt
# from collections import defaultdict
# from threading import Thread, Lock



# # Dicionário para armazenar os dados por sessão (client_id)
# dados_por_sessao = defaultdict(list)
# lock = Lock()

# # Configuração do broker MQTT
# MQTT_BROKER = "localhost"  # ou IP de outro PC
# MQTT_PORT = 1883
# MQTT_TOPIC = "gear1/telemetria"

# # --- CONFIGURAÇÕES INICIAIS ---
# st.logo("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",size="large")
# st.set_page_config(page_title="Monitor Live", layout="wide")
# st.set_page_config(page_title="Gear 1 Live Race", page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", layout="wide")
# # st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
# st.title(":green[Análise Ao Vivo]")
# st_autorefresh(interval=5000, key="refresh")  # Atualiza a cada 5 segundos

# if "dados_por_sessao" not in st.session_state:
#     st.session_state.dados_por_sessao = defaultdict(list)


# # --- Função chamada quando nova mensagem chega ---
# def on_message(client, userdata, msg):
#     try:
#         payload = json.loads(msg.payload.decode())
#         print(f"[RECEBIDO MQTT] dados={payload}")
#         client_id = "sessao_padrao"  # Pode ser algo mais dinâmico se preferir
#         with lock:
#             # dados_por_sessao[client_id].append(payload)
#             # st.session_state.dados_por_sessao[client_id].append(payload)
#             dados_por_sessao[client_id].append(payload)
#     except Exception as e:
#         print("Erro ao processar mensagem:", e)

# def iniciar_mqtt():
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Conectado ao broker MQTT com sucesso.")
#             client.subscribe(MQTT_TOPIC)
#             print(f"Inscrito no tópico {MQTT_TOPIC}")
#         else:
#             print("Falha ao conectar. Código de retorno:", rc)

#     client = mqtt.Client()
#     client.on_connect = on_connect
#     client.on_message = on_message

#     try:
#         client.connect(MQTT_BROKER, MQTT_PORT, 60)
#         client.loop_forever()
#     except Exception as e:
#         print(f"Erro ao conectar ao broker MQTT: {e}")

# # --- Inicia o listener MQTT em thread separada ---

# def iniciar_thread_mqtt():
#     thread = Thread(target=iniciar_mqtt, daemon=True)
#     thread.start()
#     return thread

# iniciar_thread_mqtt()


# # # --- SIDEBAR para seleção da sessão ---
# # st.sidebar.title("Sessões")

# # with sessao_lock:
# #     nomes_arquivos = list(sessao_dados.keys())

# # if not nomes_arquivos:
# #     st.sidebar.write("Nenhuma sessão no momento")
# #     arquivo_selecionado = None
# # else:
# #     st.sidebar.write(f"{len(nomes_arquivos)} sessão(ões) ativa(s)")
# #     arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione a sessão...")

# # st.sidebar.markdown("Configuração")

# # --- SIDEBAR ---
# st.sidebar.title("Sessões")
# with lock:
#     sessoes_disponiveis = list(dados_por_sessao.keys())
# st.write("Dados na sessão:", dict(st.session_state.dados_por_sessao))

# if not sessoes_disponiveis:
#     st.sidebar.write("Nenhuma sessão no momento")
#     sessao_selecionada = None
# else:
#     st.sidebar.write(f"{len(sessoes_disponiveis)} sessões ativas")
#     sessao_selecionada = st.sidebar.selectbox("Selecione a sessão", sessoes_disponiveis, index=None, placeholder="Selecione...")

# st.sidebar.markdown("Configuração")


# # # --- Exibição dos dados da sessão selecionada ---
# # if arquivo_selecionado:
# #     with sessao_lock:
# #         dados = sessao_dados.get(arquivo_selecionado, [])
# #         df = pd.DataFrame(dados)

# #     if not df.empty:
# #         st.subheader(f"Dados ao vivo - {arquivo_selecionado}")
# #         st.dataframe(df.tail(10), use_container_width=True)

# #         # Adicione aqui seus gráficos ou KPIs com base no DataFrame df
# #         # Exemplo:
# #         if "Lap" in df.columns and "LapTime" in df.columns:
# #             st.line_chart(df.set_index("Lap")["LapTime"])

# #     else:
# #         st.warning("Aguardando dados da sessão selecionada...")
# # else:
# #     st.info("Selecione uma sessão para visualizar os dados.")

# if sessao_selecionada:
#     with lock:
#         dados = dados_por_sessao[sessao_selecionada][-200:]  # últimos dados
#         # dados = st.session_state.dados_por_sessao[sessao_selecionada][-200:]
#     st.write(f"DEBUG: Recebidos {len(dados)} registros para a sessão {sessao_selecionada}")

#     if dados:
#         df = pd.DataFrame(dados)

#         st.subheader(f"Sessão: {sessao_selecionada}")
#         st.write(f"Total de registros recebidos: {len(df)}")

#         # Exibe métricas básicas (modifique conforme suas colunas)
#         if "Lap" in df.columns and "Speed" in df.columns:
#             st.metric("Volta Atual", int(df["Lap"].iloc[-1]))
#             st.metric("Velocidade Atual (km/h)", round(df["Speed"].iloc[-1], 1))

#         # Gráfico de velocidade (exemplo)
#         if "Speed" in df.columns:
#             st.line_chart(df["Speed"], height=200)

#         # Exibir tabela opcional
#         with st.expander("Ver dados brutos"):
#             st.dataframe(df.tail(20))
#     else:
#         st.info("Aguardando dados para esta sessão selecionada...")
# else:
#     st.info("Nenhuma sessão selecionada.")


# # # --- AUTENTICAÇÃO COM GOOGLE DRIVE ---
# # # service_account_info = st.secrets["google_service_account"]

# # # Caminho absoluto do arquivo gear1_cred.json
# # current_dir = os.path.dirname(__file__)
# # json_path = os.path.join(current_dir, "gear1_cred.json")

# # with open(json_path) as source:
# #     service_account_info = json.load(source)

# # credentials = service_account.Credentials.from_service_account_info(service_account_info)

# # def criar_servico_drive():
# #     return build('drive', 'v3', credentials=credentials)

# # drive_service = criar_servico_drive()

# # # --- LER LISTA DE ARQUIVOS (SEM CACHE) ---
# # def buscar_arquivos_live():
# #     FOLDER_ID = "1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ"  # ID da pasta 'Parquets_live'

# #     query = (
# #         f"'{FOLDER_ID}' in parents and "
# #         "mimeType='application/octet-stream' and "
# #         "trashed = false"
# #     )
# # #     query = (
# # #     f"'{FOLDER_ID}' in parents and "
# # #     "trashed = false"
# # # )
# #     results = drive_service.files().list(
# #         q=query,
# #         spaces='drive',
# #         fields='files(id, name, modifiedTime)'
# #     ).execute()

# #     # st.write("Arquivos encontrados na pasta:", results.get("files", []))

# #     files = results.get('files', [])

# #     # Aplica regex para manter apenas arquivos com _Live.parquet no nome
# #     padrao_regex = r"^(?!.*_\d{8}T\d{6})[A-Za-z0-9 _\-]+_Live\.parquet$"

# #     arquivos_filtrados = [
# #         file for file in files if re.match(padrao_regex, file["name"])
# #     ]

# #     arquivos_ordenados = sorted(
# #         arquivos_filtrados,
# #         key=lambda x: x["modifiedTime"],
# #         reverse=True,
# #     )

# #     return arquivos_ordenados


# # # --- CARREGAR O PARQUET PELO ID ---
# # def carregar_parquet_drive(file_id):
# #     request = drive_service.files().get_media(fileId=file_id)
# #     fh = BytesIO()
# #     downloader = MediaIoBaseDownload(fh, request)
# #     done = False
# #     while not done:
# #         status, done = downloader.next_chunk()
# #     fh.seek(0)

# #     try:
# #         return pd.read_parquet(fh)
# #     except Exception as e:
# #         st.warning("Erro ao carregar completo, tentando carregar colunas parciais...")
# #         import pyarrow.parquet as pq
# #         fh.seek(0)
# #         parquet_file = pq.ParquetFile(fh)
# #         schema = parquet_file.schema
# #         columns_ok = [name for name in schema.names if schema.field(name).type.__class__.__name__ not in ['StructType', 'ListType']]
# #         fh.seek(0)
# #         return pq.read_table(fh, columns=columns_ok).to_pandas()
    
# # def carregar_parquet_drive_polars(file_id, drive_service):
# #     try:
# #         request = drive_service.files().get_media(fileId=file_id)
# #         fh = BytesIO()
# #         downloader = MediaIoBaseDownload(fh, request)
# #         done = False
# #         while not done:
# #             status, done = downloader.next_chunk()
# #         fh.seek(0)

# #         # # Usa PyArrow só para pegar as colunas simples
# #         # import pyarrow.parquet as pq
# #         # parquet_file = pq.ParquetFile(fh)
# #         # schema = parquet_file.schema
# #         # colunas_ok = [name for name in schema.names if schema.field(name).type.__class__.__name__ not in ['StructType', 'ListType']]
        
# #         # # Carrega apenas colunas simples como LazyFrame
# #         # fh.seek(0)
# #         # lf = pl.read_parquet(fh, columns=colunas_ok).lazy()
# #         lf = pl.read_parquet(fh).lazy()
# #         return lf

# #     except Exception as e:
# #         st.error(f"Erro ao carregar o DataFrame: {e}")
# #         return None
    
# # def preparar_lazyframe_para_analise(
# #     lf: pl.LazyFrame, 
# #     colunas_explodir: list[str], 
# #     colunas_manter: list[str]
# # ) -> pl.DataFrame:
# #     try:
# #         print("Ordenando e limitando linhas...")
# #         lf_reduzido = lf.sort("timestamp", descending=True).limit(2)

# #         print("Coletando schema...")
# #         schema = lf_reduzido.schema
# #         print("Schema coletado com sucesso.")

# #         # Explodir apenas as colunas que existem no schema
# #         for coluna in colunas_explodir:
# #             if coluna in schema:
# #                 print(f"Explodindo coluna: {coluna}")
# #                 lf_reduzido = lf_reduzido.explode(coluna)
# #             else:
# #                 print(f"Coluna {coluna} não está presente no schema, pulando...")

# #         # Selecionar apenas as colunas desejadas (que existirem no schema)
# #         colunas_validas = [col for col in colunas_manter if col in lf_reduzido.schema]
# #         print(f"Selecionando colunas: {colunas_validas}")
# #         lf_reduzido = lf_reduzido.select(colunas_validas)

# #         print("Coletando DataFrame final...")
# #         df_final = lf_reduzido.collect()
# #         print("DataFrame carregado com sucesso.")
# #         return df_final

# #     except Exception as e:
# #         print(f"Erro ao preparar LazyFrame: {e}")
# #         raise
    
# # # --- SIDEBAR ---

# # st.sidebar.title("Sessões")
# # arquivos = buscar_arquivos_live()
# # nomes_arquivos = [f["name"] for f in arquivos]

# # if len(arquivos)==0:
# #     st.sidebar.write(f'Nenhuma sessão no momento')
# # elif len(arquivos)==1:
# #     st.sidebar.write(f'1 sessão ativa')
# #     arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")
# # else:
# #     st.sidebar.write(f'{len(arquivos)} sessões ativas')
# #     arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")
# # st.sidebar.markdown("Configuração")



# # INTERVALO_ATUALIZACAO = st.sidebar.slider("Intervalo de atualização (segundos)", 3, 20, 6)
# # # Converte para milissegundos
# # refreshed = st_autorefresh(interval=INTERVALO_ATUALIZACAO * 1000, key="refresh")



# # # arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")

# # # --- ENCONTRAR ID PELO NOME ---
# # file_id = next((f["id"] for f in arquivos if f["name"] == arquivo_selecionado), None)

# # # # --- CARREGAR E EXIBIR ---
# # # if file_id:
# # #     try:
# # #         df = carregar_parquet_drive(file_id)
# # #         # Ordena pelo valor mais recente da primeira coluna
# # #         df = df.sort_values(by=df.columns[0], ascending=False)
# # #         # st.subheader(f"Arquivo: {arquivo_selecionado}")
# # #         st.dataframe(df, use_container_width=True)
# # #     except Exception as e:
# # #         st.error(f"Erro ao carregar o DataFrame: {e}")
# # # else:
# # #     st.warning("Nenhum arquivo selecionado.")

# # # --- CARREGAR E EXIBIR ---
# # if file_id:
# #     try:
# #         # # Carrega diretamente como Polars LazyFrame
# #         # lf = carregar_parquet_drive_polars(file_id, drive_service)
# #         # if lf is None:
# #         #     st.error("LazyFrame não pôde ser carregado.")
# #         #     st.stop()
# #         colunas_explodir = [
# #              "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime",
# #             #  "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time",
# #             #  "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
# #             #  "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
# #             #  "CarIdxTireCompound"
# #         ]
        
# #         colunas_manter = [
# #              "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime",
# #             #  "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time",
# #             #  "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
# #             #  "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
# #             #  "CarIdxTireCompound"
# #         ]
    

# #         # st.write("LazyFrame carregado. Coletando schema...")
# #         with st.spinner("Carregando arquivo do Google Drive..."):
# #             lf = carregar_parquet_drive_polars(file_id, drive_service)

# #             if lf is not None:
                
# #                 df = preparar_lazyframe_para_analise(lf, colunas_explodir, colunas_manter)

# #                 if df is not None and not df.is_empty():
# #                     st.success("Arquivo carregado com sucesso!")
# #                     st.dataframe(df.head(64))  # Ou qualquer outro processamento com `df`
# #                     # st.dataframe(df)
# #                 else:
# #                     st.warning("DataFrame vazio ou erro durante preparação.")
# #             else:
# #                 st.error("Erro ao carregar LazyFrame do Parquet.")

# #         # Define colunas que serão explodidas se existirem
# #         # colunas_lista = [
# #         #     "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime", "CarIdxClass",
# #         #     "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time", "CarIdxFastRepairsUsed",
# #         #     "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
# #         #     "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxP2P_Count", "CarIdxP2P_Status",
# #         #     "CarIdxPaceFlags", "CarIdxPaceLine", "CarIdxPaceRow", "CarIdxQualTireCompound",
# #         #     "CarIdxQualTireCompoundLocked", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
# #         #     "CarIdxTireCompound", "CarIdxTrackSurface", "CarIdxTrackSurfaceMaterial"
# #         # ]

# #         # colunas_lista = [
# #         #     "CarIdxPosition", "CarIdxBestLapNum"
# #         # ]

# #         # # Aplica o pipeline otimizado
# #         # df_pl = preparar_lazyframe_para_analise(lf, colunas_lista)

# #         # if df_pl is None:
# #         #     st.error("Falha ao preparar o DataFrame.")
# #         #     st.stop()

# #         # # Converte para pandas para exibir no Streamlit
# #         # df_final = df_pl.to_pandas()
# #         # st.write("DataFrame final coletado:")
# #         # st.dataframe(df_final, use_container_width=True)

# #     except Exception as e:
# #         st.error(f"Erro ao carregar o DataFrame: {e}")
# # else:
# #     st.warning("Nenhum arquivo selecionado.")
