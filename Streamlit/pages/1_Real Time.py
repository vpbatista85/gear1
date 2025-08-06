import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
from googleapiclient.http import MediaIoBaseDownload
from streamlit_autorefresh import st_autorefresh
import re
import polars as pl
import pyarrow.parquet as pq
import time
import json
from google.oauth2 import service_account
import os

# --- CONFIGURAÇÕES INICIAIS ---
st.logo("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",size="large")
st.set_page_config(page_title="Monitor Live", layout="wide")
st.set_page_config(page_title="Gear 1 Live Race", page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", layout="wide")
# st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
st.title(":green[Análise Ao Vivo]")

# --- AUTENTICAÇÃO COM GOOGLE DRIVE ---
# service_account_info = st.secrets["google_service_account"]

# Caminho absoluto do arquivo gear1_cred.json
current_dir = os.path.dirname(__file__)
json_path = os.path.join(current_dir, "gear1_cred.json")

with open(json_path) as source:
    service_account_info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(service_account_info)

def criar_servico_drive():
    return build('drive', 'v3', credentials=credentials)

drive_service = criar_servico_drive()

# --- LER LISTA DE ARQUIVOS (SEM CACHE) ---
def buscar_arquivos_live():
    FOLDER_ID = "1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ"  # ID da pasta 'Parquets_live'

    query = (
        f"'{FOLDER_ID}' in parents and "
        "mimeType='application/octet-stream' and "
        "trashed = false"
    )
#     query = (
#     f"'{FOLDER_ID}' in parents and "
#     "trashed = false"
# )
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name, modifiedTime)'
    ).execute()

    # st.write("Arquivos encontrados na pasta:", results.get("files", []))

    files = results.get('files', [])

    # Aplica regex para manter apenas arquivos com _Live.parquet no nome
    padrao_regex = r"^(?!.*_\d{8}T\d{6})[A-Za-z0-9 _\-]+_Live\.parquet$"

    arquivos_filtrados = [
        file for file in files if re.match(padrao_regex, file["name"])
    ]

    arquivos_ordenados = sorted(
        arquivos_filtrados,
        key=lambda x: x["modifiedTime"],
        reverse=True,
    )

    return arquivos_ordenados


# --- CARREGAR O PARQUET PELO ID ---
def carregar_parquet_drive(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)

    try:
        return pd.read_parquet(fh)
    except Exception as e:
        st.warning("Erro ao carregar completo, tentando carregar colunas parciais...")
        import pyarrow.parquet as pq
        fh.seek(0)
        parquet_file = pq.ParquetFile(fh)
        schema = parquet_file.schema
        columns_ok = [name for name in schema.names if schema.field(name).type.__class__.__name__ not in ['StructType', 'ListType']]
        fh.seek(0)
        return pq.read_table(fh, columns=columns_ok).to_pandas()
    
def carregar_parquet_drive_polars(file_id, drive_service):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        # # Usa PyArrow só para pegar as colunas simples
        # import pyarrow.parquet as pq
        # parquet_file = pq.ParquetFile(fh)
        # schema = parquet_file.schema
        # colunas_ok = [name for name in schema.names if schema.field(name).type.__class__.__name__ not in ['StructType', 'ListType']]
        
        # # Carrega apenas colunas simples como LazyFrame
        # fh.seek(0)
        # lf = pl.read_parquet(fh, columns=colunas_ok).lazy()
        lf = pl.read_parquet(fh).lazy()
        return lf

    except Exception as e:
        st.error(f"Erro ao carregar o DataFrame: {e}")
        return None
    
def preparar_lazyframe_para_analise(
    lf: pl.LazyFrame, 
    colunas_explodir: list[str], 
    colunas_manter: list[str]
) -> pl.DataFrame:
    try:
        print("Ordenando e limitando linhas...")
        lf_reduzido = lf.sort("timestamp", descending=True).limit(2)

        print("Coletando schema...")
        schema = lf_reduzido.schema
        print("Schema coletado com sucesso.")

        # Explodir apenas as colunas que existem no schema
        for coluna in colunas_explodir:
            if coluna in schema:
                print(f"Explodindo coluna: {coluna}")
                lf_reduzido = lf_reduzido.explode(coluna)
            else:
                print(f"Coluna {coluna} não está presente no schema, pulando...")

        # Selecionar apenas as colunas desejadas (que existirem no schema)
        colunas_validas = [col for col in colunas_manter if col in lf_reduzido.schema]
        print(f"Selecionando colunas: {colunas_validas}")
        lf_reduzido = lf_reduzido.select(colunas_validas)

        print("Coletando DataFrame final...")
        df_final = lf_reduzido.collect()
        print("DataFrame carregado com sucesso.")
        return df_final

    except Exception as e:
        print(f"Erro ao preparar LazyFrame: {e}")
        raise
    
# --- SIDEBAR ---

st.sidebar.title("Sessões")
arquivos = buscar_arquivos_live()
nomes_arquivos = [f["name"] for f in arquivos]

if len(arquivos)==0:
    st.sidebar.write(f'Nenhuma sessão no momento')
elif len(arquivos)==1:
    st.sidebar.write(f'1 sessão ativa')
    arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")
else:
    st.sidebar.write(f'{len(arquivos)} sessões ativas')
    arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")
st.sidebar.markdown("Configuração")



INTERVALO_ATUALIZACAO = st.sidebar.slider("Intervalo de atualização (segundos)", 3, 20, 6)
# Converte para milissegundos
refreshed = st_autorefresh(interval=INTERVALO_ATUALIZACAO * 1000, key="refresh")



# arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", nomes_arquivos, index=None, placeholder="Selecione o arquivo...")

# --- ENCONTRAR ID PELO NOME ---
file_id = next((f["id"] for f in arquivos if f["name"] == arquivo_selecionado), None)

# # --- CARREGAR E EXIBIR ---
# if file_id:
#     try:
#         df = carregar_parquet_drive(file_id)
#         # Ordena pelo valor mais recente da primeira coluna
#         df = df.sort_values(by=df.columns[0], ascending=False)
#         # st.subheader(f"Arquivo: {arquivo_selecionado}")
#         st.dataframe(df, use_container_width=True)
#     except Exception as e:
#         st.error(f"Erro ao carregar o DataFrame: {e}")
# else:
#     st.warning("Nenhum arquivo selecionado.")

# --- CARREGAR E EXIBIR ---
if file_id:
    try:
        # # Carrega diretamente como Polars LazyFrame
        # lf = carregar_parquet_drive_polars(file_id, drive_service)
        # if lf is None:
        #     st.error("LazyFrame não pôde ser carregado.")
        #     st.stop()
        colunas_explodir = [
             "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime",
            #  "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time",
            #  "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
            #  "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
            #  "CarIdxTireCompound"
        ]
        
        colunas_manter = [
             "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime",
            #  "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time",
            #  "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
            #  "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
            #  "CarIdxTireCompound"
        ]
    

        # st.write("LazyFrame carregado. Coletando schema...")
        with st.spinner("Carregando arquivo do Google Drive..."):
            lf = carregar_parquet_drive_polars(file_id, drive_service)

            if lf is not None:
                
                df = preparar_lazyframe_para_analise(lf, colunas_explodir, colunas_manter)

                if df is not None and not df.is_empty():
                    st.success("Arquivo carregado com sucesso!")
                    st.dataframe(df.head(64))  # Ou qualquer outro processamento com `df`
                    # st.dataframe(df)
                else:
                    st.warning("DataFrame vazio ou erro durante preparação.")
            else:
                st.error("Erro ao carregar LazyFrame do Parquet.")

        # Define colunas que serão explodidas se existirem
        # colunas_lista = [
        #     "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime", "CarIdxClass",
        #     "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time", "CarIdxFastRepairsUsed",
        #     "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
        #     "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxP2P_Count", "CarIdxP2P_Status",
        #     "CarIdxPaceFlags", "CarIdxPaceLine", "CarIdxPaceRow", "CarIdxQualTireCompound",
        #     "CarIdxQualTireCompoundLocked", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
        #     "CarIdxTireCompound", "CarIdxTrackSurface", "CarIdxTrackSurfaceMaterial"
        # ]

        # colunas_lista = [
        #     "CarIdxPosition", "CarIdxBestLapNum"
        # ]

        # # Aplica o pipeline otimizado
        # df_pl = preparar_lazyframe_para_analise(lf, colunas_lista)

        # if df_pl is None:
        #     st.error("Falha ao preparar o DataFrame.")
        #     st.stop()

        # # Converte para pandas para exibir no Streamlit
        # df_final = df_pl.to_pandas()
        # st.write("DataFrame final coletado:")
        # st.dataframe(df_final, use_container_width=True)

    except Exception as e:
        st.error(f"Erro ao carregar o DataFrame: {e}")
else:
    st.warning("Nenhum arquivo selecionado.")
