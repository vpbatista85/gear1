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

# --- CONFIGURAÇÕES INICIAIS ---
st.logo("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",size="large")
st.set_page_config(page_title="Monitor Live", layout="wide")
st.set_page_config(page_title="Gear 1 Post Race", page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", layout="wide")
# st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
st.title(":green[Análise Ao Vivo]")

# --- AUTENTICAÇÃO COM GOOGLE DRIVE ---
service_account_info = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

def criar_servico_drive():
    return build('drive', 'v3', credentials=creds)

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
# --- CARREGAR E EXIBIR ---
if file_id:
    try:
        # Carrega diretamente como Polars LazyFrame
        lf = carregar_parquet_drive_polars(file_id, drive_service)  # ✅ Agora tem o drive_service
        if lf is None:
            st.error("LazyFrame não pôde ser carregado.")
            st.stop()
        st.write("LazyFrame carregado. Coletando schema...")

        colunas_lista = [
            "CarIdxPosition", "CarIdxBestLapNum", "CarIdxBestLapTime", "CarIdxClass",
            "CarIdxClassPosition", "CarIdxEstTime", "CarIdxF2Time", "CarIdxFastRepairsUsed",
            "CarIdxGear", "CarIdxLap", "CarIdxLapCompleted", "CarIdxLapDistPct",
            "CarIdxLastLapTime", "CarIdxOnPitRoad", "CarIdxP2P_Count", "CarIdxP2P_Status",
            "CarIdxPaceFlags", "CarIdxPaceLine", "CarIdxPaceRow", "CarIdxQualTireCompound",
            "CarIdxQualTireCompoundLocked", "CarIdxRPM", "CarIdxSessionFlags", "CarIdxSteer",
            "CarIdxTireCompound", "CarIdxTrackSurface", "CarIdxTrackSurfaceMaterial"
        ]

        # Coleta o schema do LazyFrame de forma segura
        schema = lf.collect_schema()
        st.write("Schema coletado com sucesso.")

        # Explode as colunas que forem listas
        for col in colunas_lista:
            if col in schema and isinstance(schema[col], pl.List):
                st.write(f"Explodindo coluna: {col}")
                lf = lf.explode(col)

        # Executa o LazyFrame (materializa em DataFrame)
        colunas_utilizadas = ['CarIdxPosition', 'CarIdxLap', 'CarIdxPosition', 'CarIdxLastLapTime']  # exemplo
        lf = lf.select(colunas_utilizadas)
        st.write("Coletando DataFrame final...")

        df_pl = lf.limit(30).collect()

        st.write("DataFrame coletado!")

        # Ordena pela primeira coluna (timestamp, por exemplo), se necessário
        df_pl = df_pl.sort(df_pl.columns[0], descending=True)

        # Converte para pandas para exibir no Streamlit
        df_final = df_pl.to_pandas()
        st.dataframe(df_final, use_container_width=True)

    except Exception as e:
        st.error(f"Erro ao carregar o DataFrame: {e}")
else:
    st.warning("Nenhum arquivo selecionado.")
