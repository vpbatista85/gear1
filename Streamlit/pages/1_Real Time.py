import streamlit as st
import pandas as pd
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
from googleapiclient.http import MediaIoBaseDownload

# Autenticação com Google Drive
service_account_info = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

@st.cache_resource(show_spinner=False)
def criar_servico_drive():
    return build('drive', 'v3', credentials=creds)

drive_service = criar_servico_drive()

# Função para listar todos os arquivos com "_Live.parquet"
def listar_arquivos_live():
    query = "name contains '_Live.parquet' and mimeType='application/octet-stream' and trashed = false"
    results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name, modifiedTime)').execute()
    files = results.get('files', [])
    # Ordenar pelo último modificado
    files.sort(key=lambda x: x['modifiedTime'], reverse=True)
    return files

# Função para baixar o arquivo do Drive
def carregar_parquet_drive(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_parquet(fh)

st.title("Monitoramento em Tempo Real")

placeholder = st.empty()

# Atualiza a cada 10 segundos
INTERVALO_ATUALIZACAO = 10

arquivos = listar_arquivos_live()

if not arquivos:
    st.info("Nenhuma sessão no momento.")
    st.stop()

nomes = [f['name'] for f in arquivos]

# Mantém a escolha do arquivo entre atualizações
if 'arquivo_selecionado' not in st.session_state:
    st.session_state.arquivo_selecionado = nomes[0]

# Coloca o selectbox na sidebar
arquivo_escolhido = st.sidebar.selectbox(
    "Selecione a sessão:",
    nomes,
    index=nomes.index(st.session_state.arquivo_selecionado) if st.session_state.arquivo_selecionado in nomes else 0
)

st.session_state.arquivo_selecionado = arquivo_escolhido

# Loop de atualização manual porque Streamlit não gosta de while True em scripts normais.
# Em vez disso, vamos usar o st_autorefresh ou meta refresh.
# Mas para manter parecido com o seu, podemos usar um loop simulado via st_autorefresh.

# Importa a função para autorefresh
from streamlit_extras.st_autorefresh import st_autorefresh

# Atualiza a página a cada INTERVALO_ATUALIZACAO segundos
st_autorefresh(interval=INTERVALO_ATUALIZACAO * 1000, key="auto_refresh")

# Busca o file_id do arquivo selecionado
file_id = next((f['id'] for f in arquivos if f['name'] == arquivo_escolhido), None)

with placeholder.container():
    st.subheader(f"Sessão ativa: {arquivo_escolhido}")
    try:
        df = carregar_parquet_drive(file_id)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar o DataFrame: {e}")
