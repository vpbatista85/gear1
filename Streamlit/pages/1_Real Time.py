import streamlit as st
import pandas as pd
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

# Função para buscar arquivos com "_Live.parquet" - limitando para 10 arquivos mais recentes
def buscar_arquivos_live(limite=10):
    query = "name contains '_Live.parquet' and mimeType='application/octet-stream'"
    results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name, modifiedTime)', pageSize=50).execute()
    files = results.get('files', [])
    if not files:
        return []
    files.sort(key=lambda x: x['modifiedTime'], reverse=True)
    return files[:limite]

# Função para baixar o arquivo do Drive
def carregar_parquet_drive(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    # Forçar o recarregamento do DataFrame sempre que o arquivo for baixado
    return pd.read_parquet(fh)

st.title("Monitoramento em Tempo Real")

# Intervalo em segundos para atualizar
INTERVALO_ATUALIZACAO = 10

# Buscar arquivos A CADA carga da página para atualizar a lista
arquivos = buscar_arquivos_live()

if arquivos:
    nomes = [f['name'] for f in arquivos]
    # Usar key para preservar a seleção entre atualizações
    nome_arquivo = st.sidebar.selectbox("Selecione a sessão:", nomes, key="sessao_select")
    file_id = next(f['id'] for f in arquivos if f['name'] == nome_arquivo)
else:
    st.sidebar.info("Nenhuma sessão encontrada.")
    nome_arquivo = None
    file_id = None

if file_id:
    st.subheader(f"Sessão ativa: {nome_arquivo}")
    try:
        df = carregar_parquet_drive(file_id)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar o DataFrame: {e}")
else:
    st.info("Nenhuma sessão no momento.")

# Auto refresh usando JS (recarrega a página a cada INTERVALO_ATUALIZACAO segundos)
st.markdown(f"""
    <script>
        setTimeout(() => {{
            window.location.reload();
        }}, {INTERVALO_ATUALIZACAO * 1000});
    </script>
""", unsafe_allow_html=True)
