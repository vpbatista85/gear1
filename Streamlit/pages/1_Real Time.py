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

# Função para buscar o arquivo mais recente com "_Live.parquet"
def buscar_arquivo_live():
    query = "name contains '_Live.parquet' and mimeType='application/octet-stream'"
    results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name, modifiedTime)').execute()
    files = results.get('files', [])
    if not files:
        return None, None
    # Ordenar pelo último modificado
    files.sort(key=lambda x: x['modifiedTime'], reverse=True)
    return files[0]['id'], files[0]['name']

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

# Atualiza a cada 2 segundos
INTERVALO_ATUALIZACAO = 2

while True:
    with placeholder.container():
        file_id, nome_arquivo = buscar_arquivo_live()
        if file_id:
            st.subheader(f"Sessão ativa: {nome_arquivo}")
            try:
                df = carregar_parquet_drive(file_id)
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Erro ao carregar o DataFrame: {e}")
        else:
            st.info("Nenhuma sessão no momento.")
    time.sleep(INTERVALO_ATUALIZACAO)
