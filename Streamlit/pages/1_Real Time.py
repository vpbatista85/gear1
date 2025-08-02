import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io
import time
from streamlit_extras.st_autorefresh import st_autorefresh

# === Configurações ===
SERVICE_ACCOUNT_FILE = 'C:/Users/vpb85/Documents/Gear1/gear1-ir-36de8419de96.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_FOLDER_ID = '1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ'

# === Autenticação ===
@st.cache_resource(show_spinner=False)
def criar_servico_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

drive_service = criar_servico_drive()

# === Buscar arquivos com final _Live.parquet ===
def listar_arquivos_parquet():
    query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '_Live.parquet' and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id, name)", pageSize=20).execute()
    return response.get('files', [])

arquivos = listar_arquivos_parquet()

st.title("Monitoramento de Sessão")

if arquivos:
    nomes = [arq['name'] for arq in arquivos]
    nome_arquivo = st.selectbox("Selecione uma sessão:", nomes)
    arquivo_id = next((arq['id'] for arq in arquivos if arq['name'] == nome_arquivo), None)

    if arquivo_id:
        # Fazer download do arquivo selecionado
        request = drive_service.files().get_media(fileId=arquivo_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        buffer.seek(0)
        df = pd.read_parquet(buffer)

        st.success(f"Exibindo dados de: {nome_arquivo}")
        st.dataframe(df.tail(10))  # Exibe as 10 últimas linhas

        # # Atualiza a cada 3 segundos
        # st.experimental_rerun()  # Essa chamada deve ser controlada para não gerar looping infinito
        # Atualiza a cada 3 segundos (3000 milissegundos)
        st_autorefresh(interval=1000, limit=None, key="auto-refresh")

else:
    st.warning("Nenhuma sessão no momento.")
