import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io

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

# === Auto refresh via JS ===
def auto_refresh(interval_ms=3000):
    st.markdown(f"""
        <script>
            setTimeout(function(){{
                window.location.reload(1);
            }}, {interval_ms});
        </script>
    """, unsafe_allow_html=True)

# Atualiza a página a cada 3 segundos
auto_refresh(1000)

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
else:
    st.warning("Nenhuma sessão no momento.")
