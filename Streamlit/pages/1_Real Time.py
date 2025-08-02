import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io
import time

# === Configurações ===
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_FOLDER_ID = '1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ'

# === Autenticação ===
service_account_info = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

# === Criação do serviço (sem cache)
def criar_servico_drive():
    return build('drive', 'v3', credentials=creds)

drive_service = criar_servico_drive()

# === Buscar arquivos com final _Live.parquet ===
def listar_arquivos_parquet():
    query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '_Live.parquet' and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id, name)", pageSize=20).execute()
    return response.get('files', [])

st.title("Monitoramento de Sessão")

arquivos = listar_arquivos_parquet()

if arquivos:
    nomes = [arq['name'] for arq in arquivos]
    nome_arquivo = st.selectbox("Selecione uma sessão:", nomes)
    arquivo_id = next((arq['id'] for arq in arquivos if arq['name'] == nome_arquivo), None)

    if arquivo_id:
        # Loop com autoatualização
        contagem = st.empty()
        data_placeholder = st.empty()

        while True:
            # Fazer download do arquivo selecionado
            request = drive_service.files().get_media(fileId=arquivo_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            buffer.seek(0)
            df = pd.read_parquet(buffer)

            contagem.markdown(f"Última atualização: `{pd.Timestamp.now().strftime('%H:%M:%S')}`")
            data_placeholder.dataframe(df.tail(10))

            time.sleep(3)  # atualiza a cada 3 segundos
            st.experimental_rerun()
else:
    st.warning("Nenhuma sessão no momento.")
