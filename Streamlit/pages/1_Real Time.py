import streamlit as st
import pandas as pd
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timedelta

# Autenticação com Google Drive
service_account_info = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

@st.cache_resource(show_spinner=False)
def criar_servico_drive():
    return build('drive', 'v3', credentials=creds)

drive_service = criar_servico_drive()

# Função para buscar arquivos com "_Live.parquet"
@st.cache_data(ttl=30)
def buscar_arquivos_live():
    query = "name contains '_Live.parquet' and mimeType='application/octet-stream'"
    results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name, modifiedTime)').execute()
    files = results.get('files', [])
    # Eliminar arquivos com mesmo nome
    arquivos_unicos = {}
    for f in files:
        nome = f["name"]
        if nome not in arquivos_unicos or f["modifiedTime"] > arquivos_unicos[nome]["modifiedTime"]:
            arquivos_unicos[nome] = f
    # Ordenar por data
    return sorted(arquivos_unicos.values(), key=lambda x: x["modifiedTime"], reverse=True)

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

# --- Interface ---
st.sidebar.title("Configuração")
arquivos = buscar_arquivos_live()
opcoes = [f'{f["name"]} - {f["modifiedTime"]}' for f in arquivos]
arquivo_selecionado = st.sidebar.selectbox("Selecione a sessão", opcoes)

st.title("Monitoramento em Tempo Real")

# Encontrar o ID do arquivo selecionado
file_id = None
for f in arquivos:
    nome_opcao = f'{f["name"]} - {f["modifiedTime"]}'
    if nome_opcao == arquivo_selecionado:
        file_id = f["id"]
        break

INTERVALO_ATUALIZACAO = st.sidebar.slider("Intervalo de atualização (seg)", 5, 60, 10)

placeholder = st.empty()

ultima_leitura = None

while True:
    with placeholder.container():
        if file_id:
            try:
                df = carregar_parquet_drive(file_id)
                # Verifica se o conteúdo mudou
                if ultima_leitura is None or not df.equals(ultima_leitura):
                    st.subheader(f"Arquivo: {arquivo_selecionado}")
                    st.dataframe(df, use_container_width=True)
                    ultima_leitura = df
                else:
                    st.info("Nenhuma nova atualização no arquivo.")
            except Exception as e:
                st.error(f"Erro ao carregar o DataFrame: {e}")
        else:
            st.warning("Nenhum arquivo selecionado.")
    time.sleep(INTERVALO_ATUALIZACAO)
