import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io

# === Configurações ===
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_FOLDER_ID = '1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ'
REFRESH_INTERVAL_MS = 3000  # 3 segundos

# === Autenticação ===
service_account_info = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

@st.cache_resource(show_spinner=False)
def criar_servico_drive():
    return build('drive', 'v3', credentials=creds)

drive_service = criar_servico_drive()

# === Buscar arquivos com final _Live.parquet ===
def listar_arquivos_parquet():
    query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '_Live.parquet' and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id, name)", pageSize=20).execute()
    return response.get('files', [])

arquivos = listar_arquivos_parquet()

# === Auto refresh ===
def auto_refresh(interval_ms=3000):
    st.markdown(f"""
        <script>
            setTimeout(function(){{
                window.location.reload(1);
            }}, {interval_ms});
        </script>
    """, unsafe_allow_html=True)

st.title("Monitoramento de Sessão")

# === Lógica principal ===
if arquivos:
    nomes = [arq['name'] for arq in arquivos]

    # Preserva seleção entre refreshes
    if "nome_arquivo" not in st.session_state:
        st.session_state.nome_arquivo = nomes[0]

    st.session_state.nome_arquivo = st.selectbox("Selecione uma sessão:", nomes, index=nomes.index(st.session_state.nome_arquivo))

    # Obtem ID do arquivo selecionado
    arquivo_id = next((arq['id'] for arq in arquivos if arq['name'] == st.session_state.nome_arquivo), None)

    if arquivo_id:
        try:
            # Fazer download do arquivo selecionado
            request = drive_service.files().get_media(fileId=arquivo_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            buffer.seek(0)
            df = pd.read_parquet(buffer)

            st.success(f"Exibindo dados de: {st.session_state.nome_arquivo}")
            st.dataframe(df.tail(10))

        except Exception as e:
            st.error(f"Erro ao carregar o arquivo: {e}")
else:
    st.warning("Nenhuma sessão no momento.")

# === Ativa o auto-refresh SEMPRE ===
auto_refresh(REFRESH_INTERVAL_MS)
