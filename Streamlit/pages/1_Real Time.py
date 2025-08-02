import streamlit as st
import pandas as pd
import os
import time

# Atualização automática da página a cada 5 segundos
st.markdown("""
    <meta http-equiv="refresh" content="5">
""", unsafe_allow_html=True)

# Função para ler os arquivos do Google Drive montado
def listar_arquivos_pasta(caminho):
    return [f for f in os.listdir(caminho) if f.endswith(".parquet")]

# Caminho onde os arquivos .parquet estão armazenados no Google Drive
caminho_arquivos = "/mount/drive/MyDrive/seu_diretorio"

# Lista de arquivos
nomes = listar_arquivos_pasta(caminho_arquivos)

# Verifica se há arquivos
if not nomes:
    st.warning("Nenhum arquivo encontrado no Google Drive.")
    st.stop()

# Inicializa o session_state com o nome do primeiro arquivo
if "nome_arquivo" not in st.session_state:
    st.session_state.nome_arquivo = nomes[0]

# Caixa de seleção na barra lateral
nome_arquivo = st.sidebar.selectbox(
    "Selecione uma sessão:",
    nomes,
    index=nomes.index(st.session_state.nome_arquivo) if st.session_state.nome_arquivo in nomes else 0
)
st.session_state.nome_arquivo = nome_arquivo

# Lê o arquivo selecionado
caminho_completo = os.path.join(caminho_arquivos, nome_arquivo)
df = pd.read_parquet(caminho_completo)

# Exibe o DataFrame
st.write(f"### Dados da sessão: `{nome_arquivo}`")
st.dataframe(df)
