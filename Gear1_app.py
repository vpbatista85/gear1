import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys

# Se estiver rodando no Windows, importe o pywin32
if sys.platform == "win32":
    import win32com.client


st.set_page_config(
    page_title="Gear 1 HQ",
    page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
    layout="wide",
    initial_sidebar_state="expanded",
    # menu_items={
    #     'Get Help': 'https://www.extremelycoolapp.com/help',
    #     'Report a bug': "https://www.extremelycoolapp.com/bug",
    #     'About': "# This is a header. This is an *extremely* cool app!"
    # }
)
st.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png")


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1moly2-480x79.png",width=198)
with col2:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1chem2-480x79.png",width=198)
with col3:
    st.image("https://gear1.gg/wp-content/uploads/2023/09/CompCableAsset-1@378x-300x280.png",width=78)
with col4:
    st.image("https://gear1.gg/wp-content/uploads/2023/04/Ativo-1forms-480x104.png",width=198)
st.title(":green[Bem-vindo ao Gear One Head Quarter]")

st.write(":green[Utilize o menu à esquerda para navegar entre as páginas.]")