import os
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pycountry_convert as pc
from iracingdataapi.client import irDataClient
from gender_guesser.detector import Detector
from datetime import datetime
import hashlib
import base64
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import streamlit as st
import logging
import unicodedata
import utils
import seg
from utils import encode_pw, remover_acentos, converter_data, verificar_json_existente, obter_continente, determinar_sexo, obter_paises, obter_pilotos, calcular_idade, obter_dados_piloto, verificar_iracing, driver_df, simplificar_categoria





# Configuração da API do iRacing (substitua pelas suas credenciais)
# IRACING_USERNAME = "seu_usuario"
# IRACING_PASSWORD = "sua_senha"

st.set_page_config(
page_title="Gear 1 Drivers on Iracing",
page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
layout="wide",
initial_sidebar_state="expanded",
# menu_items={
#     'Get Help': 'https://www.extremelycoolapp.com/help',
#     'Report a bug': "https://www.extremelycoolapp.com/bug",
#     'About': "# This is a header. This is an *extremely* cool app!"
# }
)   
st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

    # 🎛️ Criando filtros na sidebar
st.sidebar.title("Filtros")




username=seg.username
password=seg.password


idc=irDataClient(username=username,password=password)



pwValueToSubmit = encode_pw(username, password)

# print(f'{username}\n{pwValueToSubmit}')

r = requests.post('https://members-ng.iracing.com/auth', auth=(username, pwValueToSubmit))


# # Inicializa cliente iRacing
# iracing = irDataClient(username=IRACING_USERNAME, password=IRACING_PASSWORD)
idc=irDataClient(username=username,password=password)


# Nome do arquivo JSON
JSON_FILE = "pilotos_por_pais.json"
JSON_FILE_P = "pilotos_por_pais_parcial.json"
DF_File='pilotos_dataframe.json'

# Função principal
def main():

    # st.write("✅ Script carregado!")
    if os.path.exists(DF_File):
        pass
    else:
        if verificar_json_existente(JSON_FILE)==True:
            print("Carregando dados do JSON existente...")
            with open(JSON_FILE, "r", encoding="utf-8") as file:
                dados_finais = json.load(file)  # Carrega os dados existentes
        else:
            print("Criando novo JSON...")
            # dados = obter_dados()  # Substitua pela função que gera novos dados
            with open(JSON_FILE, "w") as file:
                json.dump(dados, file, indent=4)
            # if verificar_json_parcial_existente():
            #     return
                
            # Tenta carregar progresso existente
            if os.path.exists(JSON_FILE_P):
                with open(JSON_FILE_P, "r", encoding="utf-8") as file:
                    progresso = json.load(file)
            else:
                progresso = {}

            dados_finais = progresso  # Evita sobrescrever dados já salvos

            # Obtém a lista de países
            paises = obter_paises()

            for pais in paises:
                if pais in progresso.keys() and pais != list(progresso.keys())[-1]:
                    print(f"📌 País {pais} já processado, pulando...")
                    continue  # Pula para o próximo país

                print(f"🌍 Processando {pais}...")
                continente = obter_continente(pais)
                print(f"  ↳ Continente: {continente}")

                pilotos = obter_pilotos(pais)

                dados_finais[pais] = {"Continente": continente, "Pilotos": []}

                for piloto in pilotos:
                    print(f"  🚗 Obtendo dados de {piloto['Nome']}...")

                    categorias, nascimento, obito, idade, link = obter_dados_piloto(piloto)
                    sexo = determinar_sexo(piloto)
                    contas_iracing = verificar_iracing(piloto)

                    dados_finais[pais]["Pilotos"].append({
                        "Nome": piloto["Nome"],
                        "Categorias": categorias,
                        "Sexo": sexo,
                        "Data de Nascimento": nascimento,
                        "Data de Óbito": obito,
                        "Idade": idade,
                        "Link": link,
                        "iRacing": contas_iracing
                    })

                # Salva o progresso após cada país
                with open(JSON_FILE_P, "w", encoding="utf-8") as f:
                    json.dump(dados_finais, f, ensure_ascii=False, indent=4)
                print(f"✅ JSON parcial atualizado para {pais}!")

        # Salva o JSON final com todos os dados
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(dados_finais, f, ensure_ascii=False, indent=4)
        print("📄 JSON final salvo com sucesso!")

        # Converte para DataFrame
        df = pd.DataFrame([
            (pais, dados["Continente"], piloto["Nome"], piloto["Categorias"], piloto["Sexo"], piloto["Data de Nascimento"], piloto["Data de Óbito"], piloto["Idade"], piloto["Link"], piloto["iRacing"])
            for pais, dados in dados_finais.items() for piloto in dados["Pilotos"]
        ], columns=["País", "Continente", "Piloto", "Categorias", "Sexo", "Data de Nascimento", "Data de Óbito", "Idade", "Link", "iRacing"])

        df.to_json("pilotos_dataframe.json", orient="records", force_ascii=False, indent=4)
        print("📊 DataFrame salvo como JSON!")
    
    # Exibir no Streamlit
    with open('pilotos_dataframe.json', "r", encoding="utf-8") as f:
        json_data = json.load(f)
    pd.set_option("future.no_silent_downcasting", False)
    df_pilotos=pd.DataFrame(json_data)
    df_pilotos.drop_duplicates(subset="Link", inplace=True)
    df_pilotos.drop(df_pilotos[df_pilotos['Data de Óbito']!='-'].index,inplace=True)
    df_pilotos.reset_index(inplace=True,drop= 'index')
    cols = list(df_pilotos.columns.values)
    df_pilotos['Campeonatos']=df_pilotos['Categorias']
    #simplificando as categorias
    df_pilotos=df_pilotos.explode('Categorias')
    df_pilotos["Categorias"] = df_pilotos["Categorias"].apply(simplificar_categoria)

    # Criar uma coluna temporária para armazenar a ordem original
    df_pilotos["Ordem"] = df_pilotos.index  

    # Agrupar mantendo a ordem original
    df_pilotos = df_pilotos.groupby("Link", as_index=False).agg({
        "Piloto": "first",
        "País": "first",
        "Continente": "first",
        "Sexo": "first",
        "Data de Nascimento": "first",
        "Data de Óbito": "first",
        "Idade": "first",
        "iRacing": "first",
        "Categorias": lambda x: list(set(x)),  # Remove duplicatas
        "Ordem": "first"  # Mantém a ordem original
    })

    # Ordenar de volta pela ordem original
    df_pilotos = df_pilotos.sort_values(by="Ordem").drop(columns=["Ordem"]).reset_index(drop=True)



    #Corrigir coluna da idade:
    for i in df_pilotos['Idade']:
        if i=='-':
            df_pilotos['Idade']=df_pilotos['Idade'].replace(i,0)
    



    # Se quiser categorias como string separada por vírgula
    df_pilotos["Categorias"] = df_pilotos["Categorias"].apply(lambda x: ", ".join(map(str,x)))

    df_pilotos=df_pilotos[cols]

    #Filtrar somente os com conta no Iracing:
    df_pilotos_ir=df_pilotos.explode('iRacing')
    df_pilotos_ir=df_pilotos_ir.dropna(subset='iRacing')



    continente = st.sidebar.multiselect(
        "Continente",
        df_pilotos_ir.get("Continente", pd.Series([])).dropna().unique().tolist(),
    )

    pais = st.sidebar.multiselect(
        "País",
        df_pilotos_ir[df_pilotos_ir["Continente"].isin(continente)].get("País", pd.Series([])).dropna().unique().tolist(),
    )

    sexo = st.sidebar.multiselect(
        "Sexo",
        df_pilotos_ir["Sexo"].unique().tolist(),
    )

    # 🔢 Ajustando o slider de idade
    idade_min = int(df_pilotos_ir["Idade"].min())
    idade_max = int(df_pilotos_ir["Idade"].max())

    idade = st.sidebar.slider("Faixa Etária", idade_min, idade_max, (idade_min, idade_max))

    # 🎯 Aplicando os filtros ao DataFrame
    df_filtrado = df_pilotos_ir.copy()

    if continente:
        df_filtrado = df_filtrado[df_filtrado["Continente"].isin(continente)]
    if pais:
        df_filtrado = df_filtrado[df_filtrado["País"].isin(pais)]
    if sexo:
        df_filtrado = df_filtrado[df_filtrado["Sexo"].isin(sexo)]

    df_filtrado = df_filtrado[
        (df_filtrado["Idade"] >= idade[0]) & (df_filtrado["Idade"] <= idade[1])
    ]
    df_filtrado["iRacing"]=df_filtrado["iRacing"].astype("str")
    df_filtrado=df_filtrado.rename(columns={"iRacing": "iRacing Cust ID"})
    df_filtrado=df_filtrado.set_index('iRacing Cust ID')
    
    col_exib=['Piloto','Sexo','País','Continente','Categorias','Idade']
    # 📊 Exibir DataFrame filtrado
    col1, col2, col3 = st.columns([0.3, 0.7, 0.3])
    with col1:
        # st.image("https://upload.wikimedia.org/wikipedia/commons/1/10/Motorsport.com_Logo.png", width=128)
        st.markdown("[![Foo](https://upload.wikimedia.org/wikipedia/commons/1/10/Motorsport.com_Logo.png)](https://www.motorsport.com/)")
    with col2:
        st.title('Pilotos no iRacing')
    with col3:
        # st.image("https://scontent.fcgh11-1.fna.fbcdn.net/v/t39.30808-6/436209253_934224621832968_7585876196882521227_n.jpg?_nc_cat=106&ccb=1-7&_nc_sid=6ee11a&_nc_ohc=X9uSTnzHGHgQ7kNvgFJPwUH&_nc_oc=AdhpWAjtcffiWfJlF_hyMLuIlt6Ut06WaJF6kLf8wStWBbK14sbH_3eetpSu4chO1vp58TxmoQQGFb_YE4u6dxlZ&_nc_zt=23&_nc_ht=scontent.fcgh11-1.fna&_nc_gid=AemZtlfETPIx9LdkmtXN4YK&oh=00_AYBL_sLmhTnzSJDnh2C5dCaD5E4Qym5N8oTbUqnyb6OOAw&oe=67BAED00", width=100)
        st.markdown("[![Foo](https://scontent.fcgh11-1.fna.fbcdn.net/v/t39.30808-6/436209253_934224621832968_7585876196882521227_n.jpg?_nc_cat=106&ccb=1-7&_nc_sid=6ee11a&_nc_ohc=X9uSTnzHGHgQ7kNvgFJPwUH&_nc_oc=AdhpWAjtcffiWfJlF_hyMLuIlt6Ut06WaJF6kLf8wStWBbK14sbH_3eetpSu4chO1vp58TxmoQQGFb_YE4u6dxlZ&_nc_zt=23&_nc_ht=scontent.fcgh11-1.fna&_nc_gid=AemZtlfETPIx9LdkmtXN4YK&oh=00_AYBL_sLmhTnzSJDnh2C5dCaD5E4Qym5N8oTbUqnyb6OOAw&oe=67BAED00)](https://www.driverdb.com/)")
    st.write("Quem não gostaria de dividir a pista virtual com um piloto real? Agora, com o iRacing e este app, isso é possivel! Encontre abaixo pilotos do mundo inteiro!")
    st.write("Os dados foram adquiridos das plataformas Motorsport, coms os nomes, e Driver Database com os históricos. Leve em consideração que alguns casos podem ser pessoas homônimas.")
    st.write("Lista de Pilotos")
    st.dataframe(df_filtrado[col_exib])


    

# Executa a função principal
if __name__ == "__main__":
    main()
    
    # # st.write(df_pilotos.head())  # Mostra as primeiras linhas para depuração
    # st.write(df_pilotos.columns)  # Mostra as colunas existentes

