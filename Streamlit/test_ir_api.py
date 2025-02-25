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
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.by import By
# import streamlit as st
import unicodedata
import os
import asyncio
import logging
# from aiohttp import ClientSession
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# from utils import verificar_iracing, encode_pw, get_ir_licenses

username = 'vitor_pbatista@outlook.com'
password = '1199panigale'

# Configuração de arquivos
JSON_FILE = "pilotos_df.json"  # Contém os dados básicos dos pilotos (ESSENCIAL)
JSON_FILE_P = "driver_ir_data_progress.json"  # Armazena progresso parcial
DF_File = "pilotos_ir_df.json"  # Arquivo final

# Configuração de concorrência
BATCH_SIZE = 10  # Processa 10 pilotos por vez
MAX_RETRIES = 3  # Número máximo de tentativas


def carregar_json():
    """Carrega o arquivo essencial com os pilotos."""
    if not os.path.exists(JSON_FILE):
        print(f"❌ ERRO: O arquivo {JSON_FILE} não foi encontrado!")
        exit(1)

    with open(JSON_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def processar_batch(pilotos_batch, idc):
    """Processa um lote de pilotos de forma síncrona."""
    resultados = []
    for piloto in pilotos_batch:
        resultado = verificar_iracing(piloto, idc)
        if resultado:
            resultados.append(resultado)
    return resultados

def remover_acentos(texto):
    """
    Remove acentos de uma string para facilitar a busca no iRacing.
    """
    return ''.join(
        c for c in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(c)
    )

def verificar_iracing(piloto, idc):
    """
    Busca o piloto no iRacing pelo nome original e sem acentos.
    Retorna uma lista de customer_ids únicos.
    """
    # from iracingdataapi.client import irDataClient
    # from seg import password, username

    # idc=irDataClient()
    try:
        nome_original = piloto["nome"]
        print(nome_original)
        nome_sem_acento = remover_acentos(nome_original)
        dados_final=[]
        # Buscar com o nome original
        dados_original = idc.lookup_drivers(nome_original) or []
        
        time.sleep(1)
        dados_sem_acento = idc.lookup_drivers(nome_sem_acento) or []
        ids_original = {d["cust_id"] for d in dados_original}
       

        # Buscar com o nome sem acento (se for diferente do original)
        ids_sem_acento = set()
        if nome_original != nome_sem_acento:
            dados_sem_acento = idc.lookup_drivers(nome_sem_acento) or []
            ids_sem_acento = {d["cust_id"] for d in dados_sem_acento}

        # Unir os IDs sem duplicatas
        ids=list(ids_original | ids_sem_acento)  # Combina os dois conjuntos
        
        for i in ids:
            irl=get_ir_licenses(i, idc)
            dados_final.append({
            "nome":piloto["nome"],
            "pais":piloto["pais"],
            "sexo":piloto["sexo"],
            "categorias":piloto["categorias"],
            "nascimento":piloto["nascimento"],
            "obito":piloto["obito"],
            "idade":piloto["idade"],
            "link":piloto["link"],
            "cust_id":i,
            "Road IR":irl["Road IR"],
            'Road SR':irl['Road SR'],
            'Road LL':irl['Road LL'],
            'Form IR':irl['Form IR'],
            'Form SR':irl['Form SR'],
            'Form LL':irl['Form LL'],
            'Oval IR':irl['Oval IR'],
            'Oval SR':irl['Oval SR'],
            'Oval LL':irl['Oval LL'],
            'DRoad IR': irl['DRoad IR'],
            'DRoad SR': irl['DRoad SR'],
            'DRoad LL': irl['DRoad LL'],
            'DOval IR': irl['DOval IR'],
            'DOval SR': irl['DOval SR'],
            'DOval LL': irl['DOval LL'],
            'last_login':irl['last_login']
            }
            )
            

    except KeyError:
        logging.error(f"Erro ao acessar customer_id para piloto: {piloto}")
        return []
    except Exception as e:
        logging.error(f"Erro inesperado ao verificar piloto '{piloto}': {e}")
        return None
    return dados_final

def get_ir_licenses(cust_id, idc):
    try:
        perfil = idc.member_profile(cust_id=cust_id)
        if "member_info" not in perfil:
            return {}

        last_login = perfil["member_info"].get("last_login", "N/A")
        irl = perfil["member_info"].get("licenses", [])

        if len(irl) < 5:  # Garante que há dados suficientes para extrair
            return {}

        return {
            'Road IR': irl[1].get('irating', 0),
            'Road SR': irl[1].get('safety_rating', 0),
            'Road LL': irl[1].get('group_name', '').strip('Class '),
            'Form IR': irl[2].get('irating', 0),
            'Form SR': irl[2].get('safety_rating', 0),
            'Form LL': irl[2].get('group_name', '').strip('Class '),
            'Oval IR': irl[0].get('irating', 0),
            'Oval SR': irl[0].get('safety_rating', 0),
            'Oval LL': irl[0].get('group_name', '').strip('Class '),
            'DRoad IR': irl[4].get('irating', 0),
            'DRoad SR': irl[4].get('safety_rating', 0),
            'DRoad LL': irl[4].get('group_name', '').strip('Class '),
            'DOval IR': irl[3].get('irating', 0),
            'DOval SR': irl[3].get('safety_rating', 0),
            'DOval LL': irl[3].get('group_name', '').strip('Class '),
            'last_login': last_login
        }
    except Exception as e:
        logging.error(f"Erro ao obter licenças para {cust_id}: {e}")
        return {}


def main(username, password):
    print("📂 Carregando dados dos pilotos...")
    pilotos = carregar_json()  # Carrega o JSON essencial

    # Autenticação no iRacing
    idc = irDataClient(username=username, password=password)

    # Tenta carregar progresso existente
    progresso = []
    if os.path.exists(JSON_FILE_P):
        with open(JSON_FILE_P, "r", encoding="utf-8") as file:
            progresso = json.load(file)

    # Filtra pilotos já processados
    pilotos_pendentes = [p for p in pilotos if p["nome"] not in {x["nome"] for x in progresso}]

    if not pilotos_pendentes:
        print("✅ Todos os pilotos já foram processados!")
        return

    # Divide os pilotos em lotes
    batches = [pilotos_pendentes[i:i + BATCH_SIZE] for i in range(0, len(pilotos_pendentes), BATCH_SIZE)]

    dados_finais = progresso  # Começa com os dados já processados
    for i, batch in enumerate(batches):
        print(f"🔄 Processando batch {i+1}/{len(batches)}...")
        batch_resultado = processar_batch(batch, idc)

        # Salva progresso parcial
        with open(JSON_FILE_P, "w", encoding="utf-8") as f:
            json.dump(dados_finais + batch_resultado, f, ensure_ascii=False, indent=4)

        dados_finais.extend(batch_resultado)

    # Salva o JSON final
    with open(DF_File, "w", encoding="utf-8") as f:
        json.dump(dados_finais, f, ensure_ascii=False, indent=4)

    print("✅ JSON final salvo com sucesso!")


# Executa a função principal
if __name__ == "__main__":
    username = username
    password = password
    main(username, password)