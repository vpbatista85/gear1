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
import unicodedata
import os
from utils import encode_pw, remover_acentos, converter_data, verificar_json_existente, obter_continente, determinar_sexo, obter_paises, obter_pilotos, calcular_idade, obter_dados_piloto, verificar_iracing, driver_df, simplificar_categoria

import asyncio
import logging
from aiohttp import ClientSession
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager



username = 'vitor_pbatista@outlook.com'
password = '1199panigale'

#login iRacing
idc=irDataClient(username=username,password=password)
pwValueToSubmit = encode_pw(username, password)
r = requests.post('https://members-ng.iracing.com/auth', auth=(username, pwValueToSubmit))
idc=irDataClient(username=username,password=password)


#Configuração de arquivos
JSON_FILE = "driver_data_progress.json"  # Contém os dados básicos dos pilotos (ESSENCIAL)
JSON_FILE_P = "driver_data_progress_parcial.json"  # Armazena progresso parcial
DF_File = "pilotos_df.json"  # Arquivo final

# Configuração de concorrência
BATCH_SIZE = 100  # Processa 100 pilotos por vez
MAX_RETRIES = 3  # Número máximo de tentativas
semaphore = asyncio.Semaphore(12)  # Limita 12 requisições simultâneas # Estava rodando muito bem com 3

def carregar_json():
    """Carrega o arquivo essencial com os pilotos."""
    if not os.path.exists(JSON_FILE):
        print(f"❌ ERRO: O arquivo {JSON_FILE} não foi encontrado!")
        exit(1)
    
    with open(JSON_FILE, "r", encoding="utf-8") as file:
        return json.load(file)

async def obter_dados_piloto_async(piloto, tentativa=0):
    """Obtém os dados do piloto de forma assíncrona, com retry."""
    async with semaphore:
        try:
            return obter_dados_piloto(piloto)
        except Exception as e:
            if tentativa < MAX_RETRIES:
                print(f"⚠️ Erro ao obter dados de {piloto['driverName']}, tentativa {tentativa+1}...")
                await asyncio.sleep(2 ** tentativa)  # Backoff exponencial
                return await obter_dados_piloto_async(piloto, tentativa + 1)
            print(f"❌ Falha ao obter dados de {piloto['driverName']} após {MAX_RETRIES} tentativas.")
            return None

async def processar_batch(pilotos_batch):
    """Processa um lote de pilotos de forma assíncrona."""
    tasks = [obter_dados_piloto_async(piloto) for piloto in pilotos_batch]
    resultados = await asyncio.gather(*tasks)
    return [r for r in resultados if r is not None]  # Remove falhas

async def main():
    print("📂 Carregando dados dos pilotos...")
    pilotos = carregar_json()  # Carrega o JSON essencial

    # Tenta carregar progresso existente
    progresso = []
    if os.path.exists(JSON_FILE_P):
        with open(JSON_FILE_P, "r", encoding="utf-8") as file:
            progresso = json.load(file)

    # Filtra pilotos já processados
    pilotos_pendentes = [p for p in pilotos if p["driverName"] not in {x["driverName"] for x in progresso}]

    if not pilotos_pendentes:
        print("✅ Todos os pilotos já foram processados!")
        return

    # Divide os pilotos em lotes
    batches = [pilotos_pendentes[i:i + BATCH_SIZE] for i in range(0, len(pilotos_pendentes), BATCH_SIZE)]

    dados_finais = progresso  # Começa com os dados já processados
    for i, batch in enumerate(batches):
        print(f"🔄 Processando batch {i+1}/{len(batches)}...")
        batch_resultado = await processar_batch(batch)

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
    asyncio.run(main())