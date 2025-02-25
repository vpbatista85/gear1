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
import asyncio
import logging
from aiohttp import ClientSession
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from utils import verificar_iracing, encode_pw, get_ir_licenses
from seg import username, password



# #Configuração de arquivos
# JSON_FILE = "pilotos_df.json"  # Contém os dados básicos dos pilotos (ESSENCIAL)
# JSON_FILE_P = "driver_ir_data_progress"  # Armazena progresso parcial
# DF_File = "pilotos_ir_df.json"  # Arquivo final

# # Configuração de concorrência
# BATCH_SIZE = 10  # Processa 100 pilotos por vez
# MAX_RETRIES = 3  # Número máximo de tentativas
# semaphore = asyncio.Semaphore(1)  # Limita 10 requisições simultâneas

# def carregar_json():
#     """Carrega o arquivo essencial com os pilotos."""
#     if not os.path.exists(JSON_FILE):
#         print(f"❌ ERRO: O arquivo {JSON_FILE} não foi encontrado!")
#         exit(1)
    
#     with open(JSON_FILE, "r", encoding="utf-8") as file:
#         return json.load(file)

# async def verificar_iracing_async(piloto, idc, tentativa=0):
#     """Obtém os dados do piloto de forma assíncrona, com retry."""
#     async with semaphore:
#         try:
#             resultado = verificar_iracing(piloto, idc)
#             await asyncio.sleep(1)  # Espera 1 segundo antes da próxima requisição
#             return resultado
#         except Exception as e:
#             if tentativa < MAX_RETRIES:
#                 print(f"⚠️ Erro ao obter dados de {piloto['nome']}, tentativa {tentativa+1}...")
#                 await asyncio.sleep(2 ** tentativa)  # Backoff exponencial
#                 return await verificar_iracing_async(piloto, idc, tentativa + 1)
#             print(f"❌ Falha ao obter dados de {piloto['nome']} após {MAX_RETRIES} tentativas.")
#             return None


# async def processar_batch(pilotos_batch, idc):
#     """Processa um lote de pilotos de forma assíncrona."""
#     tasks = [verificar_iracing_async(piloto, idc, tentativa=0) for piloto in pilotos_batch]
#     resultados = await asyncio.gather(*tasks)
#     return [r for r in resultados if r is not None]  # Remove falhas

# async def main():
#     #login iRacing
#     # pwValueToSubmit = encode_pw(username, password)
#     # r = requests.post('https://members-ng.iracing.com/auth', auth=(username, pwValueToSubmit))
#     print("📂 Carregando dados dos pilotos...")
#     pilotos = carregar_json()  # Carrega o JSON essencial

#     # Tenta carregar progresso existente
#     progresso = []
#     if os.path.exists(JSON_FILE_P):
#         with open(JSON_FILE_P, "r", encoding="utf-8") as file:
#             progresso = json.load(file)

#     # Filtra pilotos já processados
#     pilotos_pendentes = [p for p in pilotos if p["nome"] not in {x["nome"] for x in progresso}]

#     if not pilotos_pendentes:
#         print("✅ Todos os pilotos já foram processados!")
#         return

#     # Divide os pilotos em lotes
#     batches = [pilotos_pendentes[i:i + BATCH_SIZE] for i in range(0, len(pilotos_pendentes), BATCH_SIZE)]

#     dados_finais = progresso  # Começa com os dados já processados
#     for i, batch in enumerate(batches):
#         print(f"🔄 Processando batch {i+1}/{len(batches)}...")
#         batch_resultado = await processar_batch(batch,idc)

#         # Salva progresso parcial
#         with open(JSON_FILE_P, "w", encoding="utf-8") as f:
#             json.dump(dados_finais + batch_resultado, f, ensure_ascii=False, indent=4)

#         dados_finais.extend(batch_resultado)

#     # Salva o JSON final
#     with open(DF_File, "w", encoding="utf-8") as f:
#         json.dump(dados_finais, f, ensure_ascii=False, indent=4)

#     print("✅ JSON final salvo com sucesso!")

# # Executa a função principal
# if __name__ == "__main__":
#     idc=irDataClient(username=username,password=password)
#     asyncio.run(main())


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


# def verificar_iracing(piloto, idc, tentativa=0):
#     """Obtém os dados do piloto de forma síncrona, com retry."""
#     try:
#         resultado = idc.get_ir_licenses(piloto["id"],idc)  # Ajuste conforme necessário
#         time.sleep(1)  # Espera 1 segundo antes da próxima requisição
#         return resultado
#     except Exception as e:
#         if tentativa < MAX_RETRIES:
#             print(f"⚠️ Erro ao obter dados de {piloto['nome']}, tentativa {tentativa+1}...")
#             time.sleep(2 ** tentativa)  # Backoff exponencial
#             return verificar_iracing(piloto, idc, tentativa + 1)
#         print(f"❌ Falha ao obter dados de {piloto['nome']} após {MAX_RETRIES} tentativas.")
#         return None


def processar_batch(pilotos_batch,idc):
    """Processa um lote de pilotos de forma síncrona."""
    resultados = []
    for piloto in pilotos_batch:
        resultado = verificar_iracing(piloto,idc)
        if resultado:
            resultados.append(resultado)
    return resultados


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
        batch_resultado = processar_batch(batch,idc)

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
