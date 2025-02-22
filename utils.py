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
import chromedriver_autoinstaller
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def encode_pw(username, password):
    initialHash = hashlib.sha256((password + username.lower()).encode('utf-8')).digest()

    hashInBase64 = base64.b64encode(initialHash).decode('utf-8')

    return hashInBase64
def remover_acentos(texto):
    """
    Remove acentos de uma string para facilitar a busca no iRacing.
    """
    return ''.join(
        c for c in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(c)
    )

def converter_data(data_str):
    """
    Converte uma string de data para o formato YYYY-MM-DD.
    Se a conversão falhar, retorna '-'.
    """
    if data_str in ["-", "", None]:  # Se for um valor inválido, retorna '-'
        return "-"

    formatos = ["%d %b %Y", "%Y-%m-%d"]  # Possíveis formatos de data

    for formato in formatos:
        try:
            return datetime.strptime(data_str, formato).strftime("%Y-%m-%d")
        except ValueError:
            continue  # Se o formato falhar, tenta o próximo

    return "-"  # Se nenhum formato funcionar, retorna '-'

# Função para verificar a idade do JSON
def verificar_json_existente(JSON_FILE):
    if os.path.exists(JSON_FILE):
        file_age = time.time() - os.path.getmtime(JSON_FILE)
        if file_age < 30 * 24 * 3600:  # Menos de 30 dias
            print("Arquivo JSON ainda é recente, não precisa atualizar.")
            return True
    print("Arquivo JSON ainda não existe.")
    return False

# Função para verificar se o JSON de progresso existe
def verificar_json_parcial_existente():
    print("Arquivo JSON parcial, existe!")
    return os.path.exists(JSON_FILE_P)

# Função para obter o continente do país
def obter_continente(pais):
    try:
        codigo_pais = pc.country_name_to_country_alpha2(pais, cn_name_format="default")
        codigo_continente = pc.country_alpha2_to_continent_code(codigo_pais)
        continentes = {
            "AF": "África",
            "NA": "América do Norte",
            "SA": "América do Sul",
            "AS": "Ásia",
            "EU": "Europa",
            "OC": "Oceania",
        }
        return continentes.get(codigo_continente, "-")
    except:
        return "-"

# Função para determinar o gênero baseado no primeiro nome
def determinar_sexo(piloto):
    # Detector de gênero
    detector = Detector()
    primeiro_nome = piloto.split()[0]
    sexo = detector.get_gender(primeiro_nome)
    
    return "Masculino" if sexo in ["male", "mostly_male"] else "Feminino" if sexo in ["female", "mostly_female"] else "-"

# Função para buscar países do Driver Database
def obter_paises():
    url = "https://www.driverdb.com/countries"
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        country_links = soup.select('div.TableRow_tableRow__Z5W8Y a')  # Ajuste conforme necessário
        country_elements = [link.text.strip() for link in country_links]
        return [c for c in country_elements]
    else:
        print(f'Falha ao acessar a página. Status code: {response.status_code}')

# Função para obter pilotos de um país com seus links corretos
def obter_pilotos(pais):
    url = f"https://www.driverdb.com/countries/{pais.replace(' ', '-').lower()}"
    print(f"Acessando URL: {url}")

    # Configurações do Selenium para rodar sem abrir a janela do navegador
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executa em modo headless (sem abrir o navegador)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

    # Inicializa o WebDriver do Chrome
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)

    # Espera um tempo para a página carregar (ajuste conforme necessário)
    time.sleep(5)

    pilotos = []

    # Obtém todas as linhas da tabela de pilotos
    elementos = driver.find_elements(By.CSS_SELECTOR, ".TableRow_tableRow__Z5W8Y.DriversByCountry_row__S24aN")

    for element in elementos:
        try:
            # Obtém nome do piloto
            nome_elemento = element.find_element(By.CSS_SELECTOR, ".DriversByCountry_driver__6JOb0")
            first_name = nome_elemento.find_element(By.TAG_NAME, "span").text.strip()
            last_name = nome_elemento.text.replace(first_name, "").strip()
            nome_completo = f"{first_name} {last_name}".strip()

            # Obtém link do perfil do piloto
            link_elemento = element.find_element(By.TAG_NAME, "a").get_attribute("href")

            pilotos.append({"Nome": nome_completo, "Link": link_elemento})
        except Exception as e:
            print(f"Erro ao processar um piloto: {e}")

    driver.quit()
    
    print(pilotos)
    return pilotos

def calcular_idade(nascimento, obito="-"):
    """
    Calcula a idade com base na data de nascimento e óbito.
    Se o óbito for "-", calcula a idade atual.
    """
    nascimento = converter_data(nascimento)  # Converte para o formato correto
    obito = converter_data(obito)  # Converte para o formato correto

    if nascimento == "-":
        return 0

    try:
        data_nasc = datetime.strptime(nascimento, "%Y-%m-%d")
        data_fim = datetime.strptime(obito, "%Y-%m-%d") if obito != "-" else datetime.today()

        idade = data_fim.year - data_nasc.year - ((data_fim.month, data_fim.day) < (data_nasc.month, data_nasc.day))
        return idade
    except Exception as e:
        print(f"Erro ao calcular idade: {e}")
        return "-"

def obter_dados_piloto(piloto):
    url_piloto = piloto["url"]
    print(f"Acessando: {url_piloto}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    # Fazendo a requisição HTTP
    response = requests.get(url_piloto, headers=headers)

    if response.status_code != 200:
        print(f"⚠️ Erro ao obter dados de {piloto['driverName']}, Status Code: {response.status_code}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")

    # Coletando categorias
    categorias = [a.text.strip() for a in soup.select(".CareerDetails_detailsTable__wU5j2 a")]

    # Coletando informações pessoais
    nascimento = "-"
    obito = "-"
    idade = "-"

    try:
        info_piloto = soup.select_one(".DriverInfo_infoTable__06taW").get_text(separator="\n").lower()
        linhas = info_piloto.split("\n")

        for i, linha in enumerate(linhas):
            if "birthday" in linha:
                nascimento = linhas[i + 1] if i + 1 < len(linhas) else "-"
            elif "died" in linha:
                obito = linhas[i + 1] if i + 1 < len(linhas) else "-"

        idade = calcular_idade(nascimento, obito)  # Mantendo a função de calcular idade
        
    except Exception as e:
        print(f"Erro ao coletar datas: {e}")

    try:
        sexo = determinar_sexo(piloto['driverName'])
       
    except Exception as e:
        print(f"Erro ao definir sexo: {e}")

    dados_piloto = {
        "nome": piloto["driverName"],
        "pais":pc.country_alpha2_to_country_name(piloto["countryCode"].upper()),
        "sexo":sexo,
        "categorias": categorias,
        "nascimento": nascimento,
        "obito": obito,
        "idade": idade,
        "link":piloto["url"]
    }
  

    print(f"✅ Dados obtidos para {piloto['driverName']}!")

    return dados_piloto
    


# Função para verificar se o piloto tem conta no iRacing
def verificar_iracing(piloto):
    """
    Busca o piloto no iRacing pelo nome original e sem acentos.
    Retorna uma lista de customer_ids únicos.
    """
    try:
        nome_original = piloto["nome"]
        nome_sem_acento = remover_acentos(nome_original)
        dados_final=[]
        # Buscar com o nome original
        dados_original = idc.lookup_drivers(nome_original) or []
        ids_original = {d["cust_id"] for d in dados_original}

        # Buscar com o nome sem acento (se for diferente do original)
        ids_sem_acento = set()
        if nome_original != nome_sem_acento:
            dados_sem_acento = idc.lookup_drivers(nome_sem_acento) or []
            ids_sem_acento = {d["cust_id"] for d in dados_sem_acento}

        # Unir os IDs sem duplicatas
        ids=list(ids_original | ids_sem_acento)  # Combina os dois conjuntos
        for i in ids:
            irl=get_ir_licenses(i)
            dados_final.append({
            "nome":piloto["nome"],
            "pais":piloto["Nome"],
            "sexo":piloto["sexo"],
            "categorias":piloto["categorias"],
            "nascimento":piloto["nascimento"],
            "obito":piloto["obito"],
            "idade":piloto["idade"],
            "link":piloto["link"],
            "cust_id":i,
            'Road IR':irl[1]['Road IR'],
            'Road SR':irl[1]['Road SR'],
            'Road LL':irl[1]['Road LL'],
            'Form IR':irl[2]['Form IR'],
            'Form SR':irl[2]['Form SR'],
            'Form LL':irl[2]['Form LL'],
            'Oval IR':irl[0]['Oval IR'],
            'Oval SR':irl[0]['Oval SR'],
            'Oval LL':irl[0]['Oval LL'],
            'DRoad IR': irl[4]['DRoad IR'],
            'DRoad SR': irl[4]['DRoad SR'],
            'DRoad LL': irl[4]['DRoad LL'],
            'DOval IR': irl[3]['DOval IR'],
            'DOval SR': irl[3]['DOval SR'],
            'DOval LL': irl[3]['DOval LL'],
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

def get_ir_licenses(cust_id):
    last_login=idc.member_profile(cust_id=cust_id)['member_info']['last_login']
    irl=idc.member_profile(cust_id=cust_id)['member_info']['licenses']
    licenses={
        'Road IR':irl[1]['irating'],
        'Road SR':irl[1]['safety_rating'],
        'Road LL':irl[1]['group_name'].strip('Class '),
        'Form IR':irl[2]['irating'],
        'Form SR':irl[2]['safety_rating'],
        'Form LL':irl[2]['group_name'].strip('Class '),
        'Oval IR':irl[0]['irating'],
        'Oval SR':irl[0]['safety_rating'],
        'Oval LL':irl[0]['group_name'].strip('Class '),
        'DRoad IR': irl[4]['irating'],
        'DRoad SR': irl[4]['safety_rating'],
        'DRoad LL': irl[4]['group_name'].strip('Class '),
        'DOval IR': irl[3]['irating'],
        'DOval SR': irl[3]['safety_rating'],
        'DOval LL': irl[3]['group_name'].strip('Class '),
        'last_login':last_login
    }
    return licenses

def driver_df(file):
    driver_df=pd.DataFrame(file)
    return driver_df

def simplificar_categoria(nome_categoria):
  # Dicionário de mapeamento das categorias
  categoria_mapeamento = {
    "Kart": [
        "FIA Karting Academy Trophy", "WSK Champions Cup", "CIK-FIA Karting",
        "Karting European Championship", "Karting Super Cup", "WSK Open Cup"
    ],
    "F4": [
        "British F4 Championship", "Italian F4 Championship", "F4 Spanish Championship",
        "Formula Winter Series", "Formula Academy"
    ],
    "F3": [
        "FIA Formula 3 Championship", "Formula Regional European Championship",
        "Euroformula Open", "Toyota Racing Series", "Asian F3 Championship"
    ],
    "F2": [
        "FIA Formula 2 Championship", "GP2 Series", "Formula Renault 3.5",
        "World Series by Renault"
    ],
    "F1": [
        "FIA Formula 1 World Championship", "Formula 1", "F1 Academy"
    ],
    "Rally": [
        "FIA World Rally Championship", "European Rally Championship",
        "Rallycross Championship", "Dakar Rally"
    ],
    "GT": [
        "GT World Challenge", "GT Masters", "GT Open", "GT Winter Series",
        "Blancpain GT Series", "Super GT", "IMSA GT"
    ],
    "Endurance": [
        "FIA World Endurance Championship", "European Le Mans Series",
        "Asian Le Mans Series", "IMSA WeatherTech SportsCar Championship"
    ],
    "Touring": [
        "British Touring Car Championship", "WTCR - FIA World Touring Car Cup",
        "DTM", "Supercars Championship", "European Touring Car Championship"
    ],
    "Stock Car": [
        "Stock Car Pro Series", "Stock Light"
    ],
    "IndyCar": [
        "IndyCar Series", "Indy Lights", "Indy Pro 2000 Championship"
    ],
    "Nascar": [
        "NASCAR Cup Series", "NASCAR Xfinity Series", "NASCAR Truck Series"
    ],
    "Moto": [
        "MotoGP", "Moto2", "Moto3", "Superbike World Championship"
    ]
}
  for categoria_simplificada, lista_nomes in categoria_mapeamento.items():
        if nome_categoria in lista_nomes:
            return categoria_simplificada
  return nome_categoria  # Se não estiver no dicionário, mantém o nome original