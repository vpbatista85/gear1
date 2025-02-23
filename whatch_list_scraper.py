from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from iracingdataapi.client import irDataClient
import requests
import hashlib
import base64
from seleniumwire import webdriver
import ast
import json
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By



username = 'vitor_pbatista@outlook.com'
password = '1199panigale'

def encode_pw(username, password):
    initialHash = hashlib.sha256((password + username.lower()).encode('utf-8')).digest()
    hashInBase64 = base64.b64encode(initialHash).decode('utf-8')
    return hashInBase64

pwValueToSubmit = encode_pw(username, password)

login_url = "https://members-ng.iracing.com/auth"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

payload = {
    "email": username,
    "password": pwValueToSubmit   # Ou "password": pwValueToSubmit se for necessário
}

session = requests.Session()
response = session.post(login_url, json=payload, headers=headers)

print("Resposta do login:", response.status_code, response.text)
print("Cookies da sessão:", session.cookies.get_dict())

# Obter cookies da sessão autenticada
cookies = session.cookies.get_dict()

# # Tente acessar o perfil para validar o login
# profile_url = "https://members-ng.iracing.com/data/member/info"
# profile_response = session.get(profile_url, headers=headers)

# print("Resposta do perfil:", profile_response.status_code, profile_response.text)

# # Tente acessar o link da resposta acima
# profile_url = ast.literal_eval(profile_response.text)
# profile_response = session.get(profile_url['link'], headers=headers)
# print(profile_response.headers())

# # Salva o JSON final com todos os dados
# with open('resposta_iracing', "w", encoding="utf-8") as f:
#     json.dump(profile_response.text, f, ensure_ascii=False, indent=4)
# print("📄 JSON salvo com sucesso!")

# print("Resposta do link:", profile_response.status_code)

# # Configurar o Selenium Wire
# options = {
#     'verify_ssl': False,  # Evita erros de SSL
# }
# Configuração do Selenium Wire
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Rodar em modo headless (opcional)
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# # Inicializar o navegador
# driver = webdriver.Chrome()
driver = webdriver.Chrome(options=chrome_options)

# # Acessar a página inicial do iRacing
# driver.get("https://members.iracing.com/membersite/member/Home.do")

# Adicionar os cookies autenticados ao WebDriver
for key, value in cookies.items():
    cookie_dict = {
        "name": key,
        "value": value,
        "domain": ".iracing.com",
        "path": "/"
    }
    driver.add_cookie(cookie_dict)

# Recarregar a página para aplicar os cookies
driver.get("https://members-ng.iracing.com/data/member/info")

# Esperar um tempo para garantir que a página carregue
time.sleep(3)

# Validar se o login foi bem-sucedido
if "Logout" in driver.page_source:
    print("Login no WebDriver bem-sucedido!")
else:
    print("Erro ao autenticar com o WebDriver.")

# Capturar e exibir as requisições HTTP feitas pelo Selenium Wire
print("\nRequisições capturadas pelo Selenium Wire:")
for request in driver.requests:
    print(f"URL: {request.url}")
    print(f"Método: {request.method}")
    print(f"Status: {request.response.status_code if request.response else 'Sem resposta'}")
    print("-" * 50)


try:
    # Acessar a página
    
    driver.get("https://members-ng.iracing.com/web/racing/home/dashboard?cust_id=593443&tab=profile")
    # Access requests via the `requests` attribute

    # Aguardar carregamento da página
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".btn-group.dropup"))
    )

    # Clicar no botão que abre o dropdown
    botao_dropdown = driver.find_element(By.CSS_SELECTOR, '.btn-group.dropup .dropdown-toggle')
    botao_dropdown.click()

    # Esperar o menu abrir
    time.sleep(1)

    # Agora encontrar o botão "Follow" dentro do dropdown aberto
    botao_follow = driver.find_element(By.XPATH, "//a[contains(@class, 'dropdown-item') and contains(text(), 'Follow')]")
    botao_follow.click()

    print("Botão 'Follow' clicado com sucesso!")

    # Aguardar possíveis requisições
    time.sleep(5)

    # Capturar requisições feitas pela página
    for request in driver.requests:
        if request.response and "iracing.com" in request.url:
            print(f"URL: {request.url}")
            print(f"Método: {request.method}")
            print(f"Status Code: {request.response.status_code}")
            print(f"headers:{request.response.headers['Content-Type']}")

finally:
    driver.quit()
# driver.quit()