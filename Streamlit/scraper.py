import json
import os
import asyncio
import aiohttp
import time
from bs4 import BeautifulSoup
from itertools import chain
from tqdm import tqdm

SAVE_FILE = "scraper_driver_data_progress.json"
BATCH_SIZE = 1000  # Número máximo de pilotos por lote
WAIT_TIME = 180 # Tempo de espera entre lotes (em segundos)



async def fetch_motorsport_pilotos(session, url):
    """Faz a requisição e extrai os nomes dos pilotos do motorsport.com"""
    
    async with session.get(url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        pilotos = [span.get_text(strip=True) for span in soup.find_all("span", class_="ms-filter-option_title")]
        return pilotos



async def scrape_motorsport():
    """Coleta a lista de pilotos do site motorsport.com"""
    base_url = 'https://www.motorsport.com/all/drivers/listing/?letter='
    alfabeto = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    total_pilotos = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in tqdm(alfabeto):
            url = base_url + i
            tasks.append(fetch_motorsport_pilotos(session, url))

        results = await asyncio.gather(*tasks)
        total_pilotos = list(chain.from_iterable(results))

    print('Aquisição do site motorsport.com finalizada!')
    return total_pilotos


async def fetch_driver_data(session, piloto):
    """Busca dados de um piloto com tratamento de erros"""
    timeout = aiohttp.ClientTimeout(total=60)  # Aumenta para 60 segundos
    base_url = 'https://www.driverdb.com/_next/data/Egwlez2ZANBhAcd5XISJE/drivers/search.json?search='
    url = base_url + piloto.strip().replace(" ", "+")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    
    for attempt in range(3):  # Tenta no máximo 3 vezes antes de desistir
        try:
            async with session.get(url, headers=headers,timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    drivers = data.get("pageProps", {}).get("driversByName", [])
                    return [{"driverName": d["driverName"], 
                             "url": f"https://www.driverdb.com/drivers/{d['url']}", 
                             "countryCode": d["countryCode"]} for d in drivers]
                elif response.status in [429, 500, 503]:  # Servidor sobrecarregado
                    print(f"🔴 Limite atingido! Esperando {WAIT_TIME} segundos antes de tentar novamente...")
                    await asyncio.sleep(WAIT_TIME)
                else:
                    print(f"⚠️ Erro {response.status} ao coletar {piloto}")
                    return []
        except aiohttp.ClientError as e:
            print(f"⚠️ Erro de conexão ({e}) ao coletar {piloto}. Tentando novamente... {attempt+1}/3")
            await asyncio.sleep(5)  # Espera 5 segundos antes de tentar de novo

    print(f"❌ Falha ao processar {piloto} após várias tentativas.")
    return []

async def driver_db_scrape_async(total_pilot):
    """Coleta os dados dos pilotos em lotes e reinicia para evitar bloqueios"""
    driver_data = []
    
    if os.path.exists(SAVE_FILE):
        with open(SAVE_FILE, "r") as f:
            try:
                saved_data = json.load(f)
            except json.JSONDecodeError:
                saved_data = []
    else:
        saved_data = []
    
    collected_names = {entry["driverName"] for entry in saved_data}
    remaining_pilots = [p for p in total_pilot if p not in collected_names]

    if not remaining_pilots:
        print("✅ Todos os pilotos já foram coletados!")
        return saved_data

    async with aiohttp.ClientSession() as session:
        print(f"São {len(remaining_pilots)} pilotos remanescentes...")
        for i in tqdm(range(0, len(remaining_pilots), BATCH_SIZE)):
            batch = remaining_pilots[i:i + BATCH_SIZE]
            print(f"🚀 Coletando {len(batch)} pilotos...")

            tasks = [fetch_driver_data(session, piloto) for piloto in batch]
            results = await asyncio.gather(*tasks)  # Executa as requisições em paralelo

            for res in results:
                driver_data.extend(res)
            
            # Salva progresso após cada lote
            saved_data.extend(driver_data)
            with open(SAVE_FILE, "w") as f:
                json.dump(saved_data, f, indent=4)

            print(f"✅ Lote de {len(batch)} pilotos salvo. Aguardando {WAIT_TIME} segundos antes de continuar...")
            await asyncio.sleep(WAIT_TIME)  # Espera para evitar bloqueios
            
    print("🏁 Coleta finalizada!")
    return saved_data

async def main():
    total_pilot = await scrape_motorsport() # Aqui você coloca a lista de pilotos coletada de outra função
    driver_data = await driver_db_scrape_async(total_pilot)
    print("🔹 Coleta finalizada com sucesso!")


# Executa a função principal assíncrona
asyncio.run(main())

