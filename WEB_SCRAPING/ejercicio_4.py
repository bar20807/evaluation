import requests
from bs4 import BeautifulSoup

# URL de la página de CoinMarketCap
url = 'https://coinmarketcap.com/currencies/bitcoin/'

# Encabezados para simular un navegador (opcional, pero útil para evitar bloqueos)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Realiza una solicitud GET para obtener el contenido de la página
response = requests.get(url, headers=headers)

# Verifica si la solicitud fue exitosa (código de respuesta 200)
if response.status_code == 200:
    # Analiza el contenido de la página web con BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encuentra el elemento que contiene el precio actual del Bitcoin
    price_tag = soup.find('span', {'data-test': 'text-cdp-price-display'})
    
    if price_tag:
        # Extrae el texto del elemento
        bitcoin_price = price_tag.text.strip()
        print(f'El precio actual del Bitcoin es: {bitcoin_price}')
    else:
        print('No se pudo encontrar el elemento del precio.')
else:
    print(f'Error al hacer la solicitud. Código de respuesta: {response.status_code}')
