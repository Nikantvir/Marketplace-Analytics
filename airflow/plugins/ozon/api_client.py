import requests
import time
import json
from airflow.models import Variable

def safe_api_request(method, url, params=None, json_data=None, max_retries=3):
    headers = {
        "Client-Id": Variable.get("ozon_client_id"),
        "Api-Key": Variable.get("ozon_api_key"),
        "Content-Type": "application/json"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                time.sleep((2 ** attempt) * 5)
            else:
                response.raise_for_status()
        except Exception as e:
            if attempt == max_retries - 1:
                raise Exception(f"API Error: {str(e)}")
    return None

def fetch_data(endpoint, date_from, date_to, limit=1000):
    url = f"https://api-seller.ozon.ru{endpoint}"
    all_data = []
    page = 1
    
    while True:
        # Разные эндпоинты могут требовать разные структуры запроса
        if 'analytics' in endpoint:
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
                "offset": (page - 1) * limit
            }
        else:
            payload = {
                "filter": {
                    "date": {"from": date_from, "to": date_to},
                },
                "page": page,
                "page_size": limit
            }
        
        data = safe_api_request('POST', url, json_data=payload)
        
        # Проверка на наличие данных и их структуру
        if not data:
            break
            
        # У разных API могут быть разные структуры ответа
        result_data = data.get('result', data)
        rows = result_data.get('rows', result_data.get('items', []))
        
        if not rows:
            break
            
        all_data.extend(rows)
        
        if len(rows) < limit:
            break
            
        page += 1
        time.sleep(1)  # Задержка между запросами
        
    return all_data



