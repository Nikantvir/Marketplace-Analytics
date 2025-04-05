import requests
import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class OzonAPI:
    BASE_URL = "https://api-seller.ozon.ru"
    
    def __init__(self, client_id=None, api_key=None):
        self.client_id = client_id or os.environ.get('OZON_CLIENT_ID')
        self.api_key = api_key or os.environ.get('OZON_API_KEY')
        
        if not self.client_id or not self.api_key:
            raise ValueError("Ozon client_id or api_key is not provided")
        
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, method, endpoint, data=None):
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to Ozon API failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_transactions(self, date_from, date_to, limit=1000):
        """
        Get transaction data for a specific date range
        """
        endpoint = "/v3/finance/transaction/list"
        data = {
            "filter": {
                "date": {
                    "from": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                    "to": date_to.strftime("%Y-%m-%dT23:59:59.999Z")
                }
            },
            "limit": limit,
            "offset": 0
        }
        return self._make_request("POST", endpoint, data=data)
    
    def get_stocks(self):
        """
        Get current stock data
        """
        endpoint = "/v2/analytics/stock_on_warehouses"
        data = {
            "limit": 1000,
            "offset": 0
        }
        return self._make_request("POST", endpoint, data=data)
    
    def get_sales(self, date_from, date_to):
        """
        Get sales data for a specific date range
        """
        endpoint = "/v1/analytics/sales"
        data = {
            "date_from": date_from.strftime("%Y-%m-%d"),
            "date_to": date_to.strftime("%Y-%m-%d"),
            "metrics": ["ordered_units", "revenue", "returns"],
            "dimension": ["sku", "day"],
            "limit": 1000,
            "offset": 0
        }
        return self._make_request("POST", endpoint, data=data)
    
    def get_orders(self, date_from, date_to):
        """
        Get orders data for a specific date range
        """
        endpoint = "/v3/posting/fbs/list"
        data = {
            "dir": "ASC",
            "filter": {
                "since": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                "to": date_to.strftime("%Y-%m-%dT23:59:59.999Z")
            },
            "limit": 1000,
            "offset": 0
        }
        return self._make_request("POST", endpoint, data=data)