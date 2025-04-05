import requests
import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class WildberriesAPI:
    BASE_URL = "https://statistics-api.wildberries.ru"
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get('WILDBERRIES_API_KEY')
        if not self.api_key:
            raise ValueError("Wildberries API key is not provided")
        
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, method, endpoint, params=None, data=None):
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to Wildberries API failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_sales(self, date_from, date_to):
        """
        Get sales data for a specific date range
        """
        endpoint = "/api/v1/supplier/sales"
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
            "dateTo": date_to.strftime("%Y-%m-%dT23:59:59.999Z"),
            "limit": 1000,
            "rrdid": 0  # Request for all products
        }
        return self._make_request("GET", endpoint, params=params)
    
    def get_orders(self, date_from, date_to):
        """
        Get orders data for a specific date range
        """
        endpoint = "/api/v1/supplier/orders"
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
            "dateTo": date_to.strftime("%Y-%m-%dT23:59:59.999Z"),
            "limit": 1000,
            "next": 0  # Start from the beginning
        }
        return self._make_request("GET", endpoint, params=params)
    
    def get_stocks(self):
        """
        Get current stock data
        """
        endpoint = "/api/v1/supplier/stocks"
        params = {
            "dateFrom": datetime.now().strftime("%Y-%m-%dT00:00:00.000Z"),
        }
        return self._make_request("GET", endpoint, params=params)
    
    def get_incomes(self, date_from, date_to):
        """
        Get incomes data for a specific date range
        """
        endpoint = "/api/v1/supplier/incomes"
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
            "dateTo": date_to.strftime("%Y-%m-%dT23:59:59.999Z"),
        }
        return self._make_request("GET", endpoint, params=params)
    
    def get_report_details(self, date_from, date_to):
        """
        Get detailed report data for a specific date range
        """
        endpoint = "/api/v1/supplier/reportDetailByPeriod"
        data = {
            "dateFrom": date_from.strftime("%Y-%m-%d"),
            "dateTo": date_to.strftime("%Y-%m-%d"),
        }
        return self._make_request("POST", endpoint, data=data)