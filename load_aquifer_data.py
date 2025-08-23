import requests
from config import config


def fetch_aquifer_api_data(endpoint):
    """Fetch JSON data from Aquifer API"""
    url = f"{config.aquifer_base_url}/{endpoint}"
    headers = {"api-key": config.aquifer_api_key, "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"API request to {endpoint} failed with status {response.status_code}")
    return response.json()