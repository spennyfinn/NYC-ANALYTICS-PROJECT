import requests
from ingestion.config import SOCRATA_APP_TOKEN
import time

def fetch_pages(since: str, limit_per_page:int) -> list[dict]:
    offset=0
    MAX_RETRIES=3

    while True:
        socrata_endpoint= f"https://data.cityofnewyork.us/resource/erm2-nwe9.json"
        params= {
            "$limit": limit_per_page,
            "$offset": offset,
            "$where": f"created_date >= '{since}T00:00:00'",
            '$order': "created_date ASC"
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(socrata_endpoint, headers={"X-App-Token": SOCRATA_APP_TOKEN}, params=params)
                resp.raise_for_status()
                break

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES-1:
                    time.sleep(2**attempt)
                else:
                    raise RuntimeError(f'API request failed after {MAX_RETRIES} attempts {e}') from e

        try:
            data=resp.json()
        except requests.exceptions.JSONDecodeError as e: 
            raise RuntimeError(f'Failed to decode API response as JSON: {resp.text[:200]}') from e

        length = len(data)
        print(f'Fetched {length} rows of data')

        yield data
        

        if length < limit_per_page:
            return 
        
        offset+= limit_per_page
        
    

    

