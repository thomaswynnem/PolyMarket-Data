#%%
# Initialize necessary tools
import requests
import pandas as pd
import glob

# API Sources
polyScanUrl = 'https://api.polygonscan.com/api'
gammaURL = 'https://gamma-api.polymarket.com/markets'

years = [2023,2022,2021]

#%%
import os
import json

def getMarketsData(year):
    marketsData = []
    offset = 0
    limit = 50
    
    print(f"Fetching markets data for year {year}")
    
    os.makedirs('./data/bronze', exist_ok=True)
    
    while True:
        file_path = f'./data/bronze/markets_{year}_offset_{offset}.json'
        
        if os.path.exists(file_path):
            print(f"Loading existing data from {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
        else:
            print(f"Requesting data: offset={offset}, limit={limit}")
            params = {
                'offset': offset,
                'limit': limit,
                'order': 'id',
                'ascending': 1,
                'start_date_min': f'{year}-01-01T00:00:00Z',
                'start_date_max': f'{year+1}-01-01T00:00:00Z'
            }
            response = requests.get(gammaURL, params=params)
            data = response.json()
            
            if data:
                print(f"Saving data to {file_path}")
                with open(file_path, 'w') as f:
                    json.dump(data, f)
        
        if not data:
            print("No more data available")
            break
        
        print(f"Received {len(data)} markets")
        marketsData.extend(data)
        
        if len(data) < limit:
            print("Reached last page of data")
            break
        
        offset += limit
    
    print(f"Total markets fetched for {year}: {len(marketsData)}")


# %%
def main():
    for year in years:
        a = getMarketsData(year)

    marketsdf = pd.DataFrame()

    for f in glob.glob('./data/bronze/markets_*.json'):
        df = pd.read_json(f)
        marketsdf = pd.concat([marketsdf, df])

    os.makedirs('./data/silver', exist_ok=True)
    marketsdf.to_csv("./data/silver/markets.csv")

    return marketsdf

if __name__ == "__main__":
    main()