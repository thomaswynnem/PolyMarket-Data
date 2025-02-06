#%%
# Initialize necessary tools
import requests
import pandas as pd
import glob
from web3 import AsyncWeb3
import asyncio
import aiohttp

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


async def getBuyInfo(session, smartContract):
    file_path = f'./data/bronze/contract_buy_{smartContract}.csv'
    
    if os.path.exists(file_path):
        # make sure the smart contract address is in the columns
        df = pd.read_csv(file_path)
        changed = False
        if 'smartContract' not in df.columns:
            df['smartContract'] = smartContract
            changed = True
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
            changed = True
        if changed:
            df.to_csv(file_path, index=False)
            print(f"Updated {file_path} with smart contract address and removed Unnamed: 0 column")
            return
            
        print(f"Data for contract {smartContract} already exists. Skipping...")
        return

    eventSignature = "FPMMBuy(address,uint256,uint256,uint256,uint256)"
    eventSignatureHash = AsyncWeb3.keccak(text=eventSignature).hex()

    try:
        payload = {
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": "0", 
                    "toBlock": "latest", 
                    "address": smartContract,
                    "topics": [
                        f'0x{eventSignatureHash}'
                    ]
                }
            ],
            "id": 1,
            "jsonrpc": "2.0"
        }

        url = "https://polygon-rpc.com/"
        headers = {"Content-Type": "application/json"}

        while True:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logs = await response.json()
                    break
                elif response.status == 429:
                    print(f"Rate limit exceeded for contract {smartContract}. Waiting 10 seconds before retrying...")
                    await asyncio.sleep(10)
                else:
                    print(f"Error: {response.status}, {await response.text()}")
                    return

        abi = {
            "anonymous": False,
            "inputs": [
                {
                    "indexed": True,
                    "name": "buyer",
                    "type": "address"
                },
                {
                    "indexed": False,
                    "name": "investmentAmount",
                    "type": "uint256"
                },
                {
                    "indexed": False,
                    "name": "feeAmount",
                    "type": "uint256"
                },
                {
                    "indexed": True,
                    "name": "outcomeIndex",
                    "type": "uint256"
                },
                {
                    "indexed": False,
                    "name": "outcomeTokensBought",
                    "type": "uint256"
                }
            ],
            "name": "FPMMBuy",
            "type": "event"
        }

        web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        contract = web3.eth.contract(address=smartContract, abi=[abi])
        totalLogs = []
        for log in logs['result']:
            logData = log["data"]
            decodedLog = contract.events.FPMMBuy().process_log(log)
            info = decodedLog['args']
            totalLogs.append(info)
        
        df = pd.DataFrame(totalLogs)
        df['smartContract'] = smartContract

        # save the data to a csv
        df.to_csv(file_path)
        print(f"Data for contract {smartContract} saved to {file_path}")

    except Exception as e:
        print(f"Error for contract {smartContract}: {e}")

async def analyzeContracts(smartContracts):
    async with aiohttp.ClientSession() as session:
        tasks = [getBuyInfo(session, contract) for contract in smartContracts if contract.startswith('0x')]
        await asyncio.gather(*tasks)

async def getSellInfo(session, smartContract):
    try:
        url = "https://polygon-rpc.com/"
        payload = {
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": "0", 
                    "toBlock": "latest", 
                    "address": smartContract,
                    "topics": [
                        "0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4"
                    ]
                }
            ],
            "id": 1,
            "jsonrpc": "2.0"
        }
        headers = {"Content-Type": "application/json"}

        async with session.post(url, json=payload, headers=headers) as response:
            if response.status == 429:
                print(f"Rate limit exceeded for contract {smartContract}. Retrying in 10 seconds...")
                await asyncio.sleep(10)
                return await getSellInfo(session, smartContract)
            elif response.status != 200:
                print(f"Error: {response.status}, {await response.text()}")
                return
            logs = await response.json()

        abi = {
            "anonymous": False,
            "inputs": [
                {
                    "indexed": True,
                    "name": "seller",
                    "type": "address"
                },
                {
                    "indexed": False,
                    "name": "returnAmount",
                    "type": "uint256"
                },
                {
                    "indexed": False,
                    "name": "feeAmount",
                    "type": "uint256"
                },
                {
                    "indexed": True,
                    "name": "outcomeIndex",
                    "type": "uint256"
                },
                {
                    "indexed": False,
                    "name": "outcomeTokensSold",
                    "type": "uint256"
                }
            ],
            "name": "FPMMSell",
            "type": "event"
        }

        web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        contract = web3.eth.contract(address=smartContract, abi=[abi])
        totalLogs = []
        for log in logs['result']:
            decodedLog = contract.events.FPMMSell().process_log(log)
            info = decodedLog['args']
            totalLogs.append(info)
        
        df = pd.DataFrame(totalLogs)
        df['smartContract'] = smartContract

        file_path = f'./data/bronze/contract_sell_{smartContract}.csv'
        df.to_csv(file_path, index=False)
        print(f"Sell data for contract {smartContract} saved to {file_path}")

    except Exception as e:
        print(f"Error for contract {smartContract}: {e}")

async def analyzeSellContracts(smartContracts):
    async with aiohttp.ClientSession() as session:
        tasks = [getSellInfo(session, contract) for contract in smartContracts if contract.startswith('0x')]
        await asyncio.gather(*tasks)


# %%
def main():

    # pull market data from gamma
    for year in years:
        a = getMarketsData(year)

    marketsdf = pd.DataFrame()

    for f in glob.glob('./data/bronze/markets_*.json'):
        df = pd.read_json(f)
        marketsdf = pd.concat([marketsdf, df])

    os.makedirs('./data/silver', exist_ok=True)
    marketsdf.to_csv("./data/silver/markets.csv", index=False)

    # pull buydata from each contract from polygon-rpc
    smartContracts = marketsdf['marketMakerAddress'].drop_duplicates().tolist()
    asyncio.run(analyzeContracts(smartContracts))

    contractBuydf = pd.DataFrame()

    for f in glob.glob('./data/bronze/contract_buy_*.csv'):
        df = pd.read_csv(f)
        contractBuydf = pd.concat([contractBuydf, df])

    contractBuydf = contractBuydf.reset_index(drop=True)
    contractBuydf.to_csv("./data/silver/contract_buy.csv", index=False)
    

    # pull sell data from each contract from polygon-rpc
    asyncio.run(analyzeSellContracts(smartContracts))

    contractSelldf = pd.DataFrame()

    for f in glob.glob('./data/bronze/contract_sell_*.csv'):
        df = pd.read_csv(f)
        contractSelldf = pd.concat([contractSelldf, df])

    contractSelldf = contractSelldf.reset_index(drop=True)
    contractSelldf.to_csv("./data/silver/contract_sell.csv", index=False)

if __name__ == "__main__":
    main()