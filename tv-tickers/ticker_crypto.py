import requests
import pandas as pd
import re

def cross_chain_assets(crypto_id):
        print(bool(re.search(r"\bbridged\b", crypto_id, re.IGNORECASE)) | bool(re.search(r"\bwrapped\b", crypto_id, re.IGNORECASE)))

def crypto_ticker_list():
    url = "https://api.coingecko.com/api/v3/coins/list"

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)

    return response.json()

def crypto_market_data(crypt_dict, marketcap_min=5000000):

    url = "https://api.coingecko.com/api/v3/coins/{0}".format(crypt_dict['id'])
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)

    if bool(cross_chain_assets(crypt_dict['id'])) ==  False | response.json()["market_data"]["market_cap"]["usd"] > marketcap_min:
        
        crypt_dict['categories'] = response.json()['categories']
        crypt_dict["market_cap (usd)"]=response.json()["market_data"]["market_cap"]["usd"]
        crypt_dict['market_cap_rank']=response.json()['market_cap_rank']
        crypt_dict['fully_diluted_valuation (usd)']=response.json()['market_data']['fully_diluted_valuation']['usd']
    
        return crypt_dict
    
    else:
        pass

if __name__ == "__main__":

    crypto_tickers = crypto_ticker_list()

    crypto_list = []

    for crypto in crypto_tickers:
        crypto_list.append(crypto_market_data(crypto))
    
    print(crypto_list)

    pd.DataFrame(crypto_list).to_csv("categories.csv")
