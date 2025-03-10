import requests
import pandas as pd
import numpy as np
import macro_asset_func as macro_assets



class IGRequests:
    def __init__(self,login_account,search_markets):
        self.login_account = login_account
        self.search_markets = search_markets
 


url_base = "https://api.ig.com/gateway/deal/session"

headers = {"Content-Type": "application/json; charset=utf-8", 
           "X-IG-API-KEY": "8a0b9aecb9c0cd6163dc4c352c13ddafc6fca71c", 
           "Version": "3"
           }

data = {
        "identifier": "NikhilChandra1234",
        "password": "Xurjxsv5@IG",
        "encryptedPassword": True
    }

response = requests.post(url_base, headers=headers, json=data)
print("Status Code", response.status_code)
response_json = response.json()
oath_token = response_json["oauthToken"]
access_token = oath_token["access_token"]
print("JSON Response ", access_token)


search_url = "https://api.ig.com/gateway/deal/markets?searchTerm="
search_term = "CBA"
search_url_with_term = search_url + search_term

headers = {"Content-Type": "application/json; charset=utf-8", 
           "X-IG-API-KEY": "8a0b9aecb9c0cd6163dc4c352c13ddafc6fca71c", 
           "Version": "1", 
           "Authorization": "Bearer " + access_token, 
           "IG-ACCOUNT-ID": "CT4UQ"
           }

response_get_search_term = requests.get(url=search_url_with_term, headers=headers)

print("Status Code", response_get_search_term.status_code)
print("JSON Response ", response_get_search_term.json())