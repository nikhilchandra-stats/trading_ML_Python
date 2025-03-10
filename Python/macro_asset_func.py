import urllib.request
import pandas
import numpy
import bs4
import requests as rq
import urllib
import random
import itertools

def read_macro_data(url = 'https://raw.githubusercontent.com/nikhilchandra-stats/macrodatasetsraw/master/data/daily_fx_macro_data.csv', 
                    encoding_var = 'cp1252'):
    returned_data = pandas.read_csv(url, encoding=encoding_var)
    returned_data['date'] = pandas.to_datetime(returned_data['date'])
    return returned_data

class ASSET:
    def __init__(self, name,country):
        self.name = name
        self.country = country

    def __str__(self):
        return f"Asset:{self.name}, Country:{self.country}"
    
    def ASSET_get_macro(self):
        raw_macro_data = read_macro_data()
        returned_data = raw_macro_data[raw_macro_data['symbol'] == self.country]
        return returned_data
    
    def ASSET_CPI(self):
        raw_macro_data = read_macro_data()
        returned_data = raw_macro_data[raw_macro_data['symbol'] == self.country]
        returned_data = returned_data[returned_data['event'].str.contains('CPI',case=True)]
        return returned_data


AUD_ASSET = ASSET(name="AUDUSD", country="AUD")
AUD_MACRO_DATA = AUD_ASSET.ASSET_CPI()
print(AUD_MACRO_DATA)

# urlx = "https://au.finance.yahoo.com/quote/AUDUSD%3DX/history/"
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}
# print('{}#!{}'.format(urlx, 'meme'))
# r = rq.get('{}#!{}'.format(urlx, 'meme'), headers=headers)
# r.status_code
# html_read = r.text
# soup = bs4.BeautifulSoup(html_read, "html.parser")
# table_find = soup.find_all("table")
# df = pandas.read_html(str(table_find))
# print(len(df))
# print(df)

# url_investing= "https://www.investing.com/"
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"}
# Here the user agent is for Edge browser on windows 10. You can find your browser user agent from the above given link.
# r = rq.get(url=url_investing, headers=headers)

# print(r.status_code)