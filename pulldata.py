import finnhub
import json 
import os
import pandas as pd
import yfinance as yf
import csv
import time

# Pull Symbol data from finnhub

SYMBOLS_FILE = "symbols.json"
COMPANY_INFO_FILE = "comp_info.csv"
COMMON_STOCK = "Common Stock"

finnhub_client = finnhub.Client(api_key="cgatfp1r01ql0m8rf36gcgatfp1r01ql0m8rf370")

#save symbols to cache to avoid reading multiple times
if os.path.exists(SYMBOLS_FILE):
    with open(SYMBOLS_FILE, "r") as file:
        symbols = json.load(file)
else:
    symbols = finnhub_client.stock_symbols('US')
    # Serializing json
    json_object = json.dumps(symbols, indent=4)
    # Writing to sample.json
    with open(SYMBOLS_FILE, "w") as outfile:
        outfile.write(json_object)
    
#only look at common stocks traded on nasdaq and nyse
stock_symbols = []
for symbol in symbols:
    if symbol['type'] == COMMON_STOCK and (symbol['mic'] == 'XNAS' or symbol['mic'] == 'XNYS'):
        stock_symbols.append(symbol['symbol'])

company_info = {}
if os.path.exists(COMPANY_INFO_FILE):
    with open(COMPANY_INFO_FILE, "r") as file:
        raw_data = csv.DictReader(file)
        for row in raw_data:
            company_info[row['symbol']] = raw_data

with open(COMPANY_INFO_FILE, "a") as file:
    writer = csv.writer(file)
    count = 0
    for symbol in stock_symbols:
        if symbol.endswith(".V"):
            print("Skipping symbol {} as it ends in .V".format(symbol))
            continue
        if symbol not in company_info:
            print('Loading company info for {}'.format(symbol))
            profile = finnhub_client.company_profile2(symbol=symbol)
            if len(profile):
                to_add = [symbol]+[profile['country'],profile['currency'],profile['exchange'],
                                profile['finnhubIndustry'], profile['marketCapitalization'],profile['name'],
                                profile['shareOutstanding'],profile['ticker']]
            else:
                print('Adding Null data for {}'.format(symbol))
                to_add = [symbol]+[None]*8
            writer.writerow(to_add)
            count += 1
        if count == 60:
            print("Sleeping 60 seconds to avoid rate limit")
            time.sleep(60)
            count = 0