import json
import yfinance as yf
import csv
import os
import math

#pull historical stock price data from yahoo finance API

SYMBOLS_FILE = "symbols.json"
COMMON_STOCK = "Common Stock"
HISTORY_FILE = 'history.csv'

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

with open(SYMBOLS_FILE, "r") as file:
        symbols = json.load(file)

processed_symbols = set()
if os.path.exists(HISTORY_FILE):
    with open(HISTORY_FILE, "r") as file:
        raw_data = csv.DictReader(file)
        for row in raw_data:
            processed_symbols.add(row['Symbol'])

stock_symbols = []
for symbol in symbols:
    if symbol['type'] == COMMON_STOCK and (symbol['mic'] == 'XNAS' or symbol['mic'] == 'XNYS'):
        stock_symbols.append(symbol['symbol'])

stock_symbols = list(filter(lambda x: x not in processed_symbols, stock_symbols))

splits = chunks(stock_symbols, 2)
num_splits = len(stock_symbols)//2

with open(HISTORY_FILE, "a") as file:
    writer = csv.writer(file)
    for i, split in enumerate(splits):
        print("Processing Split {} of {}".format(i+1, num_splits))
        symbols_str = " ".join(split)
        try:
            data = yf.download(symbols_str, start="2018-01-01", end="2023-03-17")
        except:
            print('ignoring exception')
        for date,row in data.iterrows():
            for symbol in split:
                if math.isnan(row['Open'][symbol]):
                    continue
                to_add = [symbol]+[date, row['Open'][symbol],row['Close'][symbol],row['High'][symbol],
                                   row['Low'][symbol], row['Volume'][symbol], row['Adj Close'][symbol]]
                writer.writerow(to_add)


