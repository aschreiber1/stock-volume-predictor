import yfinance as yf
import json
import csv
import os
from datetime import datetime, timedelta, timezone
import concurrent.futures


SYMBOLS_FILE = "symbols.json"
EARNINGS_FILE = "earnings.csv"
COMMON_STOCK = "Common Stock"

with open(SYMBOLS_FILE, "r") as file:
        symbols = json.load(file)

stock_symbols = []
for symbol in symbols:
    if symbol['type'] == COMMON_STOCK and (symbol['mic'] == 'XNAS' or symbol['mic'] == 'XNYS'):
        stock_symbols.append(symbol['symbol'])

processed_symbols = set()
if os.path.exists(EARNINGS_FILE):
    with open(EARNINGS_FILE, "r") as file:
        raw_data = csv.DictReader(file)
        for row in raw_data:
            processed_symbols.add(row['Symbol'])

orig = len(stock_symbols)
stock_symbols = list(filter(lambda x: x not in processed_symbols, stock_symbols))
now = datetime.now().replace(tzinfo=timezone(offset=timedelta()))

print("{} symbols remaining of {}".format(len(stock_symbols), orig))

def get_earnings(symbol):
    #print("Processing Symbol {}".format(symbol))
        #count+=1
        #print("Processing symbol {}, {} of {}".format(symbol, count, num_symbols))
    tkr = yf.Ticker(symbol)
    earnings = tkr.get_earnings_dates(limit=35)
    if earnings is None:
        return
    else:
        print("Found {} row for {}".format(len(earnings), symbol))
        for timestamp, row in earnings.iterrows():
            if timestamp < now: #dont store future earnings
                date = timestamp.strftime("%Y-%m-%d 00:00:00")
                to_add.append([symbol]+[date,row['EPS Estimate'],row['Reported EPS']])

def download_symbols(symbols):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_earnings, symbols)

total_added = 0
step = 100
to_add = []
for i in range(0,len(stock_symbols), step):
    print("Processing chunk {} of {}".format(i//step, len(stock_symbols)//step))
    nxt = stock_symbols[i:i+step]
    download_symbols(nxt)
    with open(EARNINGS_FILE, "a") as file:
        total_added += len(to_add)
        writer = csv.writer(file)
        writer.writerows(to_add)
        print("Wrote {} rows to file".format(len(to_add)))
        to_add = []

print("In total, added {} rows".format(len(to_add)))

# num_symbols = len(stock_symbols)

# count = 0
# with open(EARNINGS_FILE, "a") as file:
#     writer = csv.writer(file)
#     for symbol in stock_symbols:
#         count+=1
#         print("Processing symbol {}, {} of {}".format(symbol, count, num_symbols))
#         tkr = yf.Ticker(symbol)
#         earnings = tkr.get_earnings_dates(limit=35)
#         if earnings is None:
#             print("Ignoring {} as no earnings data retruned".format(symbol))
#             continue
#         for timestamp, row in earnings.iterrows():
#             if timestamp < now: #dont store future earnings
#                 date = timestamp.strftime("%Y-%m-%d 00:00:00")
#                 to_add = [symbol]+[date,row['EPS Estimate'],row['Reported EPS']]
#                 writer.writerow(to_add)