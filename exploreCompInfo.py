import os
import csv
import pandas as pd

COMPANY_INFO_FILE = "comp_info.csv"

company_info = {}
with open(COMPANY_INFO_FILE, "r") as file:
    raw_data = csv.DictReader(file)
    for row in raw_data:
        company_info[row['symbol']] = row
df = pd.DataFrame(company_info.values())
print(df['finnhubIndustry'].value_counts())