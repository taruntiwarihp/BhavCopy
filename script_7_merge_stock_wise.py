import os
from glob import glob
import pandas as pd

start_date = "2025-01-10"
end_date = "2025-01-17"
today_date_str = "2025-01-17"
# MERGE DATA
year = today_date_str.split("\\")[-1].split("-")[0]
month = today_date_str.split("\\")[-1].split("-")[1]
date = today_date_str.split("\\")[-1].split("-")[2]

path = f"stock_record\\{year}\\{year}-{month}-{date}.csv"
df = pd.read_csv(path)

list_target_ticker = df.apply(lambda row: f"{row['TckrSymb']}_{row['FinInstrmNm']}".replace(" ", ""), axis=1).tolist()
csv_files = glob("stock_record\\2025\\*.csv")
csv_files = [s for s in csv_files if not any(index in s for index in "/INDEX")]

csv_files = [f for f in csv_files if start_date <= f.split('\\')[-1][:10] <= end_date]

os.makedirs('stocks_by_name', exist_ok=True)

for target_ticker in list_target_ticker:

    fname = f"stocks_by_name\\{target_ticker}.csv"
    flag = os.path.isfile(fname)
    merged_data = []
    
    if flag:
        org_stock = pd.read_csv(fname)
        
    for file in csv_files:
        df = pd.read_csv(file)
        temp = target_ticker.split("_")[0]
        filtered_data = df[df['TckrSymb'] == temp]
        if not filtered_data.empty:
            merged_data.append(filtered_data)

    if merged_data:
        merged_data = pd.concat(merged_data, ignore_index=True)
        try:
            merged_data.drop(columns=['TckrSymb', 'Unnamed: 34'], inplace=True)
        except:
            print(merged_data.columns)
            pass

        merged_data = merged_data[['TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
        merged_data.columns = ['TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']


        target_ticker = target_ticker.replace("/", "_").replace(":", "_")

        if flag:
            merged_data = pd.concat([org_stock, merged_data], ignore_index=True)

        merged_data.to_csv(f'stocks_by_name/{target_ticker}.csv', index=False)
