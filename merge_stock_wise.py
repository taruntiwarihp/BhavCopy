import os, re
from glob import glob
import pandas as pd
from tqdm import tqdm
from datetime import datetime

pattern = re.compile(r"(\d{4})-(\d{2})-(\d{2}).csv$")

def merge_stock_name_wise(start_date_str, end_date_str):

    global pattern
    year = end_date_str.split("\\")[-1].split("-")[0]
    
    file_paths = glob(f"stock_record\\{year}\\*")
    path = max(file_paths,
                key=lambda f: datetime(*map(int, pattern.search(f).groups())))

    try:
        df = pd.read_csv(path)

        list_target_ticker = df.apply(lambda row: f"{row['TckrSymb']}@{row['FinInstrmNm']}".replace(" ", "").replace(".", ""), axis=1).tolist()
        list_target_ticker = [s if '(' in s and ')' in s else s.replace('(', '').replace(')', '') for s in list_target_ticker]

        csv_files = glob("stock_record\\*\\*.csv")
        # csv_files = [s for s in csv_files if not any(index in s for index in "/INDEX")]

        csv_files = [f for f in csv_files if start_date_str <= f.split('\\')[-1][:10] <= end_date_str]

        os.makedirs('stocks_by_name', exist_ok=True)
        if csv_files:
            for target_ticker in tqdm(list_target_ticker, desc="Processing", colour='cyan'):
                
                files = os.listdir("stocks_by_name/")

                pattern = target_ticker.split("@")[0]
                pattern = f"{pattern}@"
                fname = [f for f in files if f.startswith(pattern)]
                
                merged_data = []
                if fname:
                    org_stock = pd.read_csv(f"stocks_by_name/{fname[0]}")
                    
                for file in csv_files:
                    df = pd.read_csv(file)
                    temp = target_ticker.split("@")[0]
                    filtered_data = df[df['TckrSymb'] == temp]
                    if not filtered_data.empty:
                        merged_data.append(filtered_data)

                if merged_data:
                    merged_data = pd.concat(merged_data, ignore_index=True)
                    try:
                        merged_data.drop(columns=['TckrSymb', 'Unnamed: 34'], inplace=True)
                    except:
                        pass

                    merged_data = merged_data[['TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol', 'exchange']]
                    merged_data.columns = ['TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol', 'exchange']

                    target_ticker = target_ticker.replace("/", "_").replace(":", "_")

                    if fname:
                        merged_data = pd.concat([org_stock, merged_data], ignore_index=True)
                        merged_data.to_csv(f'stocks_by_name/{fname[0]}', index=False)
                    else:
                        merged_data.to_csv(f'stocks_by_name/{target_ticker}.csv', index=False)

    except:
        print("ERROR, FILE NOT FOUND", path)