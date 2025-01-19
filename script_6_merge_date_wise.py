import os
import pandas as pd
from glob import glob
from datetime import datetime, timedelta


"""
SC_CODE   SC_NAME  TRADING_DATE     OPEN     HIGH      LOW    CLOSE  NO_OF_SHRS 
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
SYMBOL    SECURITY  DATE  OPEN_PRICE  HIGH_PRICE  LOW_PRICE  CLOSE_PRICE  NET_TRDQTY
"""


start_date_str = "2025-01-10"
end_date_str = "2025-01-19"
start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

delta = end_date - start_date
date_range = []
for i in range(delta.days + 1):
    date = start_date + timedelta(days=i)
    if date.weekday() < 5:  # Only include weekdays (Monday-Friday)
        date_range.append(date)



all_paths = glob("temp*/*/*.csv")


for given_date in date_range:
    given_date = str(given_date)
    list_dfs = []
    df4_2_lst = []
    year = str(given_date).split("-")[0]
    month = str(given_date).split("-")[1]
    date = str(given_date).split("-")[2].split(" ")[0]

    # pth1 = f"temp/data_BSE/{year}-{month}-{date}-BSE-EQ.csv"
    # pth2 = f"temp1/data_BSE/{year}-{month}-{date}-BSE-EQ.csv"
    pth3 = f"temp2/Data_BSE_temporary/{year}-{month}-{date}-BSE-EQ.csv"
    pth4 = f"temp3/Data_NSE_temporary/{year}-{month}-{date}-NSE-EQ.csv"
    pth5 = f"temp4/Data_NSE_SME_temporary/{year}-{month}-{date}-NSE-SME.csv"
    
    # if os.path.exists(pth1):
    #     df1 = pd.read_csv(pth1)

    #     if "TRADING_DATE" not in df1.columns:
    #         df1['TRADING_DATE'] = f'{year}{month}{date}'

    #     df1 = df1.rename(columns={'SC_CODE': 'TckrSymb', 'SC_NAME': 'FinInstrmNm', 
    #                         'TRADING_DATE': 'TradDt', 'OPEN': 'OpnPric', 
    #                         'HIGH': 'HghPric', 'LOW': 'LwPric', 
    #                         'CLOSE': 'ClsPric', 'NO_OF_SHRS': 'TtlTradgVol'})
            
    #     df1 = df1[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
    #     df1['exchange'] = 'BSE'
    #     list_dfs.append(df1)

    # if os.path.exists(pth2):  
    #     df2 = pd.read_csv(pth2)
    #     df2 = df2[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
    #     df2['exchange'] = 'BSE'
    #     list_dfs.append(df2)

    if os.path.exists(pth3):  
        df3 = pd.read_csv(pth3)
        df3 = df3[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
        df3['exchange'] = 'BSE'
        list_dfs.append(df3)

    if os.path.exists(pth4):  
        df4 = pd.read_csv(pth4)
        df4 = df4[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
        df4['exchange'] = 'NSE'
        
        split_index = df4[df4.iloc[:, 0] == 'Index Name'].index[0]
        df4_1 = df4.iloc[:split_index]
        df4_2 = df4.iloc[split_index+1:]
        df4_1.reset_index(drop=True, inplace=True)
        df4_2.reset_index(drop=True, inplace=True)
        list_dfs.append(df4_1)

        df4_2.drop('TtlTradgVol', axis=1, inplace=True)
        df4_2 = df4_2.rename(columns={'TckrSymb': 'IndexName', 'FinInstrmNm': 'TradDt', 
                            'TRADING_DATE': 'TradDt', 'TradDt': 'OpnPric', 
                            'OpnPric': 'HghPric', 'HghPric': 'LwPric', 
                            'LwPric': 'ClsPric', 'ClsPric': 'TtlTradgVol'})
        df4_2_lst.append(df4_2)
            

    if os.path.exists(pth5):
        df5 = pd.read_csv(pth5)
        df5 = df5.rename(columns={'SYMBOL': 'TckrSymb', 'SECURITY': 'FinInstrmNm', 
                            'DATE': 'TradDt', 'OPEN_PRICE': 'OpnPric', 
                            'HIGH_PRICE': 'HghPric', 'LOW_PRICE': 'LwPric', 
                            'CLOSE_PRICE': 'ClsPric', 'NET_TRDQTY': 'TtlTradgVol'})
                                    
        df5['exchange'] = 'NSE-SME'
        list_dfs.append(df5)

    print(len(list_dfs))
    if list_dfs:
        result = pd.concat(list_dfs, axis=0, ignore_index=True)
        save_n = f"stock_record/{year}/"
        os.makedirs(save_n, exist_ok=True)
        save_n = f"stock_record/{year}/{year}-{month}-{date}.csv"
        result.to_csv(save_n, index=False)
    
    if df4_2_lst:
        save_n = f"stock_record/{year}/"
        os.makedirs(save_n, exist_ok=True)
        save_n = f"stock_record/{year}/INDEX_{year}-{month}-{date}.csv"
        df4_2.to_csv(save_n, index=False)