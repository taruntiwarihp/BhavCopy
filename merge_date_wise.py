import warnings

import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
warnings.filterwarnings("default")
"""
SC_CODE   SC_NAME  TRADING_DATE     OPEN     HIGH      LOW    CLOSE  NO_OF_SHRS 
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
TckrSymb   FinInstrmNm    TradDt  OpnPric  HghPric  LwPric  ClsPric  TtlTradgVol
SYMBOL    SECURITY  DATE  OPEN_PRICE  HIGH_PRICE  LOW_PRICE  CLOSE_PRICE  NET_TRDQTY
"""

save_pth = "stock_record"


def merge_nse_and_bse(df):
    # Finding duplicated rows based on 'TckrSymb'
    duplicates = df[df.duplicated(subset='TckrSymb', keep=False)]

    # Processing each duplicate group
    for tckr in duplicates['TckrSymb'].unique():
        group = duplicates[duplicates['TckrSymb'] == tckr]
        if len(group) > 1:
            # Combine 'FinInstrmNm' and sum 'TtlTradgVol'
            combined_names = '@'.join(group['FinInstrmNm'])
            combined_volume = group['TtlTradgVol'].sum()

            # Update the first occurrence
            first_index = group.index[0]
            df.loc[first_index, 'FinInstrmNm'] = combined_names
            df.loc[first_index, 'TtlTradgVol'] = combined_volume

            # Drop all other occurrences
            df = df.drop(group.index[1:])

    return df

def merge_data_date_wise(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    delta = end_date - start_date
    date_range = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        if date.weekday() < 5:  # Only include weekdays (Monday-Friday)
            date_range.append(date)

    for given_date in tqdm(date_range):
        given_date = str(given_date)
        list_dfs = []
        df4_2_lst = []
        year = str(given_date).split("-")[0]
        month = str(given_date).split("-")[1]
        date = str(given_date).split("-")[2].split(" ")[0]

        pth1 = f"temp1/Getbhavcopy_BSE/{year}-{month}-{date}-BSE-EQ.csv"
        pth2 = f"temp2/Getbhavcopy_NSE/{year}-{month}-{date}-NSE-EQ.csv"
        pth3 = f"temp3/Getbhavcopy_NSE_SME/{year}-{month}-{date}-NSE-SME.csv"

        if os.path.exists(pth1):  
            df3 = pd.read_csv(pth1)
            df3 = df3[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
            df3['exchange'] = 'BSE'
            list_dfs.append(df3)

        if os.path.exists(pth2):  
            df4 = pd.read_csv(pth2)
            df4 = df4[['TckrSymb', 'FinInstrmNm', 'TradDt', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol']]
            df4['exchange'] = 'NSE'
            
            split_index = df4[df4.iloc[:, 0] == 'Index Name'].index[0]
            df4_1 = df4.iloc[:split_index]
            # df4_2 = df4.iloc[split_index+1:]
            df4_1.reset_index(drop=True, inplace=True)
            # df4_2.reset_index(drop=True, inplace=True)
            list_dfs.append(df4_1)

            # df4_2.drop('TtlTradgVol', axis=1, inplace=True)
            # df4_2 = df4_2.rename(columns={'TckrSymb': 'IndexName', 'FinInstrmNm': 'TradDt', 
            #                     'TRADING_DATE': 'TradDt', 'TradDt': 'OpnPric', 
            #                     'OpnPric': 'HghPric', 'HghPric': 'LwPric', 
            #                     'LwPric': 'ClsPric', 'ClsPric': 'TtlTradgVol'})
            # df4_2_lst.append(df4_2)
                

        if os.path.exists(pth3):
            df5 = pd.read_csv(pth3)
            df5 = df5.rename(columns={'SYMBOL': 'TckrSymb', 'SECURITY': 'FinInstrmNm', 
                                'DATE': 'TradDt', 'OPEN_PRICE': 'OpnPric', 
                                'HIGH_PRICE': 'HghPric', 'LOW_PRICE': 'LwPric', 
                                'CLOSE_PRICE': 'ClsPric', 'NET_TRDQTY': 'TtlTradgVol'})
                                        
            df5['exchange'] = 'NSE-SME'
            list_dfs.append(df5)

        if list_dfs:
            result = pd.concat(list_dfs, axis=0, ignore_index=True)
            save_n = f"{save_pth}/{year}/"
            os.makedirs(save_n, exist_ok=True)
            save_n = f"{save_pth}/{year}/{year}-{month}-{date}.csv"
            result = merge_nse_and_bse(result)
            result.to_csv(save_n, index=False)
        
        if df4_2_lst:
            save_n = f"{save_pth}/{year}/"
            os.makedirs(save_n, exist_ok=True)
            save_n = f"{save_pth}/{year}/INDEX_{year}-{month}-{date}.csv"
            df4_2.to_csv(save_n, index=False)