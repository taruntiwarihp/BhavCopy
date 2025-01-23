# import pandas as pd
# from glob import glob

# paths = glob("stocks_by_name\*.csv")
# data = []
# for i in paths:
#     i = i.split("\\")[-1].replace(".csv", "")
#     i = i.split("@")
#     data.append(i)

# print(data[:10])
# df = pd.DataFrame(data, columns=['Exchange_symb', 'Exchange_Name'])


# df.to_csv("annotation_data.csv", index=False)

import pandas as pd

df = pd.read_csv("annotation_data.csv")
old_df = pd.read_csv("bhav (2) - bhav (2).csv.csv")
# for i, val in old_df.iterrows():

#     value_1 = val['stock_syb']
#     value_2 = val['stock_name']
#     print(value_1, value_2)
#     df.loc[df['Exchange_symb'] == value_1, 'Screener_Name'] = old_df[old_df['stock_syb'] == value_1]['screener_name']
#     df.loc[df['Exchange_symb'] == value_1, 'Screener_Link'] = old_df[old_df['stock_syb'] == value_1]['screener_url']
#     break

# Create mapping dictionaries
screener_name_map = old_df.set_index('stock_syb')['screener_name'].to_dict()
screener_url_map = old_df.set_index('stock_syb')['screener_url'].to_dict()

# Update values using map
df['Screener_Name'] = df['Exchange_symb'].map(screener_name_map)
df['Screener_Link'] = df['Exchange_symb'].map(screener_url_map)

df.to_csv("updated.csv", index=False)