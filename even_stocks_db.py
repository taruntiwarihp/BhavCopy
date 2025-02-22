import pandas as pd


df1 = pd.read_csv("stock_record\\2025\\2025-02-21.csv") # change with latest date datadata
df2 = pd.read_csv("Feb-database - Feb-database.csv")

df2['Screener_Name'] = df2['Screener_Name'].str.strip()
# Check for rows that need to be updated or added in df2
existing_symbols = df2['Exchange_symb'].values
new_rows = []

for index, row in df1.iterrows():
    tckr_symb = row['TckrSymb']
    fin_instrm_nm = row['FinInstrmNm']
    
    if tckr_symb in existing_symbols:
        # Update existing Exchange_Name
        df2.loc[df2['Exchange_symb'] == tckr_symb, 'Exchange_Name'] = fin_instrm_nm
    else:
        # If not in df2, prepare a new row with NaN for Screener_Name and Screener_Link
        new_rows.append({
            'Exchange_symb': tckr_symb,
            'Exchange_Name': fin_instrm_nm,
            'Screener_Name': pd.NA,  # or use np.nan if pd.NA wasn't introduced in your pandas version
            'Screener_Link': pd.NA
        })

# Create a DataFrame for new rows and append them to df2
if new_rows:
    new_rows_df = pd.DataFrame(new_rows)
    df2 = pd.concat([df2, new_rows_df], ignore_index=True)


is_unique = len(df2['Exchange_symb']) == df2['Exchange_symb'].nunique()
print(is_unique)


df2['combined_text'] = None

for i, rows in df2.iterrows():
    if pd.notna(rows['Screener_Link']) and rows['Screener_Link'] != '':
        link = rows['Screener_Link'].replace("https://www.screener.in/", "")
        link = link.replace("company", "").replace("consolidated", "").replace("/", "")
        df2.at[i, 'combined_text'] = f"{rows['Exchange_symb']} {rows['Exchange_Name']} {rows['Screener_Name']} {link}"
    
    
df2['combined_text'] = df2['combined_text'].fillna('')  
df2.to_csv("database.csv", index=False)
