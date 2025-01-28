import os
import socket
import requests
import pandas as pd
from datetime import datetime, timedelta
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def Download_BSE_Bhavcopy_File(BSE_Bhavcopy_URL, Bhavcopy_Download_Folder):
    # Download a file from the given URL to the specified output folder
    filename = os.path.basename(BSE_Bhavcopy_URL)
    output_path = os.path.join(Bhavcopy_Download_Folder, filename)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        with requests.get(BSE_Bhavcopy_URL, stream=True, headers=headers, verify=False) as response:
            response.raise_for_status()  # Check if the request was successful
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        
        print(f'Eq-bhavcopy {filename} downloaded.')
        return output_path
    
    except requests.exceptions.RequestException as e:
        print("Bhavcopy file not available to download on BSE Server"  )
        return None
    
def Rename_BSE_Bhavcopy_Files(directory):
    # Rename files in the specified directory to the desired format
    files = os.listdir(directory)
    renamed_files = []
    for file in files:
        if file.endswith("0000.CSV"):
            # Extract the date from the file name
            date = file[22:30]
            new_name = date + ".csv"
            new_name = new_name.replace("BhavCopy_BSE_CM_0_0_0_", "").replace("_F_0000", "")
            date_obj = datetime.strptime(date, "%Y%m%d")
            new_name = date_obj.strftime("%Y-%m-%d") + "-BSE-EQ.csv"

            # Rename the file
            old_path = os.path.join(directory, file)
            new_path = os.path.join(directory, new_name)
            os.rename(old_path, new_path)
            renamed_files.append((file, new_name))
        else:
            print(f"File '{file}' does not end with 'bhav.csv' and was not renamed.")
    if renamed_files:
        for old_name, new_name in renamed_files:
           print(f"File '{old_name}' renamed to: '{new_name}'")
    else:
        print("No files found or no files meet the desired condition.")
        
def Modify_BSE_Bhavcopy_Files(directory):
    # Modify the renamed files in the specified directory
    files = os.listdir(directory)
    for file in files:
        if file.endswith("-BSE-EQ.csv"):
            file_path = os.path.join(directory, file)
            df = pd.read_csv(file_path)

            try:
                # Read the CSV file
                
                # Remove the specified columns
                columns_to_remove = ['BizDt',
                                     'Sgmt',
                                     'Src',
                                     'FinInstrmTp',
                                     'FinInstrmId',
                                     'ISIN','XpryDt',
                                     'FininstrmActlXpryDt',	
                                     'StrkPric',
                                     'OptnTp',
                                    #  'FinInstrmNm',
                                     'LastPric',
                                     'PrvsClsgPric',
                                     'UndrlygPric',
                                     'SttlmPric',
                                     'OpnIntrst',
                                     'ChngInOpnIntrst',
                                     'TtlTrfVal',
                                     'TtlNbOfTxsExctd',
                                     'SsnId',
                                     'NewBrdLotQty',
                                     'Rmks',
                                     'Rsvd1',
                                     'Rsvd2',
                                     'Rsvd3',
                                     'Rsvd4']
                
                df = df.drop(columns=columns_to_remove)
                                
                Mutual_Fund_symbol_to_remove = [
                    'SENSEX1', 'SBISENSEX', 'CPSEETF', 'SENSEXBEES', 'SETFBSE100', 'UTISENSETF', 
                    'HDFCSENSEX', 'BSLSENETFG', 'IDFSENSEXE', 'ICICIB22', 'BSE500IETF', 'SETFSN50', 
                    'UTISXN50', 'SNXT50BEES', 'SENSEXADD', 'SENSEXETF', 'SENSEXIETF', 'MOM100', 
                    'NIFTYIETF', 'NIF100IETF', 'NIF100BEES', 'NIFTY1', 'UTINIFTETF', 'LICNETFN50', 
                    'HDFCNIFTY', 'LICNFNHGP', 'NV20IETF', 'MIDSELIETF', 'LOWVOLIETF', 'UTINEXT50', 
                    'NEXT50IETF', 'NIFTYETF', 'ABSLNN50ET', 'BANKIETF', 'PVTBANIETF', 'NIESSPJ', 
                    'NIESSPC', 'NIEHSPI', 'NIESSPE', 'NIEHSPD', 'NIEHSPE', 'NIESSPK', 'NIEHSPG', 
                    'NIEHSPH', 'NIEHSPL', 'NIESSPL', 'NIESSPM', 'ABSLBANETF', 'MIDCAPIETF', 'NEXT50', 
                    '08MPD', '08GPG', '11DPR', '11GPG', '11MPD', '11MPR', '11QPD', '11AGG', '11AMD', 
                    '11DPD', 'ALPL30IETF', 'ITIETF', 'HDFCNIFBAN', 'UTIBANKETF', 'ESG', 'INFRABEES', 
                    'MAFANG', 'HEALTHIETF', 'BFSI', 'FMCGIETF', 'AXISTECETF', 'AXISHCETF', 'AXISCETF', 
                    'MASPTOP50', 'CONSUMIETF', 'EQUAL50ADD', 'MAHKTECH', 'MONQ50', 'MIDQ50ADD', 
                    'NIFTY50ADD', 'AUTOIETF', 'MAKEINDIA', 'MOMOMENTUM', 'TECH', 'HEALTHY', 'BSLNIFTY', 
                    'MIDCAPETF', 'MOLOWVOL', 'MOHEALTH', 'MOM30IETF', 'HDFCNIF100', 'HDFCNEXT50', 
                    'INFRAIETF', 'NIFTYQLITY', 'MOMENTUM', 'MOVALUE', 'MOQUALITY', 'HDFCQUAL', 
                    'HDFCGROWTH', 'HDFCVALUE', 'HDFCLOWVOL', 'HDFCMOMENT', 'HDFCNIFIT', 'HDFCPVTBAN', 
                    'FINIETF', 'COMMOIETF', 'BANKETFADD', 'HDFCBSE500', 'HDFCSML250', 'HDFCMID150', 
                    'PSUBNKIETF', 'AXSENSEX', 'LOWVOL', 'ITETFADD', 'BANKETF', 'PSUBANKADD', 
                    'PVTBANKADD', 'QUAL30IETF', 'NIFMID150', 'NAVINIFTY', 'ITETF', 'ALPHAETF', 
                    'NIFTYBETF', 'BANKBETF', 'NIFITETF', 'HDFCPSUBK', 'LICNMID100', 'SMALLCAP', 
                    'MIDSMALL', 'MID150CASE', 'TOP100CASE', 'BBNPNBETF', 'EVINDIA', 'SBINEQWETF', 
                    'OILIETF', 'ABSLPSE', 'NIFTYBEES', 'JUNIORBEES', 'BANKBEES', 'PSUBANK', 
                    'PSUBNKBEES', 'SHARIABEES', 'QNIFTY', 'MOM50', 'BANKNIFTY1', 'SETFNIFBK', 
                    'SETFNIF50'
                                    ]

                # Filter out rows where 'TckrSymb' is in the list of values to remove
                df = df[~df['TckrSymb'].isin(Mutual_Fund_symbol_to_remove)]

                # Filter rows based on SERIES column
                df = df[df['SctySrs'].isin(['A', 'B', 'M',"MS","MT","P","R","T","W","X","XT","Z","ZP"])]  
                                
                # Convert the 'TradDt' column to datetime by inferring the format
                df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce')

                # Check if there are any NaT (Not a Time) values which indicate conversion issues
                if df['TradDt'].isna().any():
                    print("There are some dates that couldn't be converted:")
                    print(df[df['TradDt'].isna()])

                # Convert the datetime object to the desired string format
                df['TradDt'] = df['TradDt'].dt.strftime('%Y%m%d')              
                               
                # Reorder the columns
                cols = df.columns.tolist()
                cols.remove('TradDt')
                cols.insert(cols.index('OpnPric'), 'TradDt')
                df = df[cols]

                # Remove the SctySrs column
                df = df.drop(columns=['SctySrs'])
                
                # Sort the DataFrame by the 'TckrSymb' column
                df = df.sort_values(by='TckrSymb')

                # Save the modified dataframe back to CSV
                df.to_csv(file_path, index=False)

                print(f"{file} :Eq_Bhavcopy Data Structure converted to getbhavcopy")

            except:
                # Remove the specified columns
                columns_to_remove = ['BizDt',
                                     'Sgmt',
                                     'Src',
                                     'FinInstrmTp',
                                     'FinInstrmId',
                                     'ISIN','XpryDt',
                                     'FininstrmActlXpryDt',	
                                     'StrkPric',
                                     'OptnTp',
                                    #  'FinInstrmNm',
                                     'LastPric',
                                     'PrvsClsgPric',
                                     'UndrlygPric',
                                     'SttlmPric',
                                     'OpnIntrst',
                                     'ChngInOpnIntrst',
                                     'TtlTrfVal',
                                     'TtlNbOfTxsExctd',
                                     'SsnId',
                                     'NewBrdLotQty',
                                     'Rmks',
                                     'Rsvd01',
                                     'Rsvd02',
                                     'Rsvd03',
                                     'Rsvd04']
                
                df = df.drop(columns=columns_to_remove)
                                
                Mutual_Fund_symbol_to_remove = [
                    'SENSEX1', 'SBISENSEX', 'CPSEETF', 'SENSEXBEES', 'SETFBSE100', 'UTISENSETF', 
                    'HDFCSENSEX', 'BSLSENETFG', 'IDFSENSEXE', 'ICICIB22', 'BSE500IETF', 'SETFSN50', 
                    'UTISXN50', 'SNXT50BEES', 'SENSEXADD', 'SENSEXETF', 'SENSEXIETF', 'MOM100', 
                    'NIFTYIETF', 'NIF100IETF', 'NIF100BEES', 'NIFTY1', 'UTINIFTETF', 'LICNETFN50', 
                    'HDFCNIFTY', 'LICNFNHGP', 'NV20IETF', 'MIDSELIETF', 'LOWVOLIETF', 'UTINEXT50', 
                    'NEXT50IETF', 'NIFTYETF', 'ABSLNN50ET', 'BANKIETF', 'PVTBANIETF', 'NIESSPJ', 
                    'NIESSPC', 'NIEHSPI', 'NIESSPE', 'NIEHSPD', 'NIEHSPE', 'NIESSPK', 'NIEHSPG', 
                    'NIEHSPH', 'NIEHSPL', 'NIESSPL', 'NIESSPM', 'ABSLBANETF', 'MIDCAPIETF', 'NEXT50', 
                    '08MPD', '08GPG', '11DPR', '11GPG', '11MPD', '11MPR', '11QPD', '11AGG', '11AMD', 
                    '11DPD', 'ALPL30IETF', 'ITIETF', 'HDFCNIFBAN', 'UTIBANKETF', 'ESG', 'INFRABEES', 
                    'MAFANG', 'HEALTHIETF', 'BFSI', 'FMCGIETF', 'AXISTECETF', 'AXISHCETF', 'AXISCETF', 
                    'MASPTOP50', 'CONSUMIETF', 'EQUAL50ADD', 'MAHKTECH', 'MONQ50', 'MIDQ50ADD', 
                    'NIFTY50ADD', 'AUTOIETF', 'MAKEINDIA', 'MOMOMENTUM', 'TECH', 'HEALTHY', 'BSLNIFTY', 
                    'MIDCAPETF', 'MOLOWVOL', 'MOHEALTH', 'MOM30IETF', 'HDFCNIF100', 'HDFCNEXT50', 
                    'INFRAIETF', 'NIFTYQLITY', 'MOMENTUM', 'MOVALUE', 'MOQUALITY', 'HDFCQUAL', 
                    'HDFCGROWTH', 'HDFCVALUE', 'HDFCLOWVOL', 'HDFCMOMENT', 'HDFCNIFIT', 'HDFCPVTBAN', 
                    'FINIETF', 'COMMOIETF', 'BANKETFADD', 'HDFCBSE500', 'HDFCSML250', 'HDFCMID150', 
                    'PSUBNKIETF', 'AXSENSEX', 'LOWVOL', 'ITETFADD', 'BANKETF', 'PSUBANKADD', 
                    'PVTBANKADD', 'QUAL30IETF', 'NIFMID150', 'NAVINIFTY', 'ITETF', 'ALPHAETF', 
                    'NIFTYBETF', 'BANKBETF', 'NIFITETF', 'HDFCPSUBK', 'LICNMID100', 'SMALLCAP', 
                    'MIDSMALL', 'MID150CASE', 'TOP100CASE', 'BBNPNBETF', 'EVINDIA', 'SBINEQWETF', 
                    'OILIETF', 'ABSLPSE', 'NIFTYBEES', 'JUNIORBEES', 'BANKBEES', 'PSUBANK', 
                    'PSUBNKBEES', 'SHARIABEES', 'QNIFTY', 'MOM50', 'BANKNIFTY1', 'SETFNIFBK', 
                    'SETFNIF50'
                                    ]

                # Filter out rows where 'TckrSymb' is in the list of values to remove
                df = df[~df['TckrSymb'].isin(Mutual_Fund_symbol_to_remove)]

                # Filter rows based on SERIES column
                df = df[df['SctySrs'].isin(['A', 'B', 'M',"MS","MT","P","R","T","W","X","XT","Z","ZP"])]  
                                
                # Convert the 'TradDt' column to datetime by inferring the format
                df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce')

                # Check if there are any NaT (Not a Time) values which indicate conversion issues
                if df['TradDt'].isna().any():
                    print("There are some dates that couldn't be converted:")
                    print(df[df['TradDt'].isna()])

                # Convert the datetime object to the desired string format
                df['TradDt'] = df['TradDt'].dt.strftime('%Y%m%d')              
                               
                # Reorder the columns
                cols = df.columns.tolist()
                cols.remove('TradDt')
                cols.insert(cols.index('OpnPric'), 'TradDt')
                df = df[cols]

                # Remove the SctySrs column
                df = df.drop(columns=['SctySrs'])
                
                # Sort the DataFrame by the 'TckrSymb' column
                df = df.sort_values(by='TckrSymb')

                # Save the modified dataframe back to CSV
                df.to_csv(file_path, index=False)

                print(f"{file} :Eq_Bhavcopy Data Structure converted to getbhavcopy")
       
def copy_and_remove_files(Bhavcopy_Download_Folder, Final_Bhavcopy_Folder, remove_folders):
    
    # Copy files to destination folder
    for filename in os.listdir(Bhavcopy_Download_Folder):
        if filename.endswith("-BSE-EQ.csv"):
            source_filepath = os.path.join(Bhavcopy_Download_Folder, filename)
            destination_filepath = os.path.join(Final_Bhavcopy_Folder, filename)
            with open(source_filepath, 'rb') as source_file:
                with open(destination_filepath, 'wb') as destination_file:
                    destination_file.write(source_file.read())
            print(f"Bhavcopy file: {filename} copied to {Final_Bhavcopy_Folder}")

    # Remove source folders
    for folder in remove_folders:
        for root, dirs, files in os.walk(folder, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(folder)
      
def download_bse_data(date_range, Bhavcopy_Download_Folder, Final_Bhavcopy_Folder):
    socket.setdefaulttimeout(3)

    downloaded_files = []

    for date in date_range:
        # Construct the URL for downloading the file 
        BSE_Bhavcopy_URL = "https://www.bseindia.com/download/BhavCopy/Equity"
        date_str = date.strftime("%Y%m%d")
        filename = f"BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV"
        BSE_Bhavcopy_URL = f"{BSE_Bhavcopy_URL}/{filename}"
        formatted_date = date.strftime('%Y-%m-%d')

        try:
            # Download the file and add it to the list of downloaded files
            Downloaded_Bhavcopy_file = Download_BSE_Bhavcopy_File(BSE_Bhavcopy_URL, Bhavcopy_Download_Folder)
            if Downloaded_Bhavcopy_file:
                downloaded_files.append(Downloaded_Bhavcopy_file)
        except Exception as e:
            print(formatted_date, "Bhavcopy file not available to download on BSE Server")
            print(e)
            
    Rename_BSE_Bhavcopy_Files(Bhavcopy_Download_Folder)
    Modify_BSE_Bhavcopy_Files(Bhavcopy_Download_Folder)
    remove_folders = [Bhavcopy_Download_Folder]
    copy_and_remove_files(Bhavcopy_Download_Folder, Final_Bhavcopy_Folder, remove_folders)

