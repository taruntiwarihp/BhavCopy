
import os
import sys
import socket
import pandas as pd
import urllib.request
from datetime import datetime, timedelta

def Download_NSE_Bhavcopy_File(NSE_Bhavcopy_URL, Bhavcopy_Download_Folder):
    # Download a file from the given URL to the specified output folder
    filename = os.path.basename(NSE_Bhavcopy_URL)
    output_path = os.path.join(Bhavcopy_Download_Folder, filename)
    urllib.request.urlretrieve(NSE_Bhavcopy_URL, output_path)
    print(f'Eq-bhavcopy {filename} downloaded.')
    return output_path

def Rename_NSE_Bhavcopy_Files(directory):
    # Rename files in the specified directory to the desired format
    files = os.listdir(directory)
    renamed_files = []
    for file in files:
        if file.endswith(".csv"):
            # Extract the date from the file name
            date = file[3:9]
            new_name = date + ".csv"
            new_name = new_name.replace("sme", "")
            date_obj = datetime.strptime(date, "%d%m%y")
            new_name = date_obj.strftime("%Y-%m-%d") + "-NSE-SME.csv"

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
      
def add_date_column_to_csv(directory):
    # Process each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            # Extract the date from the filename (assuming the format is 'YYYY-MM-DD-NSE-SME.csv')
            date_part = filename.split('-NSE-SME.csv')[0]
            
            # Convert date to yyyymmdd format
            date_column_value = date_part.replace("-", "")
            
            # Load the CSV file into a DataFrame
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)
            
            # Add a new column with the extracted date
            df['DATE'] = date_column_value
            
            # Reorder the columns
            cols = df.columns.tolist()
            cols.remove('DATE')
            cols.insert(cols.index('OPEN_PRICE'), 'DATE')
            df = df[cols]
            
            # Save the modified DataFrame back to the CSV file
            df.to_csv(file_path, index=False)
            
            print(f"{filename} : Date column added")
        
def Modify_NSE_Bhavcopy_Files(directory):
    # Modify the renamed files in the specified directory
    files = os.listdir(directory)
    for file in files:
        if file.endswith("-NSE-SME.csv"):
            file_path = os.path.join(directory, file)
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)

                # Remove the specified columns
                columns_to_remove = ["MARKET",
                                     "SERIES",
                                     #"SYMBOL",
                                    #  "SECURITY",
                                     "PREV_CL_PR",
                                     #"OPEN_PRICE",
                                     #"HIGH_PRICE",
                                     #"LOW_PRICE",
                                     #"CLOSE_PRICE",
                                     "NET_TRDVAL",
                                     #"NET_TRDQTY",
                                     "CORP_IND",
                                     "HI_52_WK",
                                     "LO_52_WK"]
                
                df = df.drop(columns=columns_to_remove)
                
                
                # Save the modified dataframe back to CSV
                df.to_csv(file_path, index=False)

                print(f"{file} : Data Structure converted to getbhavcopy")
            except FileNotFoundError:
                print(f"File not found: {file}")      
                              
def change_file_extension(Bhavcopy_Download_Folder, old_extension, new_extension):
    
    # Iterate through all files in the folder
    for filename in os.listdir(Bhavcopy_Download_Folder):
        # Check if the file ends with the old extension
        if filename.endswith("-NSE-SME.csv"):
            # Split the filename into name and extension, and replace the extension with the new extension
            new_filename = os.path.splitext(filename)[0] + ".txt"
            
            # Construct the full path of the old file
            old_filepath = os.path.join(Bhavcopy_Download_Folder, filename)
            
            # Construct the full path of the new file
            new_filepath = os.path.join(Bhavcopy_Download_Folder, new_filename)
            
            # Rename the old file to the new file
            os.rename(old_filepath, new_filepath)
            
            # Print a message to confirm the file extension change
            #print(f"Changed Bhavcopy extension: {filename} to {new_filename}")
            
            
def copy_and_remove_files(Bhavcopy_Download_Folder, Final_Bhavcopy_Folder, remove_folders):
    
    # Copy files to destination folder
    for filename in os.listdir(Bhavcopy_Download_Folder):
        if filename.endswith("-NSE-SME.csv"):
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


def download_nse_smi_data(date_range, Bhavcopy_Download_Folder, Final_Bhavcopy_Folder):

    socket.setdefaulttimeout(3)
    downloaded_files = []

    for date in date_range:
        #https://archives.nseindia.com/archives/sme/bhavcopy/sme230824.csv
        NSE_Bhavcopy_URL = "https://archives.nseindia.com/archives/sme/bhavcopy"
        date_str = date.strftime("%d%m%y")
        filename = f"sme{date_str}.csv"
        NSE_Bhavcopy_URL = "{}/{}".format(NSE_Bhavcopy_URL, filename)
        date = date.strftime('%d-%m-%Y')
        #print("URL:", NSE_Bhavcopy_URL)
        try:
            # Download the file and add it to the list of downloaded files
            Downloaded_Bhavcopy_file = Download_NSE_Bhavcopy_File(NSE_Bhavcopy_URL, Bhavcopy_Download_Folder)
            downloaded_files.append(Downloaded_Bhavcopy_file)
        except Exception as e:
            print(date,"NSE_SME Bhavcopy  file not availabel to Download on NSE Server")
            #print(e)
            
    Rename_NSE_Bhavcopy_Files(Bhavcopy_Download_Folder)
    add_date_column_to_csv(Bhavcopy_Download_Folder)
    Modify_NSE_Bhavcopy_Files(Bhavcopy_Download_Folder)
    remove_folders = [Bhavcopy_Download_Folder]
    copy_and_remove_files(Bhavcopy_Download_Folder, Final_Bhavcopy_Folder, remove_folders)

        
