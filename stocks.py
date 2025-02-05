import os, sys
from datetime import datetime, timedelta
from pathlib import Path

from Getbhavcopy_BSE_Eq_New import download_bse_data
from Final_Bhavcopy_index_2024_NSE import download_nse_and_index_data
from Getbhavcopy_NSE_SME import download_nse_smi_data
from merge_date_wise import merge_data_date_wise
from merge_stock_wise import merge_stock_name_wise
from line_graph import make_line_graph


START_DATE = "2024-01-01"
# RUN after 7PM

Bhavcopy_Download_Folder = "temp1\Data_BSE_temporary"
Final_Bhavcopy_Folder = "temp1\Getbhavcopy_BSE"

if os.path.exists(Bhavcopy_Download_Folder):
    # Remove the folder and its contents
    for root, dirs, files in os.walk(Bhavcopy_Download_Folder, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(Bhavcopy_Download_Folder)

os.makedirs(Bhavcopy_Download_Folder)    

if not os.path.exists(Final_Bhavcopy_Folder):
    os.makedirs(Final_Bhavcopy_Folder)

if os.path.isdir(Final_Bhavcopy_Folder) and os.listdir(Final_Bhavcopy_Folder):

    file_list = [f for f in os.listdir(Final_Bhavcopy_Folder) if os.path.isfile(os.path.join(Final_Bhavcopy_Folder, f))]
    last_date_str = max(f[:10] for f in file_list)
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
    today = datetime.now().date()
    if last_date.date() < today:
        next_date = last_date + timedelta(days=1)
        start_date_str = next_date.strftime('%Y-%m-%d')
        end_date_str = datetime.today().strftime('%Y-%m-%d')
        
    elif last_date.date() == today:
        print(last_date.date(), today)
        print("DataBase is Up To Date Bro.")
        sys.exit()

else:    
    start_date_str = START_DATE
    end_date_str = datetime.today().strftime('%Y-%m-%d')

print(f"Downloading data from {start_date_str} till {end_date_str}")
start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

delta = end_date - start_date
date_range = []
for i in range(delta.days + 1):
    date = start_date + timedelta(days=i)
    if date.weekday() < 5:  # Only include weekdays (Monday-Friday)
        date_range.append(date)


download_bse_data(date_range, Bhavcopy_Download_Folder, Final_Bhavcopy_Folder)


####################################################################################################################################


home_dir = Path("temp2")
Bhavcopy_Download_Folder = home_dir / "Data_NSE_temporary"
index_file_download_folder = home_dir / "Data_NSE_Ind_temporary"
Final_Bhavcopy_Folder = home_dir / "Getbhavcopy_NSE"
Bhavcopy_Download_Folder.mkdir(parents=True, exist_ok=True)
index_file_download_folder.mkdir(parents=True, exist_ok=True)
Final_Bhavcopy_Folder.mkdir(parents=True, exist_ok=True)


if Bhavcopy_Download_Folder.exists():
    # Remove the folder and its contents
    for item in Bhavcopy_Download_Folder.iterdir():
        if item.is_dir():
            for sub_item in item.iterdir():
                sub_item.unlink()
            item.rmdir()
        else:
            item.unlink()
    Bhavcopy_Download_Folder.rmdir()

# Create the folder again
Bhavcopy_Download_Folder.mkdir(parents=True, exist_ok=True)

if index_file_download_folder.exists():
    # Remove the folder and its contents
    for item in index_file_download_folder.iterdir():
        if item.is_dir():
            for sub_item in item.iterdir():
                sub_item.unlink()
            item.rmdir()
        else:
            item.unlink()
    index_file_download_folder.rmdir()

# Create the folder again
index_file_download_folder.mkdir(parents=True, exist_ok=True)
    
# Ensure the final Bhavcopy folder exists
Final_Bhavcopy_Folder.mkdir(parents=True, exist_ok=True)


download_nse_and_index_data(date_range, Bhavcopy_Download_Folder, index_file_download_folder, Final_Bhavcopy_Folder)

######################################################################################################################################

Bhavcopy_Download_Folder = "temp3/Data_NSE_SME_temporary"
if os.path.exists(Bhavcopy_Download_Folder):
    # Remove the folder and its contents
    for root, dirs, files in os.walk(Bhavcopy_Download_Folder, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(Bhavcopy_Download_Folder)

# Create the folder again
os.makedirs(Bhavcopy_Download_Folder)

Final_Bhavcopy_Folder = "temp3/Getbhavcopy_NSE_SME"
if not os.path.exists(Final_Bhavcopy_Folder):
    os.makedirs(Final_Bhavcopy_Folder)


download_nse_smi_data(date_range, Bhavcopy_Download_Folder, Final_Bhavcopy_Folder)

###################################################################################################################################

# will merge data date wise
merge_data_date_wise(start_date_str, end_date_str)
# will merge date data name wise
merge_stock_name_wise(start_date_str, end_date_str)
# Draw graph

import os
try:
    # Specify the directory path
    directory_path = "stocks_by_name"
    # Remove files containing ".csv.csv"
    for filename in os.listdir(directory_path):
        if ".csv.csv" in filename:
            os.remove(os.path.join(directory_path, filename))
except:
    print("No duplicates")

import shutil
shutil.rmtree("stocks_graph")
make_line_graph()