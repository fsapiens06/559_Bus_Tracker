# This program fetchs historical bus location data from the TDX website.

from api import app_id, app_key

from tqdm import tqdm
import csv
import requests
from pprint import pprint
import json
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
import os
from memory_profiler import profile
import gc

gc.enable()

start_date = date.fromisoformat("2021-06-01")
end_date = date.today() - timedelta(days=1)

auth_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
data_url_format = "https://tdx.transportdata.tw/api/historical/v2/Historical/Bus/RealTimeNearStop/City/Taipei?Dates={}&%24format=CSV"
output_dir = "hist_loc_data/"

MAX_WORKERS = 12

class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return{
            'content-type': content_type,
            'grant_type': grant_type,
            'client_id': self.app_id,
            'client_secret': self.app_key
        }


class Data():

    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        auth_JSON = json.loads(self.auth_response.text)
        access_token = auth_JSON.get('access_token')

        return{
            'authorization': 'Bearer ' + access_token,
            'Accept-Encoding': 'gzip'
        }


# fetch data from TDX server, and save data into csv file
def fetch_n_save(data_date, tried_auth=0):
    data_url = data_url_format.format(data_date.isoformat())
    global auth_response
    try:
        d = Data(app_id, app_key, auth_response)
        data_response = requests.get(data_url, headers=d.get_data_header(), stream=True)
    
    except:
        # retry authentication
        if not tried_auth:
            a = Auth(app_id, app_key)
            auth_response = requests.post(auth_url, a.get_auth_header())
            print("Authentication requested: {}".format(auth_response))
            return fetch_n_save(data_date, 1)
        else:
            print("Download Error: {}".format(data_date.isoformat()))
            raise
            return 1

    else:
        file_path = output_dir + ("{}.csv".format(data_date.isoformat()))

        # write csv file with tqdm progress bar
        try:
            with open(file_path, mode="wb") as file, tqdm(
                desc=data_date.isoformat(),
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
                leave=True,
                smoothing=True,
                colour='green',
            ) as bar:
                for data in data_response.iter_content(chunk_size=1024):
                    size = file.write(data)
                    bar.update(size)
        except:
            # download failed
            raise
            return 1
        else:
            # job done succeessfully
            return 0


# fetch historic location data from TDX server on a specific date and save as .csv file, if data not exist
def check_n_get_data(data_date, downloaded_list):
    file_path = output_dir + ("{}.csv".format(data_date.isoformat()))

    # check if file exist
    if [data_date.isoformat()] in downloaded_list:
        print("File already downloaded: {}".format(data_date.isoformat()))
    else:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print("Corrupted file found: {}. Deleted.".format(
                data_date.isoformat()))
        # create new files
        print("File starts downloading: {}".format(data_date.isoformat()))
        
        # download file
        if not (fetch_n_save(data_date)):
            print("File successfully downloaded: {}".format(data_date.isoformat()))
            # append to successfully downloaded list
            downloaded_list.append(data_date.isoformat())
            list_file_path = output_dir + "downloaded_list.csv"
            with open(list_file_path, mode='a') as file:
                writer = csv.writer(file)
                writer.writerow([data_date.isoformat()])

        # clean up messed file
        else:
            print("Download failed: {}".format(file_path))
            if os.path.exists(file_path):
                os.remove(file_path)
                print("File corrupted and deleted: {}".format(file_path))


def get_downloaded_list():
    list_file_path = output_dir + "downloaded_list.csv"
    downloaded_list = []
    try:
        with open(list_file_path, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                downloaded_list.append(row)
    except:
        print("Dowloaded list not found at path: {}".format(list_file_path))
        raise
        return []
    print("Downloaded list dectected.")
    return downloaded_list


# auto fetch files
def auto_fetch_files(start_date, end_date):
    # read downloaded list
    downloaded_list = get_downloaded_list()

    # multitasking download
    date_processing = start_date
    while date_processing < end_date:
        executor.submit(check_n_get_data, date_processing, downloaded_list)
        
        # get data from the next day
        date_processing += timedelta(days=1)

if __name__ == "__main__":
    # authentication
    print("Hi there!")
    a = Auth(app_id, app_key)
    auth_response = requests.post(auth_url, a.get_auth_header())
    print("Authentication requested: {}".format(auth_response))
    
    # multiprocessing
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    auto_fetch_files(start_date, end_date)
    executor.shutdown()
