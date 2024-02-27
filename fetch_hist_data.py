# This program fetchs historical bus location data from the TDX website.

from api import *

import csv
import requests
from pprint import pprint
import json
from datetime import date, timedelta
import os
import threading
from memory_profiler import profile
import gc

gc.enable()

start_date = date.fromisoformat("2021-06-01")
end_date = date.today() - timedelta(days=1)

auth_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
data_url_format = "https://tdx.transportdata.tw/api/historical/v2/Historical/Bus/RealTimeNearStop/City/Taipei?Dates={}&%24format=CSV"
output_dir = "hist_loc_data/"

MAX_THREADS = 8

semaphore = threading.Semaphore(value=MAX_THREADS)


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


# fetch data from TDX server and return
def fetch_data(data_date):
    data_url = data_url_format.format(data_date.isoformat())
    global auth_response
    try:
        d = Data(app_id, app_key, auth_response)
        data_response = requests.get(data_url, headers=d.get_data_header())
    except:
        a = Auth(app_id, app_key)
        auth_response = requests.post(auth_url, a.get_auth_header())
        print("Authentication requested: {}".format(auth_response))
        try:
            d = Data(app_id, app_key, auth_response)
            data_response = requests.get(data_url, headers=d.get_data_header())
        except:
            print("Download Error: {}".format(data_date.isoformat()))
        else:
            return data_response
    else:
        return data_response

# write fetched data into csv file


def save_csv(data, file_path):
    try:
        with open(file_path, mode='w', encoding='utf8') as file:
            file.write(data)
    except:
        if os.path.exists(file_path):
            os.remove(file_path)
        print("File corrupted and deleted: {}".format(file_path))
        raise

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
        fetched_data = fetch_data(data_date).text
        save_csv(fetched_data, file_path)
        print("File successfully downloaded: {}".format(data_date.isoformat()))
        del fetched_data

        # append to successfully downloaded list
        downloaded_list.append(data_date.isoformat())
        list_file_path = output_dir + "downloaded_list.csv"
        with open(list_file_path, mode='a') as file:
            writer = csv.writer(file)
            writer.writerow([data_date.isoformat()])

    # allow next thread to start
    semaphore.release()


def get_downloaded_list():
    list_file_path = output_dir + "downloaded_list.csv"
    downloaded_list = []
    try:
        with open(list_file_path, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                downloaded_list.append(row)
    except:
        print("Dowloaded list not found!")
        return []
    print("Downloaded list dectected.")
    return downloaded_list


# auto fetch files
def auto_fetch_files(start_date, end_date):
    # read downloaded list
    downloaded_list = get_downloaded_list()

    # multitasking download
    threads = []
    date_processing = start_date
    while date_processing < end_date:
        print("active threads: {}".format(len(threads)))
        semaphore.acquire()
        thread = threading.Thread(target=check_n_get_data, args=(
            date_processing, downloaded_list,))
        threads.append(thread)
        thread.start()

        # get data from the next day
        date_processing += timedelta(days=1)

    # wait all threads to finish
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    # authentication
    print("hi")
    a = Auth(app_id, app_key)
    auth_response = requests.post(auth_url, a.get_auth_header())
    print("Authentication requested: {}".format(auth_response))

    auto_fetch_files(start_date, end_date)
