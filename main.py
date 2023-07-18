import csv
import json
import logging
import os
import random
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from dataclasses import dataclass
from typing import List

import pandas as pd
from fake_useragent import UserAgent
from win10toast import ToastNotifier

# Lock for synchronization
lock = threading.Lock()
logging.basicConfig(filename=f'logs/{date.today()}.txt', level=logging.ERROR, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s %(levelname)-8s %(message)s')
user_agent = UserAgent(browsers=['edge', 'chrome', 'safari'])
toaster = ToastNotifier()


@dataclass
class KrogerData:
    Ecommerce: str
    CityName: str
    StateAbbrev: str
    ZipCode: str
    Delivery: str
    DeliveryGrocery: list
    DeliveryRestaurants: list
    DeliveryAll: list
    Pickup: str
    PickupGrocery: list
    PickupRestaurants: list
    PickupAll: list


def postal_code_formatter(code: int) -> str:
    postal_code = str(code)
    if len(postal_code) == 4:  # Prepend 0 to postal codes with only 4 digits.
        postal_code = f'0{code}'

    return postal_code


def download_data(postal_code: str) -> str:
    try:
        api_url = 'https://www.kroger.com/atlas/v1/modality/options'
        default_headers = {
            'authority': 'www.kroger.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.kroger.com',
            'referer': 'https://www.kroger.com/',
            'user-agent': user_agent.random
        }
        payload = {
            'address': {
                'postalCode': postal_code
            }
        }
        status_code = None
        response = requests.post(api_url, timeout=random.randint(10, 100), headers=default_headers,
                                 data=json.dumps(payload))
        if response:
            raw_data = response.text
            status_code = response.status_code

            if response.ok or response.status_code in [200, 201]:
                print(f"Download success: {postal_code}")
            elif status_code == 400:
                invalid_msg = f"Postal code is invalid - {postal_code}"
                logging.error(invalid_msg)
                raise Exception(invalid_msg)

            return raw_data
        else:
            toaster.show_toast('Download data error', 'Download failure exception', duration=3)
            raise Exception(f'Download failure exception')

    except Exception as e:
        logging.error(str(f'download_data() error: {str(e)} - {postal_code}'))


def transform_data(details: dict, raw_data: str) -> List[KrogerData]:
    data = {
        'Ecommerce': 'Kroger',
        'CityName': details.get('NAME', None),
        'StateAbbrev': details.get('RG_ABBREV', None),
        'ZipCode': details.get('ID', None),
    }

    try:
        json_data = json.loads(raw_data)
        if 'data' in json_data and 'modalityOptions' in json_data['data']:
            modality_options = json_data['data']['modalityOptions']
            to_check = ['DELIVERY', 'PICKUP']

            for option in to_check:
                if option in modality_options and modality_options[option]:
                    data[option.capitalize()] = 'Yes'
                else:
                    data[option.capitalize()] = 'No'

        elif 'errors' in json_data:
            data['Delivery'] = 'No'
            data['Pickup'] = 'No'

        data.update({
            'DeliveryGrocery': ['Kroger'] if data['Delivery'] == 'Yes' else [],
            'DeliveryRestaurants': [],
            'DeliveryAll': ['Kroger'] if data['Delivery'] == 'Yes' else [],
            'PickupGrocery': ['Kroger'] if data['Pickup'] == 'Yes' else [],
            'PickupRestaurants': [],
            'PickupAll': ['Kroger'] if data['Pickup'] == 'Yes' else []
        })

        return list(data.values())

    except json.decoder.JSONDecodeError:
        toaster.show_toast('Error', 'JSON Decode Error', duration=3)
        logging.error('transform_data() error: Downloaded data could not be parsed into JSON format')
    except Exception as e:
        toaster.show_toast('Transform data error', str(e), duration=3)
        logging.error(f'transform_data() error: {str(e)}')


def write_to_file(file_path: str, data: list, mode: str = 'w'):
    with lock:
        with open(file_path, mode, newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)


def extract_csv_data(input_file: str = 'input/USZipCodesXLS.csv', output_file: str = 'output/Kroger-US-Full.csv'):
    input_df = pd.read_csv(input_file)
    output_df = pd.read_csv(output_file)
    existing_postal_codes = output_df['ZipCode'].to_list()

    df = input_df[~input_df['ID'].isin(existing_postal_codes)].to_dict(orient='records')
    print(f'{len(existing_postal_codes)} postal codes filtered out')

    return df

    # states = df['RG_NAME'].unique().tolist()
    # existing_postal_codes = []
    #
    # for state in states:
    #     state_file = f'output/{state}.csv'
    #     state_path = os.path.expanduser(f'{state_file}')
    #     if os.path.exists(state_path):
    #         state_df = pd.read_csv(state_file)
    #         existing_postal_codes.extend(state_df['ZipCode'].to_list())
    #
    # df = df[~df['ID'].isin(existing_postal_codes)]  # Filter out postal codes that have finished
    # print(f'{len(existing_postal_codes)} postal codes filtered out')
    #
    # grouped_data_by_state = df.groupby('RG_NAME').apply(lambda x: x.to_dict('records')).to_dict()
    # return grouped_data_by_state


def process_data(details: dict):
    postal_code = postal_code_formatter(details.get('ID', None))
    raw_data = download_data(postal_code)

    if raw_data:
        data = transform_data(details, raw_data)
        write_to_file(f'output/Kroger-US-Full.csv', data, 'a')
        print(f"Written to file: {postal_code}")

        delay = random.randint(5, 15)
        print(f'Delaying for {delay} seconds.')
        time.sleep(delay)
    else:
        logging.error(f"Process failed: {details['ID']}")


def initialize_output_file(filename: str = 'output/Kroger-US-Full.csv'):
    if not os.path.exists('output'):
        os.mkdir('output')

    initial_file = pd.DataFrame(columns=['Ecommerce', 'CityName', 'StateAbbrev', 'ZipCode', 'Delivery',
                                         'DeliveryGrocery', 'DeliveryRestaurants', 'DeliveryAll', 'Pickup',
                                         'PickupGrocery', 'PickupRestaurants', 'PickupAll'])
    initial_file.to_csv(filename, mode='w', index=False)


def main():
    initialize_output_file()
    csv_data = extract_csv_data()

    with ThreadPoolExecutor(max_workers=12) as executor:
        for row in csv_data:
            executor.submit(process_data, row)
            time.sleep(random.randint(1, 3))


if __name__ == '__main__':
    main()
